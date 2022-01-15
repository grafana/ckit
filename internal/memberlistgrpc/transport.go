// Package memberlistgrpc implements memberlist.Transport using gRPC. This
// would probably be a bad idea for traditional uses of memberlist, but ckit
// only gossips member status, so the overhead of using gRPC for everything
// should be minimal.
package memberlistgrpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/hashicorp/memberlist"
	"github.com/rfratto/ckit/clientpool"
	"github.com/rfratto/ckit/internal/queue"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

//go:generate protoc --go_out=. --go_opt=module=github.com/rfratto/ckit/internal/memberlistgrpc --go-grpc_out=. --go-grpc_opt=module=github.com/rfratto/ckit/internal/memberlistgrpc  ./memberlistgrpc.proto

// Options controls the memberlistgrpc transport.
type Options struct {
	// Optional logger to use.
	Log log.Logger

	// Pool to use for generating clients. Must be set.
	Pool *clientpool.Pool

	// Timeout to use when sending a packet.
	PacketTimeout time.Duration
}

func NewTransport(srv *grpc.Server, opts Options) (memberlist.Transport, error) {
	if opts.Pool == nil {
		return nil, fmt.Errorf("client Pool must be provided")
	}

	l := opts.Log
	if l == nil {
		l = log.NewNopLogger()
	}

	tx := &transport{
		log:  l,
		opts: opts,

		inPacketQueue:  queue.New(1000), // Allow a buffer of 1000 packets
		inPacketCh:     make(chan *memberlist.Packet),
		outPacketQueue: queue.New(1000), // Allow a buffer of 1000 packets

		streamCh: make(chan net.Conn),
	}

	RegisterTransportServer(srv, &transportServer{t: tx})
	return tx, nil
}

type transport struct {
	log  log.Logger
	opts Options

	startInPacketQueue sync.Once
	inPacketQueue      *queue.Queue
	inPacketCh         chan *memberlist.Packet

	// Queue for outgoing packets so we can implement WriteTo as
	// nearly-non-blocking
	startOutPacketQueue sync.Once
	outPacketQueue      *queue.Queue

	streamCh chan net.Conn

	// Incoming packets and streams should be rejected when the transport is
	// closed.
	closedMut sync.RWMutex
	closed    bool

	// Generated after calling
	localAddr net.Addr
}

var (
	_ memberlist.Transport          = (*transport)(nil)
	_ memberlist.NodeAwareTransport = (*transport)(nil)
)

// FinalAdvertiseAddr returns the IP to advertise to peers. The memberlist must
// be configured with an advertise address and port, otherwise this will fail.
func (t *transport) FinalAdvertiseAddr(ip string, port int) (net.IP, int, error) {
	if ip == "" {
		return nil, 0, fmt.Errorf("no configured advertise address")
	} else if port == 0 {
		return nil, 0, fmt.Errorf("missing real listen port")
	}

	advertiseIP := net.ParseIP(ip)
	if advertiseIP == nil {
		return nil, 0, fmt.Errorf("failed to parse advertise ip %q", ip)
	}

	// Convert to IPv4 if possible.
	if ip4 := advertiseIP.To4(); ip4 != nil {
		advertiseIP = ip4
	}

	t.localAddr = &net.TCPAddr{IP: advertiseIP, Port: port}
	return advertiseIP, port, nil
}

func (t *transport) WriteTo(b []byte, addr string) (time.Time, error) {
	type outPacket struct {
		Data []byte
		Addr string
	}

	t.startOutPacketQueue.Do(func() {
		go func() {
			for {
				v, err := t.outPacketQueue.Dequeue(context.Background())
				if err != nil {
					return
				}
				op := v.(*outPacket)
				t.writeToSync(op.Data, op.Addr)
			}
		}()
	})

	t.outPacketQueue.Enqueue(&outPacket{Data: b, Addr: addr})
	return time.Now(), nil
}

func (t *transport) writeToSync(b []byte, addr string) {
	ctx := context.Background()
	if t.opts.PacketTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.opts.PacketTimeout)
		defer cancel()
	}

	cc, err := t.opts.Pool.Get(ctx, addr)
	if err != nil {
		level.Error(t.log).Log("msg", "failed to get pooled client", "err", err)
		return
	}

	cli := NewTransportClient(cc)
	_, err = cli.SendPacket(ctx, &Message{Data: b})
	if err != nil {
		level.Error(t.log).Log("msg", "failed to send packet", "err", err)
	}
}

func (t *transport) WriteToAddress(b []byte, addr memberlist.Address) (time.Time, error) {
	return t.WriteTo(b, addr.Addr)
}

func (t *transport) PacketCh() <-chan *memberlist.Packet {
	t.startInPacketQueue.Do(func() {
		go func() {
			for {
				v, err := t.inPacketQueue.Dequeue(context.Background())
				if err != nil {
					return
				}
				t.inPacketCh <- v.(*memberlist.Packet)
			}
		}()
	})

	return t.inPacketCh
}

func (t *transport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.opts.PacketTimeout)
		defer cancel()
	}

	cc, err := t.opts.Pool.Get(ctx, addr)
	if err != nil {
		return nil, err
	}
	cli := NewTransportClient(cc)

	packetsClient, err := cli.StreamPackets(context.Background())
	if err != nil {
		return nil, err
	}

	var remoteAddr net.Addr
	if p, ok := peer.FromContext(packetsClient.Context()); ok {
		remoteAddr = p.Addr
	}

	var readMut sync.Mutex
	readCnd := sync.NewCond(&readMut)

	return &packetsClientConn{
		cli: packetsClient,

		localAddr:  t.localAddr,
		remoteAddr: remoteAddr,

		readMut:      &readMut,
		readCnd:      readCnd,
		readMessages: make(chan readResult),
	}, nil
}

func (t *transport) DialAddressTimeout(addr memberlist.Address, timeout time.Duration) (net.Conn, error) {
	return t.DialTimeout(addr.Addr, timeout)
}

func (t *transport) StreamCh() <-chan net.Conn {
	return t.streamCh
}

func (t *transport) Shutdown() error {
	t.closedMut.Lock()
	defer t.closedMut.Unlock()
	t.closed = true
	_ = t.inPacketQueue.Close()
	_ = t.outPacketQueue.Close()
	return nil
}

type transportServer struct {
	UnimplementedTransportServer

	t *transport
}

func (s *transportServer) SendPacket(ctx context.Context, msg *Message) (*emptypb.Empty, error) {
	recvTime := time.Now()

	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Internal, "missing peer in context")
	}

	s.t.inPacketQueue.Enqueue(&memberlist.Packet{
		Buf:       msg.Data,
		From:      p.Addr,
		Timestamp: recvTime,
	})
	return &emptypb.Empty{}, nil
}

func (s *transportServer) StreamPackets(stream Transport_StreamPacketsServer) error {
	p, ok := peer.FromContext(stream.Context())
	if !ok {
		return status.Errorf(codes.Internal, "missing peer in context")
	}

	waitClosed := make(chan struct{})

	var readMut sync.Mutex
	readCnd := sync.NewCond(&readMut)

	conn := &packetsClientConn{
		cli:     stream,
		onClose: func() { close(waitClosed) },

		localAddr:  s.t.localAddr,
		remoteAddr: p.Addr,

		readMut:      &readMut,
		readCnd:      readCnd,
		readMessages: make(chan readResult),
	}

	s.t.streamCh <- conn
	<-waitClosed
	return nil
}
