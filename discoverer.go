package ckit

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/hashicorp/memberlist"
	"github.com/rfratto/ckit/clientpool"
	"github.com/rfratto/ckit/internal/memberlistgrpc"
	"github.com/rfratto/ckit/internal/queue"
	"google.golang.org/grpc"
)

// DiscovererConfig controls how peers discover each other.
type DiscovererConfig struct {
	// Name of the discoverer. Must be unique from all other discoverers.
	// This will be the name found in Peer.
	Name string

	// Address in host:port form to tell peers to connect to.
	AdvertiseAddr string

	// Optional logger to use for logging events.
	Log log.Logger

	// Client pool to use for forming gRPC connections to peers.
	Pool *clientpool.Pool
}

// A Discoverer is used for nodes to discover each other. Nodes gossip over gRPC;
// once Start is called with an initial set of peers, all peers will eventually
// be found.
//
// The Discoverer will pass found peers to the attached Node for processing.
type Discoverer struct {
	d *discovererDelegate
}

// NewDiscoverer creates a new Discoverer with a Node to send events to.
// The Node's AddPeer and RemovePeer methods will be invoked as peers are
// added, updated, and removed. The Node's Close method will be invoked
// when the Discoverer is closed.
func NewDiscoverer(srv *grpc.Server, cfg *DiscovererConfig, n *Node) (*Discoverer, error) {
	if cfg.Log == nil {
		cfg.Log = log.NewNopLogger()
	}
	if cfg.Pool == nil {
		var err error
		cfg.Pool, err = clientpool.New(clientpool.DefaultOptions, grpc.WithInsecure())
		if err != nil {
			return nil, fmt.Errorf("failed to build default client pool: %w", err)
		}
	}

	advertiseAddr, advertisePortString, err := net.SplitHostPort(cfg.AdvertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to read advertise address: %w", err)
	}
	advertisePort, err := net.LookupPort("tcp", advertisePortString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse advertise port: %w", err)
	}

	grpcTransport, err := memberlistgrpc.NewTransport(srv, memberlistgrpc.Options{
		Log:           cfg.Log,
		Pool:          cfg.Pool,
		PacketTimeout: 3 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to build transport: %w", err)
	}

	mlc := memberlist.DefaultLANConfig()
	mlc.Name = cfg.Name
	mlc.Transport = grpcTransport
	mlc.AdvertiseAddr = advertiseAddr
	mlc.AdvertisePort = advertisePort
	mlc.LogOutput = io.Discard

	dd := &discovererDelegate{
		cfg:  cfg,
		log:  cfg.Log,
		node: n,

		conflictQueue: queue.New(1),
	}

	mlc.Events = dd
	mlc.Delegate = dd
	mlc.Conflict = dd

	ml, err := memberlist.Create(mlc)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}

	dd.ml = ml
	dd.NumNodes = ml.NumMembers
	dd.RetransmitMult = mlc.RetransmitMult

	return &Discoverer{d: dd}, nil
}

// Start begins discovery of peers. An initial list of peers to connect to
// is provided. Discoverers will gossip the full set of peers with each
// other.
func (d *Discoverer) Start(peers []string) error {
	_, err := d.d.ml.Join(peers)
	if err != nil {
		level.Error(d.d.log).Log("msg", "failed to join memberlist", "err", err)
		return fmt.Errorf("failed to join memberlist: %w", err)
	}

	conflict, ok := d.d.conflictQueue.TryDequeue()
	if ok {
		conflict := conflict.(*memberlist.Node)
		return fmt.Errorf("failed to join memberlist: name conflict with %s", conflict.Address())
	}

	level.Info(d.d.log).Log("msg", "successfully joined memberlist")
	return nil
}

// Close calls Close on the attached Node and then stops the Discoverer.
func (d *Discoverer) Close() error {
	_ = d.d.ml.Leave(time.Second * 5)
	return d.d.ml.Shutdown()
}

type discovererDelegate struct {
	memberlist.TransmitLimitedQueue

	cfg *DiscovererConfig
	ml  *memberlist.Memberlist
	log log.Logger

	conflictQueue *queue.Queue

	node *Node
}

func (dd *discovererDelegate) NodeMeta(limit int) []byte {
	// no-op: no expected metadata
	return nil
}

func (dd *discovererDelegate) NotifyMsg(raw []byte) {
	// no-op: no user messages being sent
}

func (dd *discovererDelegate) LocalState(join bool) []byte {
	// no-op: no local state being sent
	return nil
}

func (dd *discovererDelegate) MergeRemoteState(raw []byte, join bool) {
	// no-op: no local state being sent
}

func (dd *discovererDelegate) NotifyConflict(existing, other *memberlist.Node) {
	dd.conflictQueue.Enqueue(other)
}

func (dd *discovererDelegate) NotifyJoin(node *memberlist.Node) {
	level.Info(dd.log).Log("msg", "new member in memberlist", "name", node.Name, "addr", node.Address())

	// Pass through to NotifyUpdate since a join and update event are handled
	// the same way.
	dd.NotifyUpdate(node)
}

func (dd *discovererDelegate) NotifyUpdate(node *memberlist.Node) {
	dd.node.AddPeer(Peer{
		Name: node.Name,
		Addr: node.Address(),
		Self: node.Name == dd.cfg.Name,
	})
}

func (dd *discovererDelegate) NotifyLeave(node *memberlist.Node) {
	level.Info(dd.log).Log("msg", "memberlist peer left", "name", node.Name, "addr", node.Address())
	dd.node.RemovePeer(node.Name)
}
