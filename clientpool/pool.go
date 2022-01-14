// Package clientpool implements a gRPC client pool which can manage a changing
// number of clients to connect to.
package clientpool

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
)

// Options configures options for the client pool.
type Options struct {
	// Optional logging interface.
	Log log.Logger

	// Time a connection must be unused for before it's considered stale.
	StaleTime time.Duration

	// Frequency at which stale connections should be removed.
	StaleCleanupFrequency time.Duration

	// Maximum number of clients that may exist in the client pool. 0 means
	// unlimited.
	MaxClients int

	// CleanupLRU configures the least recently used client to be forcibly
	// removed when the maximum client size is hit.
	//
	// If this is false, no new clients can be generated past MaxClients.
	CleanupLRU bool
}

// DefaultOptions holds default options for creating client pools.
var DefaultOptions = Options{
	StaleTime:             30 * time.Second,
	StaleCleanupFrequency: 1 * time.Minute,
	MaxClients:            100,
	CleanupLRU:            true,
}

// Pool manages a set of clients.
type Pool struct {
	log      log.Logger
	dialOpts []grpc.DialOption
	opts     Options

	clientsMut    sync.RWMutex
	clients       map[string]*client
	reverseLookup map[*grpc.ClientConn]*client
	closed        bool

	exited    chan struct{}
	cancelRun context.CancelFunc
}

type client struct {
	Addr string
	Conn *grpc.ClientConn

	Mutex    sync.Mutex
	LastUsed time.Time
}

func (c *client) updateLastUsed() {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	c.LastUsed = time.Now()
}

// New creats a new Pool. An error will be returned if the options are invalid.
// The optionally provided dial options will be used to connect to clients.
//
// Call Close to close the pool.
func New(opts Options, dialOptions ...grpc.DialOption) (*Pool, error) {
	// Validations
	switch {
	case opts.StaleTime <= 0:
		return nil, fmt.Errorf("StaleTime must be greater than 0")
	case opts.StaleCleanupFrequency <= 0:
		return nil, fmt.Errorf("StaleCleanupFrequency must be greater than 0")
	case opts.MaxClients < 0:
		return nil, fmt.Errorf("MaxClients must be greater or equal to 0")
	}

	ctx, cancel := context.WithCancel(context.Background())

	l := opts.Log
	if l == nil {
		l = log.NewNopLogger()
	}

	p := &Pool{
		log:  l,
		opts: opts,

		clients:       make(map[string]*client, opts.MaxClients),
		reverseLookup: make(map[*grpc.ClientConn]*client),

		exited:    make(chan struct{}),
		cancelRun: cancel,
	}

	// Create the full set of dial options by prepending our interceptors before
	// the user-supplied options.
	var fullDialOptions []grpc.DialOption
	fullDialOptions = append(fullDialOptions, grpc.WithUnaryInterceptor(unaryLastUsedInterceptor(p)))
	fullDialOptions = append(fullDialOptions, grpc.WithStreamInterceptor(streamLastUsedInterceptor(p)))
	fullDialOptions = append(fullDialOptions, dialOptions...)
	p.dialOpts = fullDialOptions

	go p.run(ctx)
	return p, nil
}

func (p *Pool) run(ctx context.Context) {
	defer close(p.exited)

	var cleanupTick <-chan time.Time
	if p.opts.StaleCleanupFrequency == 0 {
		// Make a channel that never fires if we're not going to do cleanups.
		cleanupTick = make(<-chan time.Time)
	} else {
		t := time.NewTicker(p.opts.StaleCleanupFrequency)
		defer t.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-cleanupTick:
			p.removeStaleClients()
		}
	}
}

// removeStaleClient removes all clients which are stale or shut down.
func (p *Pool) removeStaleClients() {
	p.clientsMut.Lock()
	defer p.clientsMut.Unlock()

	for addr, client := range p.clients {
		stale := time.Since(client.LastUsed) > p.opts.StaleTime
		if !stale && client.Conn.GetState() != connectivity.Shutdown {
			continue
		}
		if err := client.Conn.Close(); err != nil {
			level.Error(p.log).Log("msg", "failed to close stale client", "err", err)
		}
		delete(p.clients, addr)
		delete(p.reverseLookup, client.Conn)
	}
}

// Get retrieves a new or existing *grpc.ClientConn for the given address. The
// provided context will is only used for creating the new connection, and will
// not close the returned client.
//
// If a previous caller closed the returned client, a new one will be
// generated. It is not recommended to manually close clients; let the
// connection pool close stale clients instead.
func (p *Pool) Get(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	p.clientsMut.Lock()
	defer p.clientsMut.Unlock()

	if p.closed {
		return nil, fmt.Errorf("clientpool has closed")
	}

	// If an existing entry exists and isn't shut down, we can return early.
	// Otherwise we either need to create a new entry or replace the terminated
	// one.
	entry, ok := p.clients[addr]
	if ok && entry.Conn.GetState() != connectivity.Shutdown {
		entry.updateLastUsed()
		return entry.Conn, nil
	}
	if entry != nil {
		// Delete the existing client
		delete(p.clients, addr)
		delete(p.reverseLookup, entry.Conn)
	}

	if p.opts.MaxClients > 0 && len(p.clients)+1 > p.opts.MaxClients {
		if !p.opts.CleanupLRU {
			return nil, fmt.Errorf("maxium number of clients reached")
		}
		if err := p.removeLRU(); err != nil {
			return nil, err
		}
	}

	cc, err := grpc.DialContext(ctx, addr, p.dialOpts...)
	if err != nil {
		return nil, err
	}
	entry = &client{
		Addr:     addr,
		Conn:     cc,
		LastUsed: time.Now(),
	}
	p.clients[addr] = entry
	p.reverseLookup[cc] = entry
	return cc, nil
}

// removeLRU removes the least recently used client. Must only be called with
// the client mut held.
func (p *Pool) removeLRU() error {
	clients := make([]*client, 0, len(p.clients))
	for _, c := range p.clients {
		clients = append(clients, c)
	}
	sort.Slice(clients, func(i, j int) bool {
		return clients[i].LastUsed.Before(clients[j].LastUsed)
	})

	if len(clients) == 0 {
		// Shoudln't be possible to get here, but let's be defensive.
		return fmt.Errorf("no clients to remove")
	}

	err := clients[0].Conn.Close()
	if err != nil {
		return err
	}
	delete(p.clients, clients[0].Addr)
	delete(p.reverseLookup, clients[0].Conn)
	return nil
}

// Close closes the client pool. Once the pool is closed, all existing
// connections will be shut down and no new connections may be opened.
func (p *Pool) Close() error {
	p.clientsMut.Lock()
	defer p.clientsMut.Unlock()

	p.cancelRun()
	<-p.exited

	// Close existing connections.
	for _, client := range p.clients {
		err := client.Conn.Close()
		if err != nil {
			level.Warn(p.log).Log("msg", "failed to close client on shutdown", "err", err)
		}
	}
	// Reset the map to empty instead of deleting everything individiually.
	p.clients = make(map[string]*client)

	p.closed = true
	return nil
}

func unaryLastUsedInterceptor(p *Pool) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		p.clientsMut.RLock()
		cli, ok := p.reverseLookup[cc]
		p.clientsMut.RUnlock()
		if ok {
			cli.Mutex.Lock()
			cli.LastUsed = time.Now()
			cli.Mutex.Unlock()
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func streamLastUsedInterceptor(p *Pool) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		p.clientsMut.RLock()
		cli, ok := p.reverseLookup[cc]
		p.clientsMut.RUnlock()
		if ok {
			cli.Mutex.Lock()
			cli.LastUsed = time.Now()
			cli.Mutex.Unlock()
		}

		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		if cli != nil {
			return &poolClientStream{cli: cli, stream: cs}, nil
		}
		return cs, nil
	}
}

// poolClientStream wraps around a grpc.ClientStream and updates the last used
// client time for every method call.
type poolClientStream struct {
	cli    *client
	stream grpc.ClientStream
}

func (s *poolClientStream) Header() (metadata.MD, error) {
	s.cli.updateLastUsed()
	return s.stream.Header()
}

func (s *poolClientStream) Trailer() metadata.MD {
	s.cli.updateLastUsed()
	return s.stream.Trailer()
}

func (s *poolClientStream) CloseSend() error {
	s.cli.updateLastUsed()
	return s.stream.CloseSend()
}

func (s *poolClientStream) Context() context.Context {
	s.cli.updateLastUsed()
	return s.stream.Context()
}

func (s *poolClientStream) SendMsg(m interface{}) error {
	s.cli.updateLastUsed()
	return s.stream.SendMsg(m)
}

func (s *poolClientStream) RecvMsg(m interface{}) error {
	// RecvMsg updates last used twice since it blocks until a message is available.
	s.cli.updateLastUsed()
	defer s.cli.updateLastUsed()
	return s.stream.RecvMsg(m)
}
