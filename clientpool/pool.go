// Package clientpool implements a gRPC client pool which can manage a changing
// number of clients to connect to.
//
// Applications should use one clientpool across the entire application to
// maximize effectiveness of sharing connections.
package clientpool

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
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
	m        *metrics

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
// The set of defaultDialOpts will be used when opening new connections.
//
//
// Call Close to close the pool.
func New(opts Options, defaultDialOpts ...grpc.DialOption) (*Pool, error) {
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
		m:    newMetrics(opts),

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
	fullDialOptions = append(fullDialOptions, defaultDialOpts...)
	p.dialOpts = fullDialOptions

	go p.run(ctx)
	return p, nil
}

// Metrics returns metrics for the Pool.
func (p *Pool) Metrics() prometheus.Collector { return p.m }

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

	p.m.gcActive.Set(1)
	defer p.m.gcActive.Set(0)

	timer := prometheus.NewTimer(p.m.gcTotal)
	defer timer.ObserveDuration()

	for addr, client := range p.clients {
		stale := time.Since(client.LastUsed) > p.opts.StaleTime
		if !stale && client.Conn.GetState() != connectivity.Shutdown {
			continue
		}
		if err := p.closeConn(addr, client); err != nil {
			level.Error(p.log).Log("msg", "failed to close stale client", "err", err)
		}
	}
}

// closeConn closes a connection. clientsMut must be held.
func (p *Pool) closeConn(addr string, client *client) error {
	err := client.Conn.Close()

	// Clean up the pool regardless of whether the connection closed
	// successfully.
	delete(p.clients, addr)
	delete(p.reverseLookup, client.Conn)
	p.m.eventsTotal.WithLabelValues("closed").Inc()
	p.m.currentConns.Set(float64(len(p.clients)))

	return err
}

// Get retrieves a new or existing *grpc.ClientConn for the given address. The
// provided context will is only used for creating the new connection, and will
// not close the returned client.
//
// A new connection will be created if there is no existing connection or the
// existing connection was closed. The provided extraDialOpts will be appended
// to defaultDialOpts to create the new connection, but are ignored if an
// existing connection is retrieved.
//
// It is not recommended to manually close clients; let the pool close stale
// clients instead.
func (p *Pool) Get(ctx context.Context, addr string, extraDialOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	p.clientsMut.Lock()
	defer p.clientsMut.Unlock()

	defer func() {
		p.m.currentConns.Set(float64(len(p.clients)))
	}()

	if p.closed {
		p.m.lookupsTotal.WithLabelValues("error_other").Inc()
		return nil, fmt.Errorf("clientpool has closed")
	}

	// If an existing entry exists and isn't shut down, we can return early.
	// Otherwise we either need to create a new entry or replace the terminated
	// one.
	entry, ok := p.clients[addr]
	if ok && entry.Conn.GetState() != connectivity.Shutdown {
		entry.updateLastUsed()

		p.m.lookupsTotal.WithLabelValues("success").Inc()
		return entry.Conn, nil
	}
	if entry != nil {
		// Delete the existing client
		delete(p.clients, addr)
		delete(p.reverseLookup, entry.Conn)
	}

	if p.opts.MaxClients > 0 && len(p.clients)+1 > p.opts.MaxClients {
		if !p.opts.CleanupLRU {
			p.m.lookupsTotal.WithLabelValues("error_max_conns").Inc()
			return nil, fmt.Errorf("maxium number of clients reached")
		}
		if err := p.removeLRU(); err != nil {
			p.m.lookupsTotal.WithLabelValues("error_other").Inc()
			return nil, err
		}
	}

	dialOpts := make([]grpc.DialOption, len(p.dialOpts)+len(extraDialOpts))
	copy(dialOpts[0:], p.dialOpts)
	copy(dialOpts[len(p.dialOpts):], extraDialOpts)

	cc, err := grpc.DialContext(ctx, addr, dialOpts...)
	if err != nil {
		p.m.lookupsTotal.WithLabelValues("error_dial").Inc()
		return nil, err
	}
	entry = &client{
		Addr:     addr,
		Conn:     cc,
		LastUsed: time.Now(),
	}
	p.clients[addr] = entry
	p.reverseLookup[cc] = entry

	p.m.lookupsTotal.WithLabelValues("success").Inc()
	p.m.eventsTotal.WithLabelValues("opened").Inc()
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

	return p.closeConn(clients[0].Addr, clients[0])
}

// Close closes the client pool. Once the pool is closed, all existing
// connections will be shut down and no new connections may be opened.
func (p *Pool) Close() error {
	p.clientsMut.Lock()
	defer p.clientsMut.Unlock()

	p.cancelRun()
	<-p.exited

	// Close existing connections.
	for addr, client := range p.clients {
		err := p.closeConn(addr, client)
		if err != nil {
			level.Warn(p.log).Log("msg", "failed to close client on shutdown", "err", err)
		}
	}

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
