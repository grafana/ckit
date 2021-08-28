package main

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/rfratto/ckit"
	"github.com/rfratto/ckit/chash"
)

// Server clusters a KV store. It implements KV.
type Server struct {
	log log.Logger

	inner *Store

	name string
	disc *ckit.Discoverer
	node *ckit.Node
}

func NewServer(inner *Store, c ckit.DiscovererConfig) (*Server, error) {
	s := &Server{log: c.Log, inner: inner}

	node := ckit.NewNode(chash.Ring(128), s.onPeersChanged)
	disc, err := ckit.NewDiscoverer(&c, node)
	if err != nil {
		return nil, fmt.Errorf("failed to create discoverer: %w", err)
	}

	s.name = c.Name
	s.node = node
	s.disc = disc

	return s, nil
}

// Start starts the server. If peers is nil, Server will be a single-node
// server.
func (s *Server) Start(peers []string) error {
	return s.disc.Start(peers)
}

func (s *Server) onPeersChanged(ps ckit.PeerSet) {
	if len(ps) == 0 {
		level.Debug(s.log).Log("msg", "peer changed to empty set, skipping rebalancing")
		return
	}

	level.Info(s.log).Log("msg", "peers changed, rebalancing keys", "peers", ps)

	// Iterate over all keys and move any key that no longer belongs to us.
	// This operation is expensive.
	//
	// Note that if the filter handoff fails, keys will be lost. A
	// production-ready KV store would want to handle this better. This is not a
	// production-ready KV store.
	s.inner.Filter(func(key, value string) bool {
		owners, err := s.node.Get(key, 1)
		if err != nil {
			level.Error(s.log).Log("msg", "ownership check failed. key will be deleted", "key", key, "err", err)
			return false
		}

		if owners[0].Name == s.name {
			return true
		}

		level.Info(s.log).Log("msg", "moving key to new owner", "key", key, "owner", owners[0].Name)

		// We're not the owner any more. Send to the new owner.
		cli := NewClient(
			fmt.Sprintf("http://%s", owners[0].ApplicationAddr),
			http.DefaultClient,
		)
		if err := cli.Set(context.Background(), key, value); err != nil {
			level.Error(s.log).Log("msg", "transfer failed. key will be deleted", "key", key, "err", err)
		}

		return false
	})
}

// Set implements KV. If key does not belong to s, the Set request will
// be forwarded to the appropriate owner.
func (s *Server) Set(ctx context.Context, key, value string) error {
	owners, err := s.node.Get(key, 1)
	if err == nil && owners[0].Name != s.name {
		// Forward request to new owner.
		cli := NewClient(
			fmt.Sprintf("http://%s", owners[0].ApplicationAddr),
			http.DefaultClient,
		)
		return cli.Set(ctx, key, value)
	}

	level.Info(s.log).Log("msg", "handling set", "key", key)
	return s.inner.Set(ctx, key, value)
}

// Delete implements KV. If key does not belong to s, the Delete request
// will be forwarded to the appropriate owner.
func (s *Server) Delete(ctx context.Context, key string) error {
	owners, err := s.node.Get(key, 1)
	if err == nil && owners[0].Name != s.name {
		// Forward request to new owner.
		cli := NewClient(
			fmt.Sprintf("http://%s", owners[0].ApplicationAddr),
			http.DefaultClient,
		)
		return cli.Delete(ctx, key)
	}

	level.Info(s.log).Log("msg", "handling delete", "key", key)
	return s.inner.Delete(ctx, key)
}

// Get implements KV. If key does not belong to s, the Get request
// will be forwarded to the appropriate owner.
func (s *Server) Get(ctx context.Context, key string) (string, error) {
	owners, err := s.node.Get(key, 1)
	if err == nil && owners[0].Name != s.name {
		// Forward request to new owner.
		cli := NewClient(
			fmt.Sprintf("http://%s", owners[0].ApplicationAddr),
			http.DefaultClient,
		)
		return cli.Get(ctx, key)
	}

	level.Info(s.log).Log("msg", "handling get", "key", key)
	return s.inner.Get(ctx, key)
}

// Close closes the Server.
func (s *Server) Close() error {
	var firstError error
	for _, c := range []io.Closer{s.disc, s.node} {
		err := c.Close()
		if firstError == nil {
			firstError = err
		}
	}

	return firstError
}
