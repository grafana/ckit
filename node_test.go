package ckit

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/ckit/internal/testlogger"
	"github.com/grafana/ckit/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func newTestNode(t *testing.T, l log.Logger, name string) (n *Node, addr string) {
	t.Helper()

	if l == nil {
		l = log.NewNopLogger()
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	cfg := Config{
		Name:          name,
		AdvertiseAddr: lis.Addr().String(),
		Log:           log.With(l, "node", name),
	}

	cli := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(network, addr string, _ *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	node, err := NewNode(cli, cfg)
	require.NoError(t, err)
	mux := http.NewServeMux()
	baseRoute, handler := node.Handler()
	mux.Handle(baseRoute, handler)

	httpServer := &http.Server{
		Addr:    lis.Addr().String(),
		Handler: h2c.NewHandler(mux, &http2.Server{}),
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	go func() {
		err := httpServer.Serve(lis)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			require.NoError(t, err)
		}
	}()
	t.Cleanup(func() { require.NoError(t, httpServer.Shutdown(context.Background())) })

	node.Observe(FuncObserver(func(peers []peer.Peer) (reregister bool) {
		names := make([]string, len(peers))
		for i := range peers {
			names[i] = fmt.Sprintf("%s (%s)", peers[i].Name, peers[i].State)
		}
		level.Debug(cfg.Log).Log("msg", "peers changed", "peers", strings.Join(names, ", "))
		return true
	}))

	return node, cfg.AdvertiseAddr
}

func runTestNode(t *testing.T, n *Node, join []string) {
	t.Helper()

	err := n.Start(join)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, n.Stop()) })
}

func TestNode_State(t *testing.T) {
	t.Run("initial state should be viewer", func(t *testing.T) {
		n, _ := newTestNode(t, testlogger.New(t), "node-a")
		runTestNode(t, n, nil)

		require.Equal(t, peer.StateViewer, n.CurrentState())
	})

	t.Run("can move states", func(t *testing.T) {
		n, _ := newTestNode(t, testlogger.New(t), "node-a")
		runTestNode(t, n, nil)

		err := n.ChangeState(context.Background(), peer.StateParticipant)
		require.NoError(t, err)

		require.Equal(t, peer.StateParticipant, n.CurrentState())

		// Check an invalid state change (StateParticipant -> StateViewer)
		err = n.ChangeState(context.Background(), peer.StateViewer)
		require.EqualError(t, err, "invalid transition from participant to viewer", "expected state change to be rejected")
	})

	t.Run("peers are notified of state changes", func(t *testing.T) {
		var (
			l   = testlogger.New(t)
			ctx = context.Background()

			a, aAddr = newTestNode(t, l, "node-a")
			b, _     = newTestNode(t, l, "node-b")
		)

		runTestNode(t, a, nil)
		runTestNode(t, b, []string{aAddr})

		// Make two changes: make node-a a participant and node-b a viewer.
		// We then want to check that each node is (eventually) aware of the other
		// node's new state.
		require.NoError(t, a.ChangeState(ctx, peer.StateParticipant))

		require.NoError(t, b.ChangeState(ctx, peer.StateParticipant))
		require.NoError(t, b.ChangeState(ctx, peer.StateTerminating))

		// Wait for each node to be aware of the other's state change.
		waitPeerState(t, b, a.cfg.Name, peer.StateParticipant)
		waitPeerState(t, a, b.cfg.Name, peer.StateTerminating)
	})

	t.Run("nodes can restart in viewer state", func(t *testing.T) {
		// This test can fail if a node receieves an old message about its state
		// before it shut down.

		var (
			l   = testlogger.New(t)
			ctx = context.Background()

			a, aAddr = newTestNode(t, l, "node-a")
			b, _     = newTestNode(t, l, "node-b")
		)

		runTestNode(t, a, nil) // Run a for the duration of the test

		// Start b and then transition it into Terminating.
		require.NoError(t, b.Start([]string{aAddr}))
		require.NoError(t, b.ChangeState(ctx, peer.StateParticipant))
		require.NoError(t, b.ChangeState(ctx, peer.StateTerminating))

		// Wait for a to know b is terminating and close the node.
		waitPeerState(t, a, b.cfg.Name, peer.StateTerminating)
		require.NoError(t, b.Stop())

		// Wait for a to remove b from its peers.
		waitClusterState(t, a, func(n *Node) bool {
			return len(a.Peers()) == 1
		})

		// Recreate b. b should be in Viewer state.
		b, _ = newTestNode(t, l, b.cfg.Name)
		require.NoError(t, b.Start([]string{aAddr}))
		defer b.Stop()

		// Wait for a to know b is a viewer.
		waitPeerState(t, a, b.cfg.Name, peer.StateViewer)
	})
}

func waitPeerState(t *testing.T, node *Node, peerName string, expect peer.State) {
	t.Helper()

	waitClusterState(t, node, func(n *Node) bool {
		for _, p := range n.Peers() {
			if p.Name != peerName {
				continue
			}
			if p.State == expect {
				return true
			}
		}
		return false
	})
}

func waitClusterState(t *testing.T, node *Node, check func(*Node) bool) {
	t.Helper()

	done := make(chan struct{}, 1)
	node.Observe(FuncObserver(func([]peer.Peer) (reregister bool) {
		if check(node) {
			done <- struct{}{}
			return false
		}
		return true
	}))

	// Do an initial check, we might be already good.
	if check(node) {
		return
	}

	// Wait for up to 30 seconds for the event.
	select {
	case <-time.After(30 * time.Second):
		require.FailNow(t, "cluster state never matched condition")
	case <-done:
		return
	}
}

func TestNode_Observe(t *testing.T) {
	t.Run("invoked when peer set changes", func(t *testing.T) {
		var (
			l       = testlogger.New(t)
			invoked atomic.Int64

			a, aAddr = newTestNode(t, l, "node-a")
			b, _     = newTestNode(t, l, "node-b")
		)

		runTestNode(t, a, nil)

		a.Observe(FuncObserver(func([]peer.Peer) (reregister bool) {
			invoked.Inc()
			return true
		}))

		runTestNode(t, b, []string{aAddr})

		require.Eventually(t, func() bool {
			return invoked.Load() > 0
		}, 5*time.Second, 250*time.Millisecond)
	})

	t.Run("observers can reregister", func(t *testing.T) {
		var (
			l = testlogger.New(t)
			//ctx = context.Background()
			invoked atomic.Int64

			a, aAddr = newTestNode(t, l, "node-a")
			b, _     = newTestNode(t, l, "node-b")
		)

		a.Observe(FuncObserver(func(_ []peer.Peer) (reregister bool) {
			invoked.Inc()
			return true
		}))

		runTestNode(t, a, nil)

		require.Eventually(t, func() bool {
			return invoked.Load() > 0
		}, 5*time.Second, 250*time.Millisecond)

		previousInvokes := invoked.Load()
		runTestNode(t, b, []string{aAddr})

		require.Eventually(t, func() bool {
			return invoked.Load() > previousInvokes
		}, 5*time.Second, 250*time.Millisecond)
	})

	t.Run("observers can unregister", func(t *testing.T) {
		var (
			l = testlogger.New(t)

			a, aAddr = newTestNode(t, l, "node-a")
			b, _     = newTestNode(t, l, "node-b")
		)

		observeCh := make(chan struct{})

		a.Observe(FuncObserver(func(_ []peer.Peer) (reregister bool) {
			close(observeCh) // Panics if called more than once
			return false
		}))

		runTestNode(t, a, nil)

		select {
		case <-observeCh:
			// no-op
		case <-time.After(5 * time.Second):
			require.FailNow(t, "Observe never invoked")
		}
		<-observeCh // Wait for our channel to be closed

		// Start our second node. We'll wait for node-a to be aware of it, and then
		// sleep for a bit longer just to make sure the background goroutine
		// processing peer events never invokes our callback again.
		runTestNode(t, b, []string{aAddr})

		waitClusterState(t, a, func(n *Node) bool {
			return len(n.Peers()) == 2
		})
		time.Sleep(500 * time.Millisecond)
	})
}

func TestNode_Peers(t *testing.T) {
	t.Run("peers join", func(t *testing.T) {
		var (
			l = testlogger.New(t)

			a, aAddr = newTestNode(t, l, "node-a")
			b, bAddr = newTestNode(t, l, "node-b")
			c, cAddr = newTestNode(t, l, "node-c")
		)

		runTestNode(t, a, nil)
		runTestNode(t, b, []string{aAddr})
		runTestNode(t, c, []string{bAddr})

		waitClusterState(t, a, func(n *Node) bool {
			return len(a.Peers()) == 3
		})

		expectPeers := []peer.Peer{
			{Name: "node-a", Addr: aAddr, Self: true, State: peer.StateViewer},
			{Name: "node-b", Addr: bAddr, Self: false, State: peer.StateViewer},
			{Name: "node-c", Addr: cAddr, Self: false, State: peer.StateViewer},
		}
		require.ElementsMatch(t, expectPeers, a.Peers())
	})

	t.Run("peers leave", func(t *testing.T) {
		var (
			l = testlogger.New(t)

			a, aAddr = newTestNode(t, l, "node-a")
			b, bAddr = newTestNode(t, l, "node-b")
			c, _     = newTestNode(t, l, "node-c")
		)

		runTestNode(t, a, nil)
		runTestNode(t, b, []string{aAddr})

		// Manually start the third node so we can control when Stop is called.
		err := c.Start([]string{bAddr})
		require.NoError(t, err)

		// Wait for node-a to be aware of all 3 nodes.
		waitClusterState(t, a, func(n *Node) bool {
			return len(a.Peers()) == 3
		})

		// Then, stop the third node and wait for node-a to receive the change.
		require.NoError(t, c.Stop())

		waitClusterState(t, a, func(n *Node) bool {
			return len(a.Peers()) == 2
		})

		expectPeers := []peer.Peer{
			{Name: "node-a", Addr: aAddr, Self: true, State: peer.StateViewer},
			{Name: "node-b", Addr: bAddr, Self: false, State: peer.StateViewer},
		}
		require.ElementsMatch(t, expectPeers, a.Peers())
	})
}

func TestNewNodeIPv6(t *testing.T) {
	t.Helper()
	l := testlogger.New(t)
	name := "node-a"

	if l == nil {
		l = log.NewNopLogger()
	}

	lis, err := net.Listen("tcp", "[::1]:0")
	require.NoError(t, err)

	cfg := Config{
		Name:          name,
		AdvertiseAddr: lis.Addr().String(),
		Log:           log.With(l, "node", name),
	}

	cli := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(network, addr string, _ *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	_, err = NewNode(cli, cfg)
	require.NoError(t, err)
}
