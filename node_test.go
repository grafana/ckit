package ckit

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/rfratto/ckit/internal/testlogger"
	"github.com/rfratto/ckit/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
)

func newTestNode(t *testing.T, l log.Logger, name string) (n *Node, addr string) {
	t.Helper()

	if l == nil {
		l = log.NewNopLogger()
	}

	grpcServer := grpc.NewServer()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	cfg := Config{
		Name:          name,
		AdvertiseAddr: lis.Addr().String(),
		Log:           log.With(l, "node", name),
	}

	node, err := NewNode(grpcServer, cfg)
	require.NoError(t, err)

	go func() {
		err := grpcServer.Serve(lis)
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			require.NoError(t, err)
		}
	}()
	t.Cleanup(grpcServer.GracefulStop)

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

		// Wait for node-b to receive node-a's state change.
		require.Eventually(t, func() bool {
			bPeers := b.Peers()
			for _, p := range bPeers {
				if p.Name != "node-a" {
					continue
				}
				return p.State == peer.StateParticipant
			}

			return false
		}, 30*time.Second, time.Millisecond*250)

		// Wait for node-a to receieve node-b's state change.
		require.Eventually(t, func() bool {
			aPeers := a.Peers()
			for _, p := range aPeers {
				if p.Name != "node-b" {
					continue
				}
				return p.State == peer.StateTerminating
			}

			return false
		}, 30*time.Second, time.Millisecond*250)
	})
}

func TestNode_Observe(t *testing.T) {
	t.Run("invoked when peer set changes", func(t *testing.T) {
		var (
			l = testlogger.New(t)
			//ctx = context.Background()
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

		require.Eventually(t, func() bool {
			return len(a.Peers()) == 2
		}, 15*time.Second, 500*time.Millisecond)

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

		require.Eventually(t, func() bool {
			return len(a.Peers()) == 3
		}, 15*time.Second, 250*time.Millisecond)

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
		require.Eventually(t, func() bool {
			return len(a.Peers()) == 3
		}, 15*time.Second, 250*time.Millisecond)

		// Then, stop the third node and wait for node-a to receive the change.
		require.NoError(t, c.Stop())

		require.Eventually(t, func() bool {
			return len(a.Peers()) == 2
		}, 15*time.Second, 250*time.Millisecond)

		expectPeers := []peer.Peer{
			{Name: "node-a", Addr: aAddr, Self: true, State: peer.StateViewer},
			{Name: "node-b", Addr: bAddr, Self: false, State: peer.StateViewer},
		}
		require.ElementsMatch(t, expectPeers, a.Peers())
	})
}
