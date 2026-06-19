package ckit

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/ckit/internal/messages"
	"github.com/grafana/ckit/internal/testlogger"
	"github.com/grafana/ckit/peer"
	"github.com/stretchr/testify/require"
)

// Two participants that each advertise per-target weights should converge to the
// same merged view, and a later update from one node should propagate too.
func TestNode_Weights(t *testing.T) {
	l := testlogger.New(t)
	ctx := context.Background()

	a, aAddr := newTestNode(t, l, "node-a")
	b, _ := newTestNode(t, l, "node-b")

	runTestNode(t, a, nil)
	runTestNode(t, b, []string{aAddr})

	require.NoError(t, a.ChangeState(ctx, peer.StateParticipant))
	require.NoError(t, b.ChangeState(ctx, peer.StateParticipant))

	// Make sure both nodes know about each other before exchanging weights.
	waitPeerState(t, a, "node-b", peer.StateParticipant)
	waitPeerState(t, b, "node-a", peer.StateParticipant)

	require.NoError(t, a.SetLocalWeights(map[uint64]uint64{1: 100, 3: 300}))
	require.NoError(t, b.SetLocalWeights(map[uint64]uint64{2: 200}))

	want := map[uint64]uint64{1: 100, 2: 200, 3: 300}
	requireWeightsEventually(t, a, want)
	requireWeightsEventually(t, b, want)

	// A later update from node-a should propagate (newer lamport time wins).
	require.NoError(t, a.SetLocalWeights(map[uint64]uint64{1: 111, 3: 333}))
	updated := map[uint64]uint64{1: 111, 2: 200, 3: 333}
	requireWeightsEventually(t, a, updated)
	requireWeightsEventually(t, b, updated)
}

// handleWeightsMessage keeps the newest message (by lamport time) per node and
// ignores stale ones.
func TestNode_handleWeightsMessage_Lamport(t *testing.T) {
	n, _ := newTestNode(t, nil, "node-a")

	set := func(msg messages.Weights) bool {
		n.peerMut.Lock()
		defer n.peerMut.Unlock()
		return n.handleWeightsMessage(msg)
	}

	require.True(t, set(messages.Weights{NodeName: "node-b", Time: 2, Weights: map[uint64]uint64{1: 10}}))
	require.False(t, set(messages.Weights{NodeName: "node-b", Time: 1, Weights: map[uint64]uint64{1: 99}}), "older message must be ignored")
	require.Equal(t, uint64(10), n.PeerWeights()[1])

	require.True(t, set(messages.Weights{NodeName: "node-b", Time: 3, Weights: map[uint64]uint64{1: 30}}), "newer message must win")
	require.Equal(t, uint64(30), n.PeerWeights()[1])
}

func requireWeightsEventually(t *testing.T, n *Node, want map[uint64]uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		got := n.PeerWeights()
		if len(got) != len(want) {
			return false
		}
		for k, v := range want {
			if got[k] != v {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "weights never converged on node")
}
