package shard_test

import (
	"fmt"
	"testing"

	"github.com/grafana/ckit"
	"github.com/grafana/ckit/shard"
	"github.com/stretchr/testify/require"
)

func Example() {
	// We can create a sharder to determine which Peers in a cluster are
	// responsible for certain keys.
	//
	// Here we use the Ring hash.
	ring := shard.Ring(256)

	// A ring must be given the set of Peers to consider for hashing. A ring can
	// be automatically updated as the cluster changes by providing the sharder
	// in ckit.Config.
	//
	// For this example, we'll manually set the peers.
	ring.SetPeers([]ckit.Peer{
		// The State of a Peer determines whether it can be used for hashing.
		//
		// Viewer nodes are never used for hashing; they're meant to be view-only
		// watchers of the cluster state.
		//
		// Participant nodes are used for hashing and can handle read or write
		// operations.
		//
		// Terminating nodes are Participant nodes that are shutting down; they can
		// no longer handle write operations, but they are still valid for read
		// operations.

		{Name: "viewer-a", State: ckit.PeerStateViewer},
		{Name: "viewer-b", State: ckit.PeerStateViewer},
		{Name: "viewer-c", State: ckit.PeerStateViewer},
		{Name: "node-a", State: ckit.PeerStateParticipant},
		{Name: "node-b", State: ckit.PeerStateParticipant},
		{Name: "node-c", State: ckit.PeerStateTerminating},
	})

	// Once SetPeers is called, you can determine the owner for some key. We'll
	// convert the "example-key" string into a Key and see which node should be
	// used for storing data for that key. This will always return the same
	// result for the same set of input peers.
	//
	// You can request any number of owners for a key, as long as that number of
	// nodes are eligible for the provided operation. We have 2 participant nodes,
	// so we can request up to 2 owners for OpReadWrite.
	owners, err := ring.Lookup(shard.StringKey("example-key"), 1, shard.OpReadWrite)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Owner of example-key: %s\n", owners[0].Name)

	// Output:
	// Owner of example-key: node-b
}

func Test_Lookup(t *testing.T) {
	var (
		viewerPeer      = ckit.Peer{Name: "viewer-peer", State: ckit.PeerStateViewer}
		participantPeer = ckit.Peer{Name: "participant-peer", State: ckit.PeerStateParticipant}
		terminatingPeer = ckit.Peer{Name: "terminating-peer", State: ckit.PeerStateTerminating}
	)

	tt := []struct {
		name   string
		peers  []ckit.Peer
		hashTy shard.Op

		expectPeer  ckit.Peer
		expectError string
	}{
		{
			name:        "HashTypeRead fails if there are no Participant or Terminating nodes",
			peers:       []ckit.Peer{viewerPeer},
			hashTy:      shard.OpRead,
			expectError: "not enough nodes: need at least 1, have 0",
		},
		{
			name:       "HashTypeRead permits Participant nodes",
			peers:      []ckit.Peer{viewerPeer, participantPeer},
			hashTy:     shard.OpRead,
			expectPeer: participantPeer,
		},
		{
			name:       "HashTypeRead permits Terminating nodes",
			peers:      []ckit.Peer{viewerPeer, terminatingPeer},
			hashTy:     shard.OpRead,
			expectPeer: terminatingPeer,
		},
		{
			name:        "HashTypeReadWrite fails if there are no Participant nodes",
			peers:       []ckit.Peer{viewerPeer, terminatingPeer},
			hashTy:      shard.OpReadWrite,
			expectError: "not enough nodes: need at least 1, have 0",
		},
		{
			name:       "HashTypeReadWrite permits Participant nodes",
			peers:      []ckit.Peer{viewerPeer, participantPeer, terminatingPeer},
			hashTy:     shard.OpReadWrite,
			expectPeer: participantPeer,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			ring := shard.Ring(128)
			ring.SetPeers(tc.peers)

			res, err := ring.Lookup(0, 1, tc.hashTy)
			switch {
			case tc.expectError != "":
				require.EqualError(t, err, tc.expectError)
			default:
				require.Equal(t, []ckit.Peer{tc.expectPeer}, res)
			}
		})
	}
}
