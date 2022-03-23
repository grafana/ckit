package ckit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_chasher_Lookup(t *testing.T) {
	var (
		viewerPeer      = Peer{Name: "viewer-peer", State: StateViewer}
		participantPeer = Peer{Name: "participant-peer", State: StateParticipant}
		terminatingPeer = Peer{Name: "terminating-peer", State: StateTerminating}
	)

	tt := []struct {
		name   string
		peers  []Peer
		hashTy HashType

		expectPeer  Peer
		expectError string
	}{
		{
			name:        "HashTypeRead fails if there are no Participant or Terminating nodes",
			peers:       []Peer{viewerPeer},
			hashTy:      HashTypeRead,
			expectError: "not enough nodes: need at least 1, have 0",
		},
		{
			name:       "HashTypeRead permits Participant nodes",
			peers:      []Peer{viewerPeer, participantPeer},
			hashTy:     HashTypeRead,
			expectPeer: participantPeer,
		},
		{
			name:       "HashTypeRead permits Terminating nodes",
			peers:      []Peer{viewerPeer, terminatingPeer},
			hashTy:     HashTypeRead,
			expectPeer: terminatingPeer,
		},
		{
			name:        "HashTypeReadWrite fails if there are no Participant nodes",
			peers:       []Peer{viewerPeer, terminatingPeer},
			hashTy:      HashTypeReadWrite,
			expectError: "not enough nodes: need at least 1, have 0",
		},
		{
			name:       "HashTypeReadWrite permits Participant nodes",
			peers:      []Peer{viewerPeer, participantPeer, terminatingPeer},
			hashTy:     HashTypeReadWrite,
			expectPeer: participantPeer,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			hasher := RingHasher(128)
			hasher.SetPeers(tc.peers)

			res, err := hasher.Lookup(0, 1, tc.hashTy)
			switch {
			case tc.expectError != "":
				require.EqualError(t, err, tc.expectError)
			default:
				require.Equal(t, []Peer{tc.expectPeer}, res)
			}
		})
	}
}
