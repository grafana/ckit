package ckit

import (
	"testing"

	"github.com/rfratto/ckit/peer"
	"github.com/stretchr/testify/require"
)

func TestParticipantObserver(t *testing.T) {
	tt := []struct {
		name          string
		before, after []peer.Peer
		shouldCall    bool
	}{
		{
			name:       "no peers",
			before:     nil,
			after:      nil,
			shouldCall: false,
		},
		{
			name:       "new non-participant",
			before:     nil,
			after:      []peer.Peer{{Name: "foo", State: peer.StateViewer}},
			shouldCall: false,
		},
		{
			name:       "new participant",
			before:     nil,
			after:      []peer.Peer{{Name: "foo", State: peer.StateParticipant}},
			shouldCall: true,
		},
		{
			name:       "existing participant",
			before:     []peer.Peer{{Name: "foo", State: peer.StateParticipant}},
			after:      []peer.Peer{{Name: "foo", State: peer.StateParticipant}},
			shouldCall: false,
		},
		{
			name:       "existing participant changed",
			before:     []peer.Peer{{Name: "foo", Addr: "oldaddr", State: peer.StateParticipant}},
			after:      []peer.Peer{{Name: "foo", Addr: "newaddr", State: peer.StateParticipant}},
			shouldCall: true,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			var called bool

			obs := participantObserver{
				lastParticipants: tc.before,
				next: FuncObserver(func([]peer.Peer) bool {
					called = true
					return true
				}),
			}

			_ = obs.NotifyPeersChanged(tc.after)
			require.Equal(t, tc.shouldCall, called)
		})
	}
}
