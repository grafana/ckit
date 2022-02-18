package ckit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParticipantObserver(t *testing.T) {
	tt := []struct {
		name          string
		before, after []Peer
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
			after:      []Peer{{Name: "foo", State: StateViewer}},
			shouldCall: false,
		},
		{
			name:       "new participant",
			before:     nil,
			after:      []Peer{{Name: "foo", State: StateParticipant}},
			shouldCall: true,
		},
		{
			name:       "existing participant",
			before:     []Peer{{Name: "foo", State: StateParticipant}},
			after:      []Peer{{Name: "foo", State: StateParticipant}},
			shouldCall: false,
		},
		{
			name:       "existing participant changed",
			before:     []Peer{{Name: "foo", Addr: "oldaddr", State: StateParticipant}},
			after:      []Peer{{Name: "foo", Addr: "newaddr", State: StateParticipant}},
			shouldCall: true,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			var called bool

			obs := participantObserver{
				lastParticipants: tc.before,
				next: FuncObserver(func([]Peer) bool {
					called = true
					return true
				}),
			}

			_ = obs.NotifyPeersChanged(tc.after)
			require.Equal(t, tc.shouldCall, called)
		})
	}
}
