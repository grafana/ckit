package ckit

import (
	"fmt"

	"github.com/grafana/ckit/internal/lamport"
	"github.com/grafana/ckit/internal/messages"
)

// stateMessage represents a State change broadcast from a node.
type stateMessage struct {
	// Name of the node this state change is for.
	NodeName string
	// New State of the node.
	NewState PeerState
	// Time the state was generated.
	Time lamport.Time
}

// String returns the string representation of the State message.
func (s stateMessage) String() string {
	return fmt.Sprintf("%s @%d: %s", s.NodeName, s.Time, s.NewState)
}

var _ messages.Message = (*stateMessage)(nil)

// Type implements Message.
func (s *stateMessage) Type() messages.Type { return messages.TypeState }

// Invalidates implements Message.
func (s *stateMessage) Invalidates(m messages.Message) bool {
	other, ok := m.(*stateMessage)
	if !ok {
		return false
	}
	return s.NodeName == other.NodeName && s.Time > other.Time
}

// Cache implements Message.
func (s *stateMessage) Cache() bool { return true }
