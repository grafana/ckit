package messages

import "github.com/rfratto/ckit/internal/lamport"

// State represents a State change broadcast from a node.
type State struct {
	// Name of the node this state change is for.
	NodeName string
	// New State of the node.
	NewState int
	// Time the state was generated.
	Time lamport.Time
}

var _ Message = (*State)(nil)

// Type implements Message.
func (s *State) Type() Type { return TypeState }

// Invalidates implements Message.
func (s *State) Invalidates(m Message) bool {
	other, ok := m.(*State)
	if !ok {
		return false
	}
	return s.NodeName == other.NodeName && s.Time > other.Time
}

// Cache implements Message.
func (s *State) Cache() bool { return true }
