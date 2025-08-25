package messages

import (
	"fmt"

	"github.com/grafana/ckit/internal/lamport"
	"github.com/grafana/ckit/peer"
)

// State represents a State change broadcast from a node.
type State struct {
	// Name of the node this state change is for.
	NodeName string
	// New State of the node.
	NewState peer.State
	// Time the state was generated.
	Time lamport.Time
	// UnknownFields is used to store any fields that were added in newer
	// versions of State.
	UnknownFields map[string]any `codec:"-"`
}

// String returns the string representation of the State message.
func (s State) String() string {
	return fmt.Sprintf("%s @%d: %s", s.NodeName, s.Time, s.NewState)
}

var _ Message = (*State)(nil)

// Type implements Message.
func (s *State) Type() Type { return TypeState }

// Name implements Message.
func (s *State) Name() string { return s.NodeName }

// Cache implements Message.
func (s *State) Cache() bool { return true }

// CodecMissingField registers a missing field into s.UnknownFields.
func (s *State) CodecMissingField(field []byte, value interface{}) bool {
	if s.UnknownFields == nil {
		s.UnknownFields = make(map[string]interface{})
	}
	s.UnknownFields[string(field)] = value
	return true
}

// CodecMissingFields returns the current set of unknown fields.
func (s *State) CodecMissingFields() map[string]interface{} {
	return s.UnknownFields
}
