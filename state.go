package ckit

import "fmt"

// State is used by Nodes to inform their peers what their role is as part of
// gossip.
type State uint

const (
	// StatePending is the default state. Nodes in the Pending state should not
	// be used for hashing.
	StatePending State = iota

	// StateParticipant allows a Node to be used for hashing.
	StateParticipant

	// StateViewer allows a Node to have a read-only view of the cluster: the
	// node itself will never be considered for hashing.
	StateViewer

	// StateTerminating is used when a node is shutting down. Terminating nodes
	// should not be used for hashing.
	StateTerminating
)

var allStates = []State{
	StatePending,
	StateParticipant,
	StateViewer,
	StateTerminating,
}

// String returns the string representation of s.
func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateParticipant:
		return "participant"
	case StateViewer:
		return "viewer"
	case StateTerminating:
		return "terminating"
	default:
		return fmt.Sprintf("<unknown state %d>", s)
	}
}

// ErrStateTransition is returned when a node requests an invalid state
// transition.
type ErrStateTransition struct {
	From, To State
}

// Error implements error.
func (e ErrStateTransition) Error() string {
	return fmt.Sprintf("invalid transition from %s to %s", e.From, e.To)
}
