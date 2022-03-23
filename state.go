package ckit

import "fmt"

// State is used by Nodes to inform their peers what their role is as part of
// gossip.
type State uint

const (
	// StateViewer is the default state. Nodes in the Viewer state have a
	// read-only view of the cluster, and are never considered owners when
	// hashing.
	StateViewer State = iota

	// StateParticipant marks a node as available to receive writes. It will be
	// considered a potential owner while hashing read or write operations.
	StateParticipant

	// StateTerminating is used when a Participant node is shutting down.
	// Terminating nodes are considered potential owners while hashing read
	// operations.
	StateTerminating
)

var allStates = []State{
	StateViewer,
	StateParticipant,
	StateTerminating,
}

// String returns the string representation of s.
func (s State) String() string {
	switch s {
	case StateViewer:
		return "viewer"
	case StateParticipant:
		return "participant"
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
