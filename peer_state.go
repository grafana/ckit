package ckit

import "fmt"

// PeerState is used by Nodes to inform their peers what their role is as part
// of gossip.
type PeerState uint

const (
	// PeerStateViewer is the default state. Nodes in the Viewer state have a
	// read-only view of the cluster, and are never considered owners when
	// hashing.
	PeerStateViewer PeerState = iota

	// PeerStateParticipant marks a node as available to receive writes. It will
	// be considered a potential owner while hashing read or write operations.
	PeerStateParticipant

	// PeerStateTerminating is used when a Participant node is shutting down.
	// Terminating nodes are considered potential owners while hashing read
	// operations.
	PeerStateTerminating
)

// AllStates holds a list of all valid states.
var allStates = [...]PeerState{
	PeerStateViewer,
	PeerStateParticipant,
	PeerStateTerminating,
}

// String returns the string representation of s.
func (s PeerState) String() string {
	switch s {
	case PeerStateViewer:
		return "viewer"
	case PeerStateParticipant:
		return "participant"
	case PeerStateTerminating:
		return "terminating"
	default:
		return fmt.Sprintf("<unknown state %d>", s)
	}
}
