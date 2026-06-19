package messages

import (
	"fmt"

	"github.com/grafana/ckit/internal/lamport"
)

// Weights represents the per-target series weights advertised by a node, used
// for series-aware (weighted) work distribution. Keys are target hashes
// (shard.Key as uint64) and values are that target's observed series count.
//
// Like State, a Weights message is keyed by NodeName for gossip: a newer
// message for a node supersedes older ones, so frequent updates collapse to the
// latest. Time is a lamport timestamp used to decide which message is newer.
type Weights struct {
	// Name of the node these weights are for.
	NodeName string
	// Time the weights were generated.
	Time lamport.Time
	// Weights maps a target key to its observed series count.
	Weights map[uint64]uint64
}

// String returns the string representation of the Weights message.
func (w Weights) String() string {
	return fmt.Sprintf("%s @%d: %d targets", w.NodeName, w.Time, len(w.Weights))
}

var _ Message = (*Weights)(nil)

// Type implements Message.
func (w *Weights) Type() Type { return TypeWeights }

// Name implements Message.
func (w *Weights) Name() string { return w.NodeName }

// Cache implements Message.
func (w *Weights) Cache() bool { return true }
