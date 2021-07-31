package messages

import (
	"github.com/rfratto/ckit/internal/lamport"
)

// Metadata holds information about a node.
type Metadata struct {
	// Time that the metadata was generated.
	Time lamport.Time
	// Application-specific address exposed by the node.
	ApplicationAddr string
}

var _ Message = (*Metadata)(nil)

// Type implements Message.
func (md *Metadata) Type() Type { return TypeMetadata }

// Invalidates implements Message.
func (md *Metadata) Invalidates(m Message) bool {
	other, ok := m.(*Metadata)
	return ok && md.Time > other.Time
}

// Cache implements Message.
func (md *Metadata) Cache() bool { return false }
