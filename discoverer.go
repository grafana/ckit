package ckit

import (
	"github.com/go-kit/kit/log"
)

// DiscovererConfig controls how peers discover each other.
type DiscovererConfig struct {
	// Name of the discoverer. Must be unique from all other discoverers.
	// This will be the name found in Peer.
	Name string

	// Address to listen for gossip traffic on.
	ListenAddr string
	// Address to tell peers to connect to.
	AdvertiseAddr string

	// Port (UDP & TCP) to listen for gossip traffic on.
	ListenPort int

	// ApplicationAddr is an application-specific address to share with peers.
	// Useful for sharing the address where an API is exposed.
	ApplicationAddr string

	// Log to use for logging events.
	Log log.Logger
}

// A Discoverer is used to find peers in the cluster. They work using gossip;
// once Start is called with an initial set of peers, all peers will eventually
// be found.
//
// The Discoverer will pass found peers to the attached Node for processing.
type Discoverer struct {
}

// NewDiscoverer creates a new Discoverer with a Node to send events to.
// The Node's AddPeer and RemovePeer methods will be invoked as peers are
// added, updated, and removed. The Node's Close method will be invoked
// when the Discoverer is closed.
func NewDiscoverer(cfg *DiscovererConfig, n Node) (*Discoverer, error) {
	panic("NYI")
}

// Start begins discovery of peers. An initial list of peers to connect to
// is provided. Discoverers will gossip the full set of peers with each
// other.
func (d *Discoverer) Start(peers []string) error {
	panic("NYI")
}

// Close calls Close on the attached Node and then stops the Discoverer.
func (d *Discoverer) Close() error {
	panic("NYI")
}
