// Package ckit is an extendible cluster toolkit to create clusters for
// building distributed systems that use consistent hashing for message
// distribution. It has a few core concepts:
//
// 1. The Discoverer is the base of the cluster and used by every node.
//    Discoverers find peers with a Discoverer using gossip.
//
// 2. A Node sits in front of a Discoverer and responds to peer discoveries.
//    A Node may implement extra logic to determine which subset of
//    discovered nodes can be used for message distribution.
//
// 3. Finally, implementers of Node use a Hash to determine how to implement
//    consistent hashing.
package ckit

// Peer is a discovered peer in the cluster. It holds its node name along
// with its address used for discovery (gossip) and an API.
type Peer struct {
	// Name of the Peer, uniquely identifying it from other peers.
	Name string
	// Address used for gossiping to this peer. Includes the port number.
	GossipAddr string
	// An application-specific addr. Typically used for sharing API addresses.
	ApplicationAddr string
}

// OnPeersChanged is a function that will be invoked whenever the set of peers
// changes. It is only guaranteed that the latest set of changes will be passed
// to the callback; intermediate changes may be skipped.
type OnPeersChanged = func(ps []Peer)
