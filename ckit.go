// Package ckit is a lightweight cluster toolkit for creating distributed
// systems that use consistent hashing for message distribution. It uses
// three main concepts:
//
// 1. Discoverers use gossip to find other machines with a Discoverer. These
// are cluster Peers.
//
// 2. Discovered Peers are sent to a Node, which keeps track of cluster state.
//
// 3. Nodes manage the state of a Hash, which determines which Peer in a
// cluster owns a message.
package ckit

import "strings"

// Peer is a discovered peer within the cluster.
type Peer struct {
	// Name of the Peer. Unique across the whole cluster.
	Name string
	// Address used for gossiping to this peer. Includes the port number.
	GossipAddr string
	// An application-specific addr. Typically used for sharing API addresses.
	ApplicationAddr string
	// Self is true when this Peer represents the local discoverer.
	Self bool
}

// String returns the name of p.
func (p Peer) String() string {
	return p.Name
}

// PeerSet is a set of Peers.
type PeerSet []Peer

// String returns the set of peers.
func (ps PeerSet) String() string {
	names := make([]string, len(ps))
	for i, p := range ps {
		names[i] = p.String()
	}
	return strings.Join(names, ",")
}

// OnPeersChanged is a function that will be invoked whenever the set of peers
// changes. It is only guaranteed that the latest set of changes will be passed
// to the callback; intermediate changes may be skipped.
type OnPeersChanged = func(ps PeerSet)
