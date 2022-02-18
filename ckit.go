// Package ckit is a cluster toolkit for creating distributed systems that use
// consistent hashing for message distribution. There are two main concepts:
//
// 1. Nodes use gossip to find other Nodes running in the cluster. Gossip is
//    performed over gRPC. Nodes in the cluster are respresented as Peers.
//
// 2. Nodes manage the state of a Hash, which are used to determine which Peer
//    owns some key.
package ckit

// Peer is a discovered node within the cluster.
type Peer struct {
	Name  string // Name of the Peer. Unique across the cluster.
	Addr  string // host:port address of the peer.
	Self  bool   // True if Peer is the local Node.
	State State  // State of the peer.
}

// String returns the name of p.
func (p Peer) String() string { return p.Name }
