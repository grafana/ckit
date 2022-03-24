// Package ckit is a cluster toolkit for creating distributed systems that use
// consistent hashing for message distribution. There are two main concepts:
//
// 1. Nodes use gossip to find other Nodes running in the cluster. Gossip is
// performed over gRPC. Nodes in the cluster are respresented as Peers.
//
// 2. Nodes manage the state of a Hash, which are used to determine which Peer
// owns some key.
package ckit
