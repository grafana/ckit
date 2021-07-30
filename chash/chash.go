// Package chash implements a set of different consistent hashing algorithms.
package chash

// Hash is a consistent hashing algorithm. Once given a set of nodes with
// SetNodes, Get will return the set of nodes responsible for a key. When the
// set of nodes changes, only a fraction of keys get reassigned to a new node.
//
// Implementations of Hash must be goroutine safe.
type Hash interface {
	// Get will retrieve the n owners for key. Get will return an error if
	// there are not at least n nodes.
	Get(key string, n int) ([]string, error)

	// SetNodes updates the set of nodes used for hashing.
	SetNodes(nodes []string)
}

// Builder will return an implementation of Hash.
type Builder = func() Hash
