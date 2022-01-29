// Package chash implements a set of different consistent hashing algorithms.
package chash

// Hash is a consistent hashing algorithm. Implementations of Hash are
// goroutine safe.
type Hash interface {
	// Get will retrieve the n owners for key. Get will return an error if there
	// are not at least n nodes.
	Get(key uint64, n int) ([]string, error)

	// SetNodes updates the set of nodes used for hashing.
	SetNodes(nodes []string)
}
