package chash

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

var allHashes = []struct {
	Name string
	H    func() Hash
}{
	{Name: "ring 256 tokens", H: func() Hash { return Ring(256) }},
	{Name: "ring 512 tokens", H: func() Hash { return Ring(512) }},
	{Name: "rendezvous", H: func() Hash { return Rendezvous() }},
	{Name: "multiprobe", H: func() Hash { return Multiprobe() }},
}

// TestHashes_Consistent enforces that all consistent hashing algorithms
// properly implement consistent hashing.
func TestHashes_Consistent(t *testing.T) {
	for _, tc := range allHashes {
		t.Run(tc.Name, func(t *testing.T) {
			h := tc.H()

			// Setting up 3 nodes should return those three unique nodes, in some order.
			var (
				nodes = []string{"node-a", "node-b", "node-c"}
				key   = xxhash.Sum64String("some-key")
			)

			h.SetNodes(nodes)
			allOwners, err := h.Get(key, len(nodes))
			require.NoError(t, err)
			require.ElementsMatch(t, nodes, allOwners, "did not get all input nodes as output")

			// If you remove allOwners[0] from nodes, allOwners[1] should become the next
			// top owner.
			h.SetNodes(allOwners[1:])
			newOwners, err := h.Get(key, len(allOwners)-1)

			require.NoError(t, err)
			require.Len(t, newOwners, len(allOwners)-1, "did not get all input nodes as output")
			require.Equal(t, allOwners[1], newOwners[0], "hash not using consistent hashing")

			// If you remove allOwners[1] from nodes, allOwners[2] should become the next
			// second-best owner.
			h.SetNodes(append(allOwners[:1], allOwners[2:]...))
			newOwners, err = h.Get(key, len(allOwners)-1)
			require.NoError(t, err)
			require.Len(t, newOwners, len(allOwners)-1, "did not get all input nodes as output")
			require.Equal(t, allOwners[2], newOwners[1], "hash not using consistent hashing")
		})
	}
}

// TestHashes_Distribution enforces that all consistent hashing algorithms
// distribute data evenly within some controlled tolerance.
func TestHashes_Distribution(t *testing.T) {
	var (
		numNodes  = 10
		numHashes = 10_000 * numNodes

		perfectDist = numHashes / numNodes
		errorMargin = 0.25 // Tolerance for distribution (percentage)
		minDist     = perfectDist - int(math.Floor(errorMargin*float64(perfectDist)))
		maxDist     = perfectDist + int(math.Ceil(errorMargin*float64(perfectDist)))
	)

	for _, hasher := range allHashes {
		t.Run(hasher.Name, func(t *testing.T) {
			var (
				h = hasher.H()

				nodes    = make([]string, numNodes)
				nodeDist = map[string]int{}
			)

			r := rand.New(rand.NewSource(0))
			randStr := func() string {
				key := make([]byte, 5)
				_, _ = r.Read(key)
				return fmt.Sprintf("%2x", key)
			}

			// Initialize the set of nodes and their distribution.
			for n := 0; n < numNodes; n++ {
				nodes[n] = randStr()
				nodeDist[nodes[n]] = 0
			}
			h.SetNodes(nodes)

			// Hash numHashes random keys.
			for i := 0; i < numHashes; i++ {
				owners, err := h.Get(xxhash.Sum64String(randStr()), 1)
				require.NoError(t, err)
				for _, owner := range owners {
					nodeDist[owner]++
				}
			}

			// Calculate stats of distributions across all nodes.
			var (
				dists = make([]float64, 0, len(nodeDist))
			)

			for _, calls := range nodeDist {
				dist := 100 * (float64(calls) / float64(perfectDist))
				dists = append(dists, dist)
			}
			sort.Float64s(dists)

			fmt.Printf(
				"%s distribution stats: min %0.1f%%, median %0.1f%%, max %0.1f%%\n",
				hasher.Name,
				dists[0], median(dists), dists[len(dists)-1],
			)

			for node, calls := range nodeDist {
				if calls < minDist || calls > maxDist {
					require.Failf(t, "distribution out of acceptable range",
						"unacceptable distribution for %s. expected [%d, %d], got %d",
						node, minDist, maxDist, calls,
					)
				}
			}
		})
	}
}

func median(nums []float64) float64 {
	mid := len(nums) / 2
	if len(nums)%2 != 0 {
		return nums[mid]
	}
	return (nums[mid-1] + nums[mid]) / 2.0
}

// BenchmarkHashes tests the hashing speed of each hashing algorithm.
func BenchmarkHashes(b *testing.B) {
	counts := []int{1, 10, 50, 100, 500, 1000}
	for _, count := range counts {
		b.Run(fmt.Sprintf("%d nodes", count), func(b *testing.B) {
			runBenchmarkHashes(b, count)
		})
	}
}

func runBenchmarkHashes(b *testing.B, numNodes int) {
	b.Helper()

	var nodes = make([]string, numNodes)
	for n := range nodes {
		nodes[n] = fmt.Sprintf("node_%d", n+1)
	}

	for _, hasher := range allHashes {
		b.Run(hasher.Name, func(b *testing.B) {
			b.StopTimer()
			h := hasher.H()
			h.SetNodes(nodes)
			r := rand.New(rand.NewSource(0))
			b.StartTimer()

			for n := 0; n < b.N; n++ {
				key := make([]byte, 5)
				_, _ = r.Read(key)
				_, _ = h.Get(xxhash.Sum64String(fmt.Sprintf("%2x", key)), 3)
			}
		})
	}
}

// BenchmarkHashes tests the resources consumed of each hashing algorithm.
func BenchmarkHashResource(b *testing.B) {
	counts := []int{1, 10, 50, 100, 500, 1000}
	for _, count := range counts {
		b.Run(fmt.Sprintf("%d nodes", count), func(b *testing.B) {
			runBenchmarkHashConsumption(b, count)
		})
	}
}

func runBenchmarkHashConsumption(b *testing.B, numNodes int) {
	b.Helper()

	var nodes = make([]string, numNodes)
	for n := range nodes {
		nodes[n] = fmt.Sprintf("node_%d", n+1)
	}

	for _, hasher := range allHashes {
		b.Run(hasher.Name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				h := hasher.H()
				h.SetNodes(nodes)

			}
		})
	}
}
