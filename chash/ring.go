package chash

import (
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/cespare/xxhash"
)

// Ring implements a ring consistent hash. numTokens determines how many tokens
// each node should have. Tokens are mapped to the unit circle, and then
// ownership of a key is determined by finding the next token on the unit
// circle. If two nodes have the same token, the node that lexicographically
// comes first will be used as the first owner.
//
// Ring hash is extremely fast, running in O(log N) time, but increases in
// memory usage as numTokens increases. Low values of numTokens will cause poor
// distribution; 256 or 512 is a good starting point.
func Ring(numTokens int) func() Hash {
	return func() Hash {
		return &ringHash{numTokens: numTokens}
	}
}

type ringHash struct {
	mut       sync.RWMutex
	numTokens int

	// Tokens for all nodes. Must be sorted at all times.
	numNodes int
	tokens   []ringToken
}

type ringToken struct {
	node  string
	token uint64
}

func (r *ringHash) Get(key string, n int) ([]string, error) {
	r.mut.RLock()
	defer r.mut.RUnlock()

	if n > r.numNodes {
		return nil, fmt.Errorf("not enough nodes: need at least %d, have %d", n, r.numNodes)
	} else if n == 0 {
		return []string{}, nil
	}

	keyHash := xxhash.Sum64String(key)
	idx := sort.Search(len(r.tokens), func(i int) bool {
		return r.tokens[i].token >= keyHash
	})
	if idx == len(r.tokens) {
		// Wrap around if we hit the end of the list.
		idx = 0
	}

	var (
		res   = make([]string, 0, n)
		cache = make(map[string]struct{})
	)

	for {
		owner := r.tokens[idx].node
		if _, found := cache[owner]; !found {
			res = append(res, owner)
			cache[owner] = struct{}{}
		}

		// Increment idx with wraparound.
		idx = (idx + 1) % len(r.tokens)

		if len(res) == n {
			break
		}
	}

	return res, nil
}

func (r *ringHash) SetNodes(nodes []string) {
	toks := make([]ringToken, 0, len(nodes)*r.numTokens)
	for _, node := range nodes {
		for t := 0; t < r.numTokens; t++ {
			toks = append(toks, ringToken{
				node:  node,
				token: xxhash.Sum64String(node + strconv.Itoa(t+1)),
			})
		}
	}
	sort.Sort(byRingToken(toks))

	r.mut.Lock()
	defer r.mut.Unlock()
	r.numNodes = len(nodes)
	r.tokens = toks
}

type byRingToken []ringToken

func (b byRingToken) Len() int      { return len(b) }
func (b byRingToken) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

func (b byRingToken) Less(i, j int) bool {
	if b[i].token == b[j].token {
		return b[i].node < b[j].node
	}
	return b[i].token < b[j].token
}
