package ckit

import (
	"fmt"
	"sort"
	"sync"

	"github.com/rfratto/ckit/internal/chash"
)

// HashType is used to signify how a hash is intended to be used.
type HashType uint8

const (
	// HashTypeRead is used for read-only lookups. Only nodes in the Participant
	// or Terminating state are considered.
	HashTypeRead HashType = iota
	// HashTypeReadWrite is used for read or write lookups. Only nodes in the
	// Participant state are considered.
	HashTypeReadWrite
)

// String returns a string representation of the HashType.
func (ht HashType) String() string {
	switch ht {
	case HashTypeRead:
		return "Read"
	case HashTypeReadWrite:
		return "ReadWrite"
	default:
		return fmt.Sprintf("HashType(%d)", ht)
	}
}

// A Hasher can lookup the owner for a specific key.
type Hasher interface {
	// Lookup returns numOwners Peers for the provided key. The provided HashType
	// is used to determine which peers may be considered potential owners.
	//
	// An error will be returned if the type of eligible peers for the provided
	// HashType is less than numOwners.
	Lookup(key Key, numOwners int, ty HashType) ([]Peer, error)

	// SetPeers updates the set of peers used for hashing.
	SetPeers(ps []Peer)
}

// chasher wraps around two chash.Hash and adds logic for HashType.
type chasher struct {
	peersMut sync.RWMutex
	peers    map[string]Peer // Set of all peers shared across both hashes

	read, readWrite chash.Hash
}

// SetPeers implements Hasher.
func (ch *chasher) SetPeers(ps []Peer) {
	sort.Slice(ps, func(i, j int) bool { return ps[i].Name < ps[j].Name })

	var (
		newPeers     = make(map[string]Peer, len(ps))
		newRead      = make([]string, 0, len(ps))
		newReadWrite = make([]string, 0, len(ps))
	)

	for _, p := range ps {
		// NOTE(rfratto): newRead and newReadWrite remain in sorted order since we
		// append to them from the already-sorted ps slice.
		switch p.State {
		case StateParticipant:
			newRead = append(newRead, p.Name)
			newReadWrite = append(newReadWrite, p.Name)
			newPeers[p.Name] = p
		case StateTerminating:
			newRead = append(newRead, p.Name)
			newPeers[p.Name] = p
		}
	}

	ch.peersMut.Lock()
	defer ch.peersMut.Unlock()

	ch.peers = newPeers
	ch.read.SetNodes(newRead)
	ch.readWrite.SetNodes(newReadWrite)
}

// Lookup implements Hasher.
func (ch *chasher) Lookup(key Key, numOwners int, ty HashType) ([]Peer, error) {
	ch.peersMut.RLock()
	defer ch.peersMut.RUnlock()

	var (
		names []string
		err   error
	)

	switch ty {
	case HashTypeRead:
		names, err = ch.read.Get(uint64(key), numOwners)
	case HashTypeReadWrite:
		names, err = ch.readWrite.Get(uint64(key), numOwners)
	default:
		return nil, fmt.Errorf("unknown hash type %s", ty)
	}
	if err != nil {
		return nil, err
	}

	res := make([]Peer, len(names))
	for i, name := range names {
		p, ok := ch.peers[name]
		if !ok {
			panic("Unexpected peer " + name)
		}
		res[i] = p
	}
	return res, nil
}

// MultiprobeHasher implements a multi-probe hasher:
// https://arxiv.org/abs/1505.00062
//
// Multiprobe is optimized for a median peak-to-average load ratio of 1.05. It
// performs a lookup in O(K * log N) time, where K is 21.
func MultiprobeHasher() Hasher {
	return &chasher{
		read:      chash.Multiprobe(),
		readWrite: chash.Multiprobe(),
	}
}

// RendezvousHasher returns a rendezvous hasher (HRW, Highest Random Weight).
//
// RendezvousHasher is optimized for excellent load distribution, but has a
// runtime complexity of O(N).
func RendezvousHasher() Hasher {
	return &chasher{
		read:      chash.Rendezvous(),
		readWrite: chash.Rendezvous(),
	}
}

// RingHasher implements a ring hasher. numTokens determines how many tokens
// each node should have. Tokens are mapped to the unit circle, and then
// ownership of a key is determined by finding the next token on the unit
// circle. If two nodes have the same token, the node that lexicographically
// comes first will be used as the first owner.
//
// RingHasher is extremely fast, running in O(log N) time, but increases in
// memory usage as numTokens increases. Low values of numTokens will cause poor
// distribution; 256 or 512 is a good starting point.
func RingHasher(numTokens int) Hasher {
	return &chasher{
		read:      chash.Ring(numTokens),
		readWrite: chash.Ring(numTokens),
	}
}
