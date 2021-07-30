package ckit

import (
	"sort"
	"sync"

	"github.com/rfratto/ckit/chash"
)

// Node keeps track of peers that are part of the cluster and allows
// applications to look up owners of a key.
type Node interface {
	// Get looks up the n owners of key. Fails if there are not at least n nodes.
	Get(key string, n int) ([]Peer, error)

	// Close will stop any Node implementation-specific logic that determines
	// cluster membership.
	Close() error

	// AddPeer adds a peer to the Node. The local node should always be added
	// as a Peer.
	//
	// AddPeer may also be called when p is updated.
	AddPeer(p Peer)

	// RemovePeer removes a peer from the Node.
	RemovePeer(name string)
}

// BasicNode implements Node. Basic nodes will use all discovered peers for
// hashing.
type BasicNode struct {
	h              chash.Hash
	onPeersChanged OnPeersChanged

	peersMut sync.RWMutex
	peers    map[string]Peer
}

// NewBasicNode returns a new BasicNode. hb will be used for the hashing
// algorithm and cb will be invoked whenever the set of active peers changes.
func NewBasicNode(hb chash.Builder, cb OnPeersChanged) *BasicNode {
	bn := &BasicNode{
		h:              hb(),
		onPeersChanged: cb,

		peers: make(map[string]Peer),
	}
	return bn
}

// Get retrieves the n owners of key. Fails if there are not at least n Peers.
func (bn *BasicNode) Get(key string, n int) ([]Peer, error) {
	bn.peersMut.RLock()
	defer bn.peersMut.RUnlock()

	owners, err := bn.h.Get(key, n)
	if err != nil {
		return nil, err
	}
	res := make([]Peer, len(owners))
	for i, o := range owners {
		res[i] = bn.peers[o]
	}
	return res, nil
}

// Close closes the BasicNode. No further events will be handled.
func (bn *BasicNode) Close() error {
	return nil
}

// AddPeer adds or updates a peer to bn. The peer will automatically be
// considered for hashing.
func (bn *BasicNode) AddPeer(p Peer) {
	bn.peersMut.Lock()
	defer bn.peersMut.Unlock()

	bn.peers[p.Name] = p
	bn.syncPeers()
}

// syncPeers will update the hasher with the new set of peers and invoke
// the callback. syncPeers must only be invoked when peersMut is held.
func (bn *BasicNode) syncPeers() {
	peersCopy := make([]Peer, 0, len(bn.peers))
	keys := make([]string, 0, len(bn.peers))
	for key, peer := range bn.peers {
		peersCopy = append(peersCopy, peer)
		keys = append(keys, key)
	}
	sort.Strings(keys)
	bn.h.SetNodes(keys)

	if bn.onPeersChanged != nil {
		// TODO(rfratto): run in goroutine, allow this to be non-blocking.
		bn.onPeersChanged(peersCopy)
	}
}

// RemovePeer removes a peer from bn. The peer will automatically no longer
// be considered for hashing.
func (bn *BasicNode) RemovePeer(name string) {
	bn.peersMut.Lock()
	defer bn.peersMut.Unlock()

	delete(bn.peers, name)
	bn.syncPeers()
}
