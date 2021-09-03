package ckit

import (
	"context"
	"reflect"
	"sort"
	"sync"

	"github.com/rfratto/ckit/chash"
	"github.com/rfratto/ckit/internal/queue"
)

// Node keeps track of peers that are part of the cluster and allows
// applications to look up owners of a key.
type Node struct {
	h chash.Hash

	peerQueue      *queue.Queue
	onPeersChanged OnPeersChanged
	stopPeerQueue  context.CancelFunc
	lastSet        PeerSet
	runDone        chan struct{}

	peersMut sync.RWMutex
	peers    map[string]Peer
}

// NewNode returns a new Node. hb will be used for the hashing algorithm.
//
// cb will be invoked with the latest set of peers when the peers change
// and cb is not currently running. This means if peers change multiple
// times while cb is executing, cb will only be invoked again with the
// final set of peers.
func NewNode(hb chash.Builder, cb OnPeersChanged) *Node {
	ctx, cancel := context.WithCancel(context.Background())

	bn := &Node{
		h: hb(),

		peerQueue:      queue.New(1),
		onPeersChanged: cb,
		stopPeerQueue:  cancel,
		runDone:        make(chan struct{}),

		peers: make(map[string]Peer),
	}
	go bn.run(ctx)
	return bn
}

func (bn *Node) run(ctx context.Context) {
	defer close(bn.runDone)

	if bn.onPeersChanged == nil {
		return
	}

	for {
		v, err := bn.peerQueue.Dequeue(ctx)
		if err != nil {
			break
		}

		bn.lastSet = v.([]Peer)
		bn.onPeersChanged(bn.lastSet)
	}
}

// Get retrieves the n owners of key. Fails if there are not at least n Peers.
func (bn *Node) Get(key string, n int) ([]Peer, error) {
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

// Close closes the Node. No further events will be handled.
//
// If a OnPeersChanged callback was given to NewNode, Close will synchronously
// call the callback before exiting with the set of peers excluding the local
// node.
func (bn *Node) Close() error {
	bn.stopPeerQueue()
	<-bn.runDone
	bn.flush()
	return nil
}

func (bn *Node) flush() {
	if bn.onPeersChanged == nil {
		return
	}

	bn.peersMut.RLock()
	defer bn.peersMut.RUnlock()

	rem := make(PeerSet, 0, len(bn.peers))
	for _, p := range bn.peers {
		if p.Self {
			continue
		}
		rem = append(rem, p)
	}

	// Optimization for clients: don't call the callback again if we've already
	// just invoked it with the same set.
	sort.Slice(bn.lastSet, func(i, j int) bool { return bn.lastSet[i].Name < bn.lastSet[j].Name })
	sort.Slice(rem, func(i, j int) bool { return rem[i].Name < rem[j].Name })

	if !reflect.DeepEqual(rem, bn.lastSet) {
		bn.onPeersChanged(rem)
	}
}

// AddPeer adds or updates a peer to bn. The peer will automatically be
// considered for hashing.
func (bn *Node) AddPeer(p Peer) {
	bn.peersMut.Lock()
	defer bn.peersMut.Unlock()

	bn.peers[p.Name] = p
	bn.syncPeers()
}

// syncPeers will update the hasher with the new set of peers and invoke
// the callback. syncPeers must only be invoked when peersMut is held.
func (bn *Node) syncPeers() {
	peersCopy := make([]Peer, 0, len(bn.peers))
	keys := make([]string, 0, len(bn.peers))
	for key, peer := range bn.peers {
		peersCopy = append(peersCopy, peer)
		keys = append(keys, key)
	}
	sort.Strings(keys)
	bn.h.SetNodes(keys)

	if bn.onPeersChanged != nil {
		sort.Slice(peersCopy, func(i, j int) bool { return peersCopy[i].Name < peersCopy[j].Name })
		bn.peerQueue.Enqueue(peersCopy)
	}
}

// RemovePeer removes a peer from bn. The peer will automatically no longer
// be considered for hashing.
func (bn *Node) RemovePeer(name string) {
	bn.peersMut.Lock()
	defer bn.peersMut.Unlock()

	delete(bn.peers, name)
	bn.syncPeers()
}
