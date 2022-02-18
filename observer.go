package ckit

// An Observer watches a Node, waiting for its peers to change.
type Observer interface {
	// NotifyPeersChanged is invoked any time the set of Peers for a node
	// changes. The slice of peers should not be modified.
	//
	// The real list of peers may have changed; call Node.Peers to get the
	// current list.
	//
	// If NotifyPeersChanged returns false, the Observer will no longer receive
	// any notifications. This can be used for single-use watches.
	NotifyPeersChanged(peers []Peer) (reregister bool)
}

// FuncObserver implements Observer.
type FuncObserver func(peers []Peer) (reregister bool)

// NotifyPeersChanged implements Observer.
func (f FuncObserver) NotifyPeersChanged(peers []Peer) (reregister bool) { return f(peers) }
