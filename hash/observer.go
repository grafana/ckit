package hash

import (
	"github.com/rfratto/ckit"
	"github.com/rfratto/ckit/peer"
)

// Observer returns a ckit.Observer which will update the Hasher whenever the
// cluster changes. The returned Observer must be registered to a Node to start
// receiving events.
//
// h.SetPeers should no longer be called after the Observer is registered to a
// node.
func Observer(h Hasher) ckit.Observer {
	return ckit.FuncObserver(func(peers []peer.Peer) (reregister bool) {
		h.SetPeers(peers)
		return true
	})
}
