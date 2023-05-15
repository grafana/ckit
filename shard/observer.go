package shard

import (
	"github.com/grafana/ckit"
	"github.com/grafana/ckit/peer"
)

// Observer returns a [ckit.Observer] which updates the nodes assigned to a
// peer whenever the cluster state changes.
//
// To receive updates as quickly as possible, the resulting Observer should be
// passed as a synchronous Observer in [ckit.Config] and not to the
// [ckit.Node.Observe] method.
func Observer(s Sharder) ckit.Observer {
	return &sharderObserver{s: s}
}

type sharderObserver struct{ s Sharder }

func (o *sharderObserver) NotifyPeersChanged(peers []peer.Peer) {
	o.s.SetPeers(peers)
}
