// Package peer describes a ckit peer.
package peer

import "encoding/json"

// Peer is a discovered node within the cluster.
type Peer struct {
	Name  string // Name of the Peer. Unique across the cluster.
	Addr  string // host:port address of the peer.
	Self  bool   // True if Peer is the local Node.
	State State  // State of the peer.
}

// String returns the name of p.
func (p Peer) String() string { return p.Name }

// MarshalJSON implements json.Marshaler.
func (p Peer) MarshalJSON() ([]byte, error) {
	type peerStatusJSON struct {
		Name  string `json:"name"`   // Name of the Peer. Unique across the cluster.
		Addr  string `json:"addr"`   // host:port address of the peer.
		Self  bool   `json:"isSelf"` // True if Peer is the local Node.
		State string `json:"state"`  // State of the peer.
	}
	return json.Marshal(&peerStatusJSON{
		Name:  p.Name,
		Addr:  p.Addr,
		Self:  p.Self,
		State: p.State.String(),
	})
}
