package ckit

import (
	"sort"
)

// An Observer watches a Node, waiting for its peers to change.
type Observer interface {
	// NotifyPeersChanged is invoked any time the set of Peers for a node
	// changes. The slice of peers must not be modified.
	NotifyPeersChanged(peers []Peer)
}

// FuncObserver implements Observer.
type FuncObserver func(peers []Peer)

// NotifyPeersChanged implements Observer.
func (f FuncObserver) NotifyPeersChanged(peers []Peer) { f(peers) }

// ParticipantObserver wraps an observer and filters out events where the list
// of peers in the Participants state haven't changed. When the set of
// participants have changed, next.NotifyPeersChanged will be invoked with the
// full set of peers (i.e., not just participants).
func ParticipantObserver(next Observer) Observer {
	return &participantObserver{next: next}
}

type participantObserver struct {
	lastParticipants []Peer // Participants ordered by name
	next             Observer
}

func (po *participantObserver) NotifyPeersChanged(peers []Peer) {
	// Filter peers down to those in StateParticipant.
	participants := make([]Peer, 0, len(peers))
	for _, p := range peers {
		if p.State == PeerStateParticipant {
			participants = append(participants, p)
		}
	}
	sort.Slice(participants, func(i, j int) bool { return participants[i].Name < participants[j].Name })

	if peersEqual(participants, po.lastParticipants) {
		return
	}

	po.lastParticipants = participants
	po.next.NotifyPeersChanged(peers)
}

func peersEqual(a, b []Peer) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
