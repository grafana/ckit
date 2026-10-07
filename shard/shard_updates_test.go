package shard

import (
	"fmt"
	"testing"

	"github.com/grafana/ckit/internal/chash"
	"github.com/grafana/ckit/peer"
	"github.com/stretchr/testify/require"
)

type countingHash struct {
	chash.Hash
	updates int
}

func (h *countingHash) SetNodes(nodes []string) {
	h.updates++
	h.Hash.SetNodes(nodes)
}

func TestSharderOnlyRebuildsChangedMembership(t *testing.T) {
	read := &countingHash{Hash: chash.Ring(512)}
	write := &countingHash{Hash: chash.Ring(512)}
	s := &chasher{read: read, readWrite: write}
	ps := []peer.Peer{
		{Name: "a", State: peer.StateParticipant},
		{Name: "b", State: peer.StateParticipant},
	}
	s.SetPeers(ps)
	require.Equal(t, 1, read.updates)
	require.Equal(t, 1, write.updates)

	before := make([]string, 1000)
	for i := range before {
		owners, err := s.Lookup(StringKey(fmt.Sprint(i)), 1, OpReadWrite)
		require.NoError(t, err)
		before[i] = owners[0].Name
	}
	ps[0].Addr = "new-address"
	ps[0].Self = true
	s.SetPeers(append(ps, peer.Peer{Name: "viewer", State: peer.StateViewer}))
	require.Equal(t, 1, read.updates, "viewer and address changes must not rebuild rings")
	require.Equal(t, 1, write.updates)
	for i := range before {
		owners, err := s.Lookup(StringKey(fmt.Sprint(i)), 1, OpReadWrite)
		require.NoError(t, err)
		require.Equal(t, before[i], owners[0].Name)
		if owners[0].Name == "a" {
			require.True(t, owners[0].Self)
			require.Equal(t, "new-address", owners[0].Addr)
		}
	}

	ps[0].State = peer.StateTerminating
	s.SetPeers(ps)
	require.Equal(t, 1, read.updates, "terminating peers remain eligible for reads")
	require.Equal(t, 2, write.updates)
	for i := range before {
		owners, err := s.Lookup(StringKey(fmt.Sprint(i)), 1, OpReadWrite)
		require.NoError(t, err)
		require.Equal(t, "b", owners[0].Name)
	}
	owners, err := s.Lookup(0, 2, OpRead)
	require.NoError(t, err)
	require.Len(t, owners, 2)

	s.SetPeers(nil)
	require.Equal(t, 2, read.updates)
	require.Equal(t, 3, write.updates)
	_, err = s.Lookup(0, 1, OpRead)
	require.Error(t, err)
	s.SetPeers(nil)
	require.Equal(t, 2, read.updates)
	require.Equal(t, 3, write.updates)

	// Re-adding a removed participant must rebuild both previously empty rings.
	s.SetPeers([]peer.Peer{{Name: "c", State: peer.StateParticipant}})
	require.Equal(t, 3, read.updates)
	require.Equal(t, 4, write.updates)
	owners, err = s.Lookup(0, 1, OpReadWrite)
	require.NoError(t, err)
	require.Equal(t, "c", owners[0].Name)
}

func BenchmarkSharderViewerChurn(b *testing.B) {
	ps := make([]peer.Peer, 101)
	for i := 0; i < 100; i++ {
		ps[i] = peer.Peer{Name: fmt.Sprintf("node-%03d", i), State: peer.StateParticipant}
	}
	ps[100] = peer.Peer{Name: "viewer", State: peer.StateViewer}
	s := Ring(512)
	s.SetPeers(ps)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps[100].Addr = fmt.Sprint(i)
		s.SetPeers(ps)
	}
}
