package messages_test

import (
	"fmt"
	"testing"

	"github.com/grafana/ckit/internal/lamport"
	"github.com/grafana/ckit/internal/messages"
	"github.com/grafana/ckit/peer"
	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"
)

func BenchmarkBroadcast(b *testing.B) {
	b.Run("Different names", func(b *testing.B) {
		var broadcasts []memberlist.Broadcast

		for i := range b.N {
			bcast, err := messages.Broadcast(&messages.State{
				NodeName: fmt.Sprintf("node-%d", i),
				NewState: peer.StateParticipant,
				Time:     lamport.Time(i),
			}, nil)
			require.NoError(b, err)

			broadcasts = append(broadcasts, bcast)
		}

		var queue memberlist.TransmitLimitedQueue
		queue.RetransmitMult = 4

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			queue.QueueBroadcast(broadcasts[i])
		}
	})

	b.Run("Same name", func(b *testing.B) {
		var broadcasts []memberlist.Broadcast

		for i := range b.N {
			bcast, err := messages.Broadcast(&messages.State{
				NodeName: "node",
				NewState: peer.StateParticipant,
				Time:     lamport.Time(i),
			}, nil)
			require.NoError(b, err)

			broadcasts = append(broadcasts, bcast)
		}

		var queue memberlist.TransmitLimitedQueue
		queue.RetransmitMult = 4

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			queue.QueueBroadcast(broadcasts[i])
		}
	})
}
