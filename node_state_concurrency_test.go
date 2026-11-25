package ckit

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/ckit/internal/messages"
	"github.com/grafana/ckit/internal/queue"
	"github.com/grafana/ckit/peer"
	"github.com/grafana/ckit/shard"
	"github.com/stretchr/testify/require"
)

func TestNode_MergeRemoteStateConcurrentChangeState(t *testing.T) {
	for _, transition := range []stateTransition{
		{From: peer.StateViewer, To: peer.StateParticipant},
		{From: peer.StateParticipant, To: peer.StateTerminating},
	} {
		t.Run(transition.To.String(), func(t *testing.T) {
			merging := make(chan struct{})
			releaseMerge := make(chan struct{})
			changing := make(chan struct{})
			var mergeOnce, changeOnce, releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseMerge) }) }
			defer release()

			// Pause the real merge at its existing log call. This schedules a
			// concurrent transition without changing either method's locking.
			logger := log.LoggerFunc(func(keyvals ...interface{}) error {
				for _, value := range keyvals {
					message, _ := value.(string)
					switch message {
					case "got stale message about self":
						mergeOnce.Do(func() {
							close(merging)
							<-releaseMerge
						})
					case "changing node state":
						changeOnce.Do(func() { close(changing) })
					}
				}
				return nil
			})
			n := &Node{
				cfg:                  Config{Name: "self", Sharder: shard.Ring(512)},
				log:                  logger,
				m:                    newMetrics(""),
				localState:           transition.From,
				notifyObserversQueue: queue.New(1),
				peers: map[string]peer.Peer{
					"self": {Name: "self", Self: true, State: transition.From},
				},
				peerStates: make(map[string]messages.State),
			}
			n.broadcasts.NumNodes = func() int { return 1 }
			raw, err := encodeLocalState(&localState{
				CurrentTime: 10,
				NodeStates: []messages.State{
					{NodeName: "self", Time: 10, NewState: peer.StateTerminating},
				},
			})
			require.NoError(t, err)

			merged := make(chan struct{})
			go func() {
				(&nodeDelegate{n}).MergeRemoteState(raw, true)
				close(merged)
			}()
			select {
			case <-merging:
			case <-time.After(5 * time.Second):
				t.Fatal("merge did not reach the stale self-state")
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			changed := make(chan error, 1)
			started := make(chan struct{})
			go func() {
				close(started)
				changed <- n.ChangeState(ctx, transition.To)
			}()
			<-started
			// Before the fix, the transition can acquire stateMut while the
			// merge holds peerMut. With the fix it waits for the merge's RLock.
			select {
			case <-changing:
			case <-time.After(50 * time.Millisecond):
			}
			release()

			select {
			case err := <-changed:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				stack := make([]byte, 64*1024)
				stack = stack[:runtime.Stack(stack, true)]
				t.Fatalf("state change deadlocked with merge:\n%s", stack)
			}
			select {
			case <-merged:
			case <-time.After(time.Second):
				t.Fatal("merge did not finish")
			}
			require.Equal(t, transition.To, n.CurrentState())
			require.Equal(t, transition.To, n.Peers()[0].State)
		})
	}
}
