// Package queue implements a non-blocking message queue.
package queue

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/rfratto/ckit/internal/lamport"
)

// Unbounded indicates the Queue does not have a limit.
const Unbounded int = -1

// Queue implements a multi-producer, single consumer event queue.
type Queue struct {
	sema     *sync.Cond
	elements []entry

	dequeueInUse uint32

	clock lamport.Clock
	limit int
}

type entry struct {
	Value interface{}
	Time  lamport.Time
}

func New(limit int) *Queue {
	return &Queue{
		sema:  &sync.Cond{L: &sync.Mutex{}},
		limit: limit,
	}
}

// Dequeue blocks until ctx is canceled or an item can be dequeued. Dequeue
// will panic if there are multiple concurrent callers.
func (q *Queue) Dequeue(ctx context.Context) (interface{}, error) {
	if !atomic.CompareAndSwapUint32(&q.dequeueInUse, 0, 1) {
		panic("cannot call dequeue concurrently")
	}
	defer atomic.StoreUint32(&q.dequeueInUse, 0)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Ensure that if context is canceled we wake ourselves up so we can
	// exit.
	go func() {
		<-ctx.Done()
		q.sema.Signal()
	}()

	q.sema.L.Lock()
	for {
		if ctx.Err() != nil || len(q.elements) > 0 {
			break
		}
		q.sema.Wait()
	}
	defer q.sema.L.Unlock()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	element := q.elements[0]
	q.elements = q.elements[1:]
	return element.Value, nil
}

// TryDequeue will return an element from q if one exists.
func (q *Queue) TryDequeue() (interface{}, bool) {
	if !atomic.CompareAndSwapUint32(&q.dequeueInUse, 0, 1) {
		panic("cannot call dequeue concurrently")
	}
	defer atomic.StoreUint32(&q.dequeueInUse, 0)

	q.sema.L.Lock()
	defer q.sema.L.Unlock()

	if len(q.elements) > 0 {
		element := q.elements[0]
		q.elements = q.elements[1:]
		return element.Value, true
	}

	return nil, false
}

// Enqueue queues an item. Messages are guarenteed to be dequeued in call order.
// If the queue has reached its limit, the oldest message will be discarded.
func (q *Queue) Enqueue(v interface{}) {
	element := entry{Time: q.clock.Tick(), Value: v}

	q.sema.L.Lock()
	defer q.sema.L.Unlock()

	// Perform a sorted insert into the slice.
	insert := sort.Search(len(q.elements), func(i int) bool {
		return q.elements[i].Time > element.Time
	})
	if insert == len(q.elements) {
		q.elements = append(q.elements, element)
	} else {
		q.elements = append(q.elements[:insert], append([]entry{element}, q.elements[insert:]...)...)
	}

	// Remove the first element if we've grown too big.
	if q.limit != Unbounded && len(q.elements) > q.limit {
		q.elements = q.elements[1:]
	}

	q.sema.Signal()
}
