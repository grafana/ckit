package queue

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDequeue_Concurrent_Panic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := New(Unbounded)

	go q.Dequeue(ctx)
	time.Sleep(500 * time.Millisecond)

	require.Panics(t, func() {
		q.Dequeue(ctx)
	})
}

func TestDequeue_Timeout(t *testing.T) {
	testCtx, testCancel := context.WithTimeout(context.Background(), time.Second)
	go func() {
		select {
		case <-time.After(5 * time.Second):
			require.FailNow(t, "test timed out")
		case <-testCtx.Done():
			// no-op
		}
	}()
	defer testCancel()

	q := New(Unbounded)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()
	_, err := q.Dequeue(ctx)
	require.EqualError(t, err, ctx.Err().Error())
}

func TestDequeue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	q := New(Unbounded)
	q.Enqueue("hello")

	v, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.Equal(t, "hello", v)
}

func TestQueue_Close(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	q := New(Unbounded)
	q.Enqueue("hello")
	require.NoError(t, q.Close())

	v, err := q.Dequeue(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, v)
}

func TestEnqueue_Race(t *testing.T) {
	q := New(Unbounded)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			q.Enqueue(i)
		}(i)
	}
	wg.Wait()

	require.Len(t, q.elements, 100)

	// Each element must be larger than the one before it
	for i := len(q.elements) - 1; i > 0; i-- {
		require.Greater(t, q.elements[i-0].Time, q.elements[i-1].Time)
	}
}

func TestEnqueue_Limit(t *testing.T) {
	q := New(1)

	for i := 0; i < 100; i++ {
		q.Enqueue(i)
	}
	v, err := q.Dequeue(context.Background())
	require.NoError(t, err)
	require.Equal(t, 99, v)
}
