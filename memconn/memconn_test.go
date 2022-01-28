package memconn

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func BenchmarkConn(b *testing.B) {
	local, remote := Pipe()
	defer local.Close()

	go func() {
		_, _ = io.Copy(io.Discard, remote)
	}()

	start := time.Now()

	var (
		data    = make([]byte, 8192)
		written int
	)
	for i := 0; i < b.N; i++ {
		n, _ := local.Write(data)
		written += n
	}

	b.ReportMetric(float64(written/1e6)/time.Since(start).Seconds(), "MB/s")
}

func TestConn_SetReadTimeout(t *testing.T) {
	local, _ := getConnPair(t)

	err := local.SetReadDeadline(time.Now().Add(time.Millisecond * 100))
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		_, err = local.Read(make([]byte, connBufferSize*10))
		errCh <- err
	}()

	select {
	case <-time.After(time.Second):
		require.Fail(t, "read deadline did not fire")
	case err := <-errCh:
		require.ErrorIs(t, err, os.ErrDeadlineExceeded)
	}
}

func TestConn_SetWriteTimeout(t *testing.T) {
	local, _ := getConnPair(t)

	err := local.SetWriteDeadline(time.Now().Add(time.Millisecond * 100))
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		_, err = local.Write(make([]byte, connBufferSize*10))
		errCh <- err
	}()

	select {
	case <-time.After(time.Second):
		require.Fail(t, "write deadline did not fire")
	case err := <-errCh:
		require.ErrorIs(t, err, os.ErrDeadlineExceeded)
	}
}

func TestConn_SetTimeout(t *testing.T) {
	local, _ := getConnPair(t)

	err := local.SetDeadline(time.Now().Add(time.Millisecond * 100))
	require.NoError(t, err)

	errCh := make(chan error, 1)
	waitTimeout := func() {
		select {
		case <-time.After(time.Second):
			require.Fail(t, "deadline did not fire")
		case err := <-errCh:
			require.ErrorIs(t, err, os.ErrDeadlineExceeded)
		}
	}

	go func() {
		_, err = local.Read(make([]byte, connBufferSize*10))
		errCh <- err
	}()
	waitTimeout()

	err = local.SetDeadline(time.Now().Add(time.Millisecond * 100))
	require.NoError(t, err)

	go func() {
		_, err = local.Write(make([]byte, connBufferSize*10))
		errCh <- err
	}()
	waitTimeout()
}
