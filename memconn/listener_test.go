package memconn

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestListener(t *testing.T) {
	local, remote := getConnPair(t)

	// Write to local should be read from remote. Close after writing so we get
	// an io.EOF back on the other side.
	go func() {
		fmt.Fprint(local, "Hello, world!")
		require.NoError(t, local.Close())
	}()

	var sb strings.Builder
	_, err := io.Copy(&sb, remote)
	require.NoError(t, err)
	require.Equal(t, "Hello, world!", sb.String())
}

func getConnPair(t *testing.T) (local, remote net.Conn) {
	t.Helper()

	local, remote = Pipe()
	t.Cleanup(func() {
		_ = local.Close()
		_ = remote.Close()
	})
	return local, remote
}

// TestListener_LocalConnPipe ensures that writes over a local conn are not
// read back by the same local conn.
func TestListener_LocalConnPipe(t *testing.T) {
	local, _ := getConnPair(t)

	// Write to local should be read from remote. Close after writing so we get
	// an io.EOF back on the other side.
	go func() {
		fmt.Fprint(local, "Hello, world!")
	}()

	local.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	var sb strings.Builder
	_, err := io.Copy(&sb, local)
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)
	require.NotEqual(t, "Hello, world!", sb.String())
}

// TestListener_RemoteConnPipe ensures that writes over a local conn are not
// read back by the same local conn.
func TestListener_RemoteConnPipe(t *testing.T) {
	_, remote := getConnPair(t)

	// Write to local should be read from remote. Close after writing so we get
	// an io.EOF back on the other side.
	go func() {
		fmt.Fprint(remote, "Hello, world!")
	}()

	remote.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	var sb strings.Builder
	_, err := io.Copy(&sb, remote)
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)
	require.NotEqual(t, "Hello, world!", sb.String())
}
