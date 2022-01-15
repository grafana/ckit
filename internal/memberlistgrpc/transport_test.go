package memberlistgrpc

import (
	"net"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/rfratto/ckit/clientpool"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestTransport(t *testing.T) {
	envA := newTestEnvironment(t)
	nodeA := envA.Start(t, nil)

	envB := newTestEnvironment(t)
	nodeB := envB.Start(t, []string{nodeA.LocalNode().Address()})

	envC := newTestEnvironment(t)
	nodeC := envC.Start(t, []string{nodeA.LocalNode().Address()})

	time.Sleep(500 * time.Millisecond)

	require.Len(t, nodeC.Members(), 3)
	require.Len(t, nodeB.Members(), 3)
	require.Len(t, nodeA.Members(), 3)
}

// newTestEnvironment generates a new unstarted test environment.
func newTestEnvironment(t *testing.T) *testEnvironment {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	pool, err := clientpool.New(clientpool.DefaultOptions, grpc.WithInsecure())
	require.NoError(t, err)

	grpcSrv := grpc.NewServer()
	tx, err := NewTransport(grpcSrv, Options{
		Pool:          pool,
		PacketTimeout: 1 * time.Second,
	})
	require.NoError(t, err)

	mcfg := memberlist.DefaultLANConfig()
	mcfg.Name = lis.Addr().String()
	mcfg.Transport = tx
	mcfg.AdvertiseAddr = lis.Addr().(*net.TCPAddr).IP.String()
	mcfg.AdvertisePort = lis.Addr().(*net.TCPAddr).Port

	return &testEnvironment{
		Listener: lis,
		Server:   grpcSrv,
		Pool:     pool,
		Config:   mcfg,
	}
}

type testEnvironment struct {
	Listener net.Listener
	Server   *grpc.Server
	Pool     *clientpool.Pool
	Config   *memberlist.Config
}

// Start the test environment. The test environment will be terminated during
// test cleanup.
func (te *testEnvironment) Start(t *testing.T, peers []string) *memberlist.Memberlist {
	t.Helper()

	ml, err := memberlist.Create(te.Config)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ml.Leave(5*time.Second))
		require.NoError(t, ml.Shutdown())
	})

	_, err = ml.Join(peers)
	require.NoError(t, err)

	go func() {
		require.NoError(t, te.Server.Serve(te.Listener))
	}()
	t.Cleanup(te.Server.GracefulStop)

	return ml
}
