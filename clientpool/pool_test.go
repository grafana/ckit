package clientpool

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestClientPool(t *testing.T) {
	server := newTestServer(t)

	t.Run("Connection reuse", func(t *testing.T) {
		p := newTestPool(t)
		cc, err := p.Get(context.Background(), server)
		require.NoError(t, err)

		cc2, err := p.Get(context.Background(), server)
		require.NoError(t, err)

		require.True(t, cc == cc2, "connpool didn't return existing cached client")
	})

	t.Run("LastUsed updates", func(t *testing.T) {
		p := newTestPool(t)
		cc, err := p.Get(context.Background(), server)
		require.NoError(t, err)

		ent, ok := p.reverseLookup[cc]
		require.True(t, ok)
		firstUsed := ent.LastUsed

		// Do a health check, which should force the LastUsed to update
		_, err = grpc_health_v1.NewHealthClient(cc).Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
		require.NoError(t, err)

		require.True(t, ent.LastUsed.After(firstUsed), "LastUsed did not update")
	})

	t.Run("Recent clients stay", func(t *testing.T) {
		p := newTestPool(t)
		_, err := p.Get(context.Background(), server)
		require.NoError(t, err)

		p.removeStaleClients()
		require.Len(t, p.clients, 1)
	})

	t.Run("Stale clients get removed", func(t *testing.T) {
		p := newTestPool(t)
		cc, err := p.Get(context.Background(), server)
		require.NoError(t, err)

		ent, ok := p.reverseLookup[cc]
		require.True(t, ok)
		ent.LastUsed = time.Now().Add(-24 * time.Hour)

		p.removeStaleClients()
		require.Len(t, p.clients, 0)
	})
}

func newTestServer(t *testing.T) (serverAddr string) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcSrv := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcSrv, health.NewServer())
	go func() {
		require.NoError(t, grpcSrv.Serve(lis))
	}()
	t.Cleanup(grpcSrv.GracefulStop)

	return lis.Addr().String()
}

func newTestPool(t *testing.T) *Pool {
	t.Helper()

	p, err := New(DefaultOptions, grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})
	return p
}
