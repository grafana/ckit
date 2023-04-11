package gossiphttp

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type testEnvironment struct {
	Listener net.Listener
	Server   *httptest.Server
	Config   *memberlist.Config
}

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

	cli := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(network, addr string, _ *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	tr, _, err := NewTransport(Options{
		Client:        cli,
		PacketTimeout: 5 * time.Second,
	})
	require.NoError(t, err)

	baseRoute, handler := tr.Handler()
	srv := httptest.NewServer(h2c.NewHandler(handler, &http2.Server{}))
	mcfg := memberlist.DefaultLANConfig()
	mcfg.Name = srv.Listener.Addr().String()
	mcfg.Transport = tr
	mcfg.AdvertiseAddr = srv.Listener.Addr().(*net.TCPAddr).IP.String()
	mcfg.AdvertisePort = srv.Listener.Addr().(*net.TCPAddr).Port

	mux := http.NewServeMux()
	mux.Handle(baseRoute, h2c.NewHandler(handler, &http2.Server{}))

	return &testEnvironment{
		Listener: srv.Listener,
		Server:   srv,
		Config:   mcfg,
	}
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

	return ml
}
