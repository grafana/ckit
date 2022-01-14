package httpgrpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func Test(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cli := newTestServer(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodGet, r.Method)
			assert.Equal(t, "/hello", r.RequestURI)
			assert.Equal(t, int64(0), r.ContentLength)
			assert.Equal(t, "testhost", r.Host)

			fmt.Fprint(rw, "world")
		}))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "GET", "/hello", nil)
		req.Host = "testhost"
		require.NoError(t, err)
		resp, body := doRequest(t, cli, req)

		require.Equal(t, "world", string(body))
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("echo", func(t *testing.T) {
		cli := newTestServer(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			bb, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}

			assert.Equal(t, http.MethodPost, r.Method)
			assert.Equal(t, "/echo", r.RequestURI)
			assert.Equal(t, int64(len(bb)), r.ContentLength)

			_, _ = rw.Write(bb)
		}))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "POST", "/echo", strings.NewReader("testing!"))
		require.NoError(t, err)
		resp, body := doRequest(t, cli, req)

		require.Equal(t, "testing!", string(body))
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

func doRequest(t *testing.T, cli *http.Client, req *http.Request) (resp *http.Response, body []byte) {
	t.Helper()
	resp, err := cli.Do(req)
	require.NoError(t, err)
	respBB, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp, respBB
}

// newTestServer runs a new test gRPC server where HTTP requests will be
// handled by h. The returned client conn can be used to send requests to the
// server.
//
// The test server will be shut down after the test completes.
func newTestServer(t *testing.T, h http.Handler) *http.Client {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcSrv := grpc.NewServer()
	Serve(grpcSrv, h)
	go func() {
		require.NoError(t, grpcSrv.Serve(lis))
	}()
	t.Cleanup(grpcSrv.GracefulStop)

	connCtx, connCancel := context.WithTimeout(context.Background(), time.Second)
	defer connCancel()

	cc, err := grpc.DialContext(connCtx, lis.Addr().String(), grpc.WithInsecure(), grpc.WithBlock())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, cc.Close())
	})

	return &http.Client{Transport: ClientTransport(cc)}
}
