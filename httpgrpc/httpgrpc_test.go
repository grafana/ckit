package httpgrpc_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rfratto/ckit/clientpool"
	"github.com/rfratto/ckit/httpgrpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func Example() {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "Hello, world!")
	})

	grpcSrv := grpc.NewServer()
	httpgrpc.Serve(grpcSrv, handler)
	go func() {
		_ = grpcSrv.Serve(lis)
	}()
	defer grpcSrv.GracefulStop()

	p, err := clientpool.New(clientpool.DefaultOptions, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	cli := http.Client{Transport: httpgrpc.ClientTransport(p)}

	req, err := http.NewRequest(http.MethodGet, "http://"+lis.Addr().String(), nil)
	if err != nil {
		panic(err)
	}

	resp, err := cli.Do(req)
	if err != nil {
		panic(err)
	}

	bb, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(bb))

	// Output:
	// Hello, world!
}

func Test(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cli, baseURL := newTestServer(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodGet, r.Method)
			assert.Equal(t, "/hello", r.RequestURI)
			assert.Equal(t, int64(0), r.ContentLength)

			fmt.Fprint(rw, "world")
		}))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "GET", baseURL+"/hello", nil)
		require.NoError(t, err)
		resp, body := doRequest(t, cli, req)

		require.Equal(t, "world", string(body))
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("echo", func(t *testing.T) {
		cli, baseURL := newTestServer(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
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

		req, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/echo", strings.NewReader("testing!"))
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
func newTestServer(t *testing.T, h http.Handler) (cli *http.Client, baseURL string) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcSrv := grpc.NewServer()
	httpgrpc.Serve(grpcSrv, h)
	go func() {
		require.NoError(t, grpcSrv.Serve(lis))
	}()
	t.Cleanup(grpcSrv.GracefulStop)

	p, err := clientpool.New(clientpool.DefaultOptions, grpc.WithInsecure())
	require.NoError(t, err)
	return &http.Client{Transport: httpgrpc.ClientTransport(p)}, fmt.Sprintf("http://%s", lis.Addr().String())
}
