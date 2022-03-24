// Package httpgrpc allows using gRPC for transporting HTTP/1.1 messages. This
// is useful for nodes to call HTTP endpoints without having to expose or
// communicate extra addresses.
package httpgrpc

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/rfratto/ckit/clientpool"
	pb "github.com/rfratto/ckit/internal/httpgrpcpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Serve configures httpgrpc against the provided gRPC server. Serve must be
// called before the gRPC server starts listening for traffic. Requests will
// be served by calling the provided Handler.
func Serve(reg grpc.ServiceRegistrar, h http.Handler) {
	pb.RegisterTransportServer(reg, &handler{h: h})
}

type handler struct {
	pb.UnimplementedTransportServer
	h http.Handler
}

func (h *handler) Handle(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	bodyReader := bytes.NewBuffer(req.GetBody())
	r, err := http.NewRequestWithContext(ctx, req.GetMethod(), req.GetUri(), bodyReader)
	if err != nil {
		return nil, err
	}
	copyHeaders(r.Header, req.GetHeader())
	r.RequestURI = req.GetUri()
	r.ContentLength = int64(len(req.GetBody()))
	r.RemoteAddr = getCallerAddr(ctx)
	if host := r.Header.Get("Host"); host != "" {
		r.Host = host
	}

	if r.Header.Get("Upgrade") != "" {
		return nil, status.Errorf(codes.InvalidArgument, "Upgrade requests are not allowed")
	}

	recorder := httptest.NewRecorder()
	h.h.ServeHTTP(recorder, r)

	return &pb.Response{
		StatusCode: int32(recorder.Code),
		Header:     convertHeaders(recorder.Header()),
		Body:       recorder.Body.Bytes(),
	}, nil
}

func copyHeaders(dest http.Header, src []*pb.Header) {
	for _, h := range src {
		dest[http.CanonicalHeaderKey(h.Name)] = h.Values
	}
}

func convertHeaders(in http.Header) []*pb.Header {
	res := make([]*pb.Header, 0, len(in))
	for h, vv := range in {
		res = append(res, &pb.Header{
			Name:   h,
			Values: vv,
		})
	}
	return res
}

// getCallerAddr attempts to extract the caller address from the incoming gRPC
// context.
func getCallerAddr(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}
	return p.Addr.String()
}

// ClientTransport returns an http.RoundTripper that uses the gRPC client pool
// for sending HTTP requests over gRPC.
func ClientTransport(p *clientpool.Pool) http.RoundTripper {
	return &poolRT{p: p}
}

type poolRT struct{ p *clientpool.Pool }

func (rt *poolRT) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Body != nil {
		defer r.Body.Close()
	}

	cc, err := rt.p.Get(r.Context(), r.URL.Host)
	if err != nil {
		return nil, err
	}
	cli := pb.NewTransportClient(cc)

	pbReq := &pb.Request{
		Method: r.Method,
		Uri:    r.URL.RequestURI(),
		Header: convertHeaders(r.Header.Clone()),
	}
	if r.Body != nil {
		bb, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		pbReq.Body = bb

		if err := r.Body.Close(); err != nil {
			return nil, err
		}
	}

	pbResp, err := cli.Handle(r.Context(), pbReq)
	if err != nil {
		return nil, err
	}

	resp := &http.Response{
		Status:     http.StatusText(int(pbResp.GetStatusCode())),
		StatusCode: int(pbResp.GetStatusCode()),
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,

		Header:        make(http.Header, len(pbResp.GetHeader())),
		Body:          io.NopCloser(bytes.NewReader(pbResp.GetBody())),
		ContentLength: int64(len(pbResp.GetBody())),
		Request:       r,
	}
	copyHeaders(resp.Header, pbResp.GetHeader())
	return resp, nil
}
