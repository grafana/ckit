package memconn

import (
	"context"
	"net"
	"net/url"

	"google.golang.org/grpc"
)

// GRPCDialer returns a gRPC DialOption which will connect to the memconn
// Listener for addresses with the memconn string (i.e., `memconn:`). All other
// addresses will fall back to the next net.Dialer. If next is nil, the
// default dialer will be used.
func GRPCDialer(lis *Listener, next *net.Dialer) grpc.DialOption {
	if next == nil {
		next = &net.Dialer{}
	}

	return grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
		u, err := url.Parse(s)
		if err != nil {
			return nil, err
		}
		switch u.Scheme {
		case "memconn":
			return lis.DialContext(ctx)
		default:
			return next.DialContext(ctx, u.Scheme, u.Host)
		}
	})
}
