package httpgrpcpb

//go:generate protoc --go_out=. --go_opt=module=github.com/rfratto/ckit/internal/httpgrpcpb --go-grpc_out=. --go-grpc_opt=module=github.com/rfratto/ckit/internal/httpgrpcpb  ./httpgrpc.proto
