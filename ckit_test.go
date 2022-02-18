package ckit_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/rfratto/ckit/chash"
	ckit "github.com/rfratto/ckit/v2"
	"google.golang.org/grpc"
)

func Example() {
	// Our cluster works over gRPC, so we must first create a gRPC server.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer()

	// Create a config to use for joining the cluster. The config must at least
	// have a unique name for the node in the cluster, the address that other
	// nodes can connect to using gRPC, and a consistent hashing algorithm used
	// for distributing messages.
	//
	// Here we use Rendezvous hashing, which can lookup an owner in O(N) time and
	// has very good distribution.
	cfg := ckit.Config{
		// Name of the discoverer. Must be unique.
		Name: "first-node",

		// AdvertiseAddr will be the address shared with other nodes.
		AdvertiseAddr: lis.Addr().String(),

		// Hash to use for ownership checks.
		Hash: chash.Rendezvous(),

		Log: log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)),
	}

	// We can create a node from our config with a gRPC server to use. Nodes do not
	// join the cluster until Start is called.
	node, err := ckit.NewNode(grpcServer, cfg)
	if err != nil {
		panic(err)
	}

	// Nodes can optionally emit events to any number of observers to notify when
	// the list of peers in the cluster has changed.
	//
	// Note that Observers are invoked in the background and so this function
	// might not always execute within this example.
	node.Observe(ckit.FuncObserver(func(peers []ckit.Peer) (reregister bool) {
		names := make([]string, len(peers))
		for i, p := range peers {
			names[i] = p.Name
		}

		level.Info(cfg.Log).Log("msg", "peers changed", "new_peers", strings.Join(names, ","))
		return true
	}))

	// Run our gRPC server. This can only happen after the discoverer is created.
	go func() {
		err := grpcServer.Serve(lis)
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			panic(err)
		}
	}()
	defer grpcServer.GracefulStop()

	// Join the cluster with an initial set of peers to connect to. We're the only
	// node, so pass an empty string slice. Otherwise, we'd give the address of
	// another peer to connect to.
	err = node.Start(nil)
	if err != nil {
		panic(err)
	}
	defer node.Stop()

	// Nodes initially join the cluster in the Pending state. They must move to a
	// Participant state if they wish to participate in hashing.
	err = node.ChangeState(context.Background(), ckit.StateParticipant)
	if err != nil {
		panic(err)
	}

	// Get the list of owners for some-key. We're the only node, so it should
	// return ourselves.
	owners, err := node.Lookup(chash.Key("some-key"), 1)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Owners of some-key: %s\n", owners[0].Name)

	// Output:
	// Owners of some-key: first-node
}
