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
	"github.com/rfratto/ckit"
	"github.com/rfratto/ckit/peer"
	"github.com/rfratto/ckit/shard"
	"google.golang.org/grpc"
)

func Example() {
	// Our cluster works over gRPC, so we must first create a gRPC server.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer()

	// We want to be able to perform consistent hashing against the state of the
	// cluster. We'll create a ring for our node to update.
	ring := shard.Ring(128)

	// Create a config to use for joining the cluster. The config must at least
	// have a unique name for the node in the cluster, and the address that other
	// nodes can connect to using gRPC.
	cfg := ckit.Config{
		// Name of the discoverer. Must be unique.
		Name: "first-node",

		// AdvertiseAddr will be the address shared with other nodes.
		AdvertiseAddr: lis.Addr().String(),

		// Cluster changes will be immediately synchronized with a sharder (when
		// provided).
		Sharder: ring,

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
	node.Observe(ckit.FuncObserver(func(peers []peer.Peer) (reregister bool) {
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

	// Nodes initially join the cluster in the Viewer state. We can move to the
	// Participant state to signal that we wish to participate in reading or
	// writing data.
	err = node.ChangeState(context.Background(), peer.StateParticipant)
	if err != nil {
		panic(err)
	}

	// Changing our state will have caused our sharder to be updated as well. We
	// can now look up the owner for a key. We should be the owner since we're
	// the only node.
	owners, err := ring.Lookup(shard.StringKey("some-key"), 1, shard.OpReadWrite)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Owner of some-key: %s\n", owners[0].Name)

	// Output:
	// Owner of some-key: first-node
}
