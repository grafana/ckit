package ckit_test

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/rfratto/ckit"
	"github.com/rfratto/ckit/chash"
	"google.golang.org/grpc"
)

func Example() {
	// Our cluster works over gRPC, so we must first create a gRPC server.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer()

	// Create a node to use for consistent hashing and distributing messages.
	// Nodes must be given a consistent hashing algorithm to use. Here we
	// use Rendezvous hashing, which can lookup an owner in O(N) time and has
	// very good distribution.
	//
	// We also provide a callback function to allow our application to implement
	// custom logic for when the set of active peers changes. Note that our
	// callback function gets called in the background and may not always
	// be executed when running this example.
	node := ckit.NewNode(chash.Rendezvous, func(peers ckit.PeerSet) {
		names := make([]string, len(peers))
		for i, p := range peers {
			names[i] = p.Name
		}
		log.Printf("Peers changed: %s", strings.Join(names, ","))
	})
	defer node.Close()

	// We must give our node to a Discoverer. Discoverers will find other
	// Discoverers using gossip and inform our node when peers are found or
	// when peers leave.
	cfg := &ckit.DiscovererConfig{
		// Name of the discoverer. Must be unique.
		Name: "first-node",

		// AdvertiseAddr will be the address shared with other nodes.
		AdvertiseAddr: lis.Addr().String(),
	}
	disc, err := ckit.NewDiscoverer(grpcServer, cfg, node)
	if err != nil {
		panic(err)
	}

	// Run our gRPC server. This can only happen after the discoverer is created.
	go func() {
		err := grpcServer.Serve(lis)
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			panic(err)
		}
	}()
	defer grpcServer.GracefulStop()

	// Discover peers in the cluster. We're the only node, so pass an empty string
	// slice. Otherwise, we'd give the address of another peer to connect to.
	err = disc.Start(nil)
	if err != nil {
		panic(err)
	}
	defer disc.Close()

	// Get the list of owners for some-key. We're the only node, so it should
	// return ourselves.
	owners, err := node.Get("some-key", 1)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Owners of some-key: %s\n", owners[0].Name)

	// Output:
	// Owners of some-key: first-node
}
