package ckit_test

import (
	"fmt"
	"strings"
	"time"

	"github.com/rfratto/ckit"
	"github.com/rfratto/ckit/chash"
)

func Example() {
	// Create a node to use for consistent hashing and distributing messages.
	// Nodes must be given a consistent hashing algorithm to use. Here we
	// use Rendezvous hashing, which can lookup an owner in O(N) time and has
	// very good distribution.
	//
	// We also provide a callback function to allow our application to implement
	// custom logic for when the set of active peers changes.
	node := ckit.NewNode(chash.Rendezvous, func(peers []ckit.Peer) {
		names := make([]string, len(peers))
		for i, p := range peers {
			names[i] = p.Name
		}
		fmt.Printf("Peers changed: %s\n", strings.Join(names, ","))
	})
	defer node.Close()

	// We must give our node to a Discoverer. Discoverers will find other
	// Discoverers using gossip and inform our node when peers are found or
	// when peers leave.
	cfg := &ckit.DiscovererConfig{
		// Name of the discoverer. Must be unique.
		Name: "first-node",

		// Address and port to listen on for gossip. AdvertiseAddr will be the
		// address shared with other nodes.
		ListenAddr:    "127.0.0.1",
		ListenPort:    7950,
		AdvertiseAddr: "127.0.0.1",

		// The address of the API exposing application business logic. Can be
		// any string.
		ApplicationAddr: "127.0.0.1:8080",
	}
	disc, err := ckit.NewDiscoverer(cfg, node)
	if err != nil {
		panic(err)
	}

	// Discover peers in the cluster. We're the only node, so pass an empty string
	// slice. Otherwise, we'd give the address of another peer to connect to.
	err = disc.Start(nil)
	if err != nil {
		panic(err)
	}

	// The NewNode callback gets invoked asynchronously, so we need to wait a
	// little for the event to be called. We'll do the same on shutdown.
	time.Sleep(100 * time.Millisecond)

	defer func() {
		disc.Close()
		time.Sleep(100 * time.Millisecond)
	}()

	// Get the list of owners for some-key. We're the only node, so it should
	// return ourselves.
	owners, err := node.Get("some-key", 1)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Owners of some-key: %s\n", owners[0].Name)

	// Output:
	// Peers changed: first-node
	// Owners of some-key: first-node
	// Peers changed:
}
