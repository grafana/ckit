// Command kv implements an example kv store that uses ckit for clustering.
// Definitely not suitable for production use.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/rfratto/ckit"
	"github.com/spf13/cobra"
)

func main() {
	cmd := &cobra.Command{
		Use:   "kv",
		Short: "ckit example kv server and client",
	}

	cmd.AddCommand(
		cmdServe(),

		// Client commands
		cmdSet(),
		cmdDel(),
		cmdGet(),
	)

	_ = cmd.Execute()
}

func cmdServe() *cobra.Command {
	var (
		listenAddr string
		apiPort    int
		joinAddrs  []string
		disc       ckit.DiscovererConfig
	)

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Starts a kv server",
		Run: func(cmd *cobra.Command, args []string) {
			l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))

			disc.ListenAddr = listenAddr
			disc.ApplicationAddr = fmt.Sprintf("%s:%d", disc.AdvertiseAddr, apiPort)
			disc.Log = l

			var (
				store = NewStore()
				r     = mux.NewRouter()
			)

			server, err := NewServer(store, disc)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			_ = NewAPI(server, r)

			lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", listenAddr, apiPort))
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			go http.Serve(lis, r)
			defer lis.Close()

			if err := server.Start(joinAddrs); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			defer server.Close()

			ch := make(chan os.Signal, 1)
			signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
			<-ch
			level.Info(l).Log("msg", "shutting down...")
		},
	}

	cmd.Flags().StringVar(&listenAddr, "listen-addr", "0.0.0.0", "Address to listen for traffic on")
	cmd.Flags().IntVar(&apiPort, "api-port", 8080, "Port to listen for API traffic on")

	cmd.Flags().StringVar(&disc.Name, "node-name", "", "Name of the node")
	cmd.Flags().StringVar(&disc.AdvertiseAddr, "advertise-addr", "127.0.0.1", "Address to adertise to peers for gossip and API access")
	cmd.Flags().IntVar(&disc.ListenPort, "gossip-port", 7935, "Port to perform gossip on")
	cmd.Flags().StringSliceVar(&joinAddrs, "join-addrs", nil, "Peers to join on startup")

	cmd.MarkFlagRequired("node-name")
	cmd.MarkFlagRequired("gossp-port")

	return cmd
}

func cmdSet() *cobra.Command {
	var (
		addr string
	)

	cmd := &cobra.Command{
		Use:   "set [key] [value]",
		Short: "Sets a key in a server",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			cli := NewClient(fmt.Sprintf("http://%s", addr), http.DefaultClient)
			err := cli.Set(context.Background(), args[0], args[1])
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}

	cmd.Flags().StringVar(&addr, "addr", "127.0.0.1:8080", "KV server address to connect to.")

	return cmd
}

func cmdDel() *cobra.Command {
	var (
		addr string
	)

	cmd := &cobra.Command{
		Use:   "del [key]",
		Short: "Deletes a key from a server",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cli := NewClient(fmt.Sprintf("http://%s", addr), http.DefaultClient)
			err := cli.Delete(context.Background(), args[0])
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}

	cmd.Flags().StringVar(&addr, "addr", "127.0.0.1:8080", "KV server address to connect to.")

	return cmd
}

func cmdGet() *cobra.Command {
	var (
		addr string
	)

	cmd := &cobra.Command{
		Use:   "get [key]",
		Short: "Gets a value from a server",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cli := NewClient(fmt.Sprintf("http://%s", addr), http.DefaultClient)
			val, err := cli.Get(context.Background(), args[0])
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			fmt.Println(val)
		},
	}

	cmd.Flags().StringVar(&addr, "addr", "127.0.0.1:8080", "KV server address to connect to.")

	return cmd
}
