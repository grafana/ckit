# ckit-kv

`ckit-kv` provides an example KV store implementation that uses ckit for
clustering. It's just a toy KV store and really, really shouldn't be used for
production.

To install:

```
go install github.com/rfratto/ckit/example/ckit-kv@main
```

Then bring up three terminal windows. Two will act as servers in a cluster, and
one will be the client.

In terminal window 1, run the following to create `node-a`:

```
ckit-kv serve --node-name=node-a --gossip-port=7935 --api-port=8080
```

Then, in terminal window 2, connect a new `node-b` to `node-a`:

```
ckit-kv serve --node-name=node-b --gossip-port=7936 --api-port=8081 --join-addrs=127.0.0.1:7935
```

Finally, you can use the third terminal window to interface with either node:

```
ckit set --addr=127.0.0.1:8080 hello world
ckit get --addr=127.0.0.1:8081 hello
ckit del --addr=127.0.0.1:8081 hello
```
