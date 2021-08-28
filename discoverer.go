package ckit

import (
	"fmt"
	"io"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/hashicorp/memberlist"
	"github.com/rfratto/ckit/internal/lamport"
	"github.com/rfratto/ckit/internal/messages"
	"github.com/rfratto/ckit/internal/queue"
)

// DiscovererConfig controls how peers discover each other.
type DiscovererConfig struct {
	// Name of the discoverer. Must be unique from all other discoverers.
	// This will be the name found in Peer.
	Name string

	// Address to listen for gossip traffic on.
	ListenAddr string
	// Address to tell peers to connect to.
	AdvertiseAddr string

	// Port (UDP & TCP) to listen for gossip traffic on.
	ListenPort int

	// ApplicationAddr is an application-specific address to share with peers.
	// Useful for sharing the address where an API is exposed.
	ApplicationAddr string

	// Log to use for logging events.
	Log log.Logger
}

// A Discoverer is used to find peers in the cluster. They work using gossip;
// once Start is called with an initial set of peers, all peers will eventually
// be found.
//
// The Discoverer will pass found peers to the attached Node for processing.
type Discoverer struct {
	d *discovererDelegate
}

// NewDiscoverer creates a new Discoverer with a Node to send events to.
// The Node's AddPeer and RemovePeer methods will be invoked as peers are
// added, updated, and removed. The Node's Close method will be invoked
// when the Discoverer is closed.
func NewDiscoverer(cfg *DiscovererConfig, n *Node) (*Discoverer, error) {
	if cfg.Log == nil {
		cfg.Log = log.NewNopLogger()
	}

	mlc := memberlist.DefaultLANConfig()
	mlc.Name = cfg.Name
	mlc.BindAddr = cfg.ListenAddr
	mlc.AdvertiseAddr = cfg.AdvertiseAddr
	mlc.BindPort = cfg.ListenPort
	mlc.AdvertisePort = cfg.ListenPort
	mlc.LogOutput = io.Discard

	dd := &discovererDelegate{
		cfg:  cfg,
		log:  cfg.Log,
		node: n,

		conflictQueue: queue.New(1),
	}

	mlc.Events = dd
	mlc.Delegate = dd
	mlc.Conflict = dd

	ml, err := memberlist.Create(mlc)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}

	dd.ml = ml
	dd.TransmitLimitedQueue.NumNodes = ml.NumMembers
	dd.TransmitLimitedQueue.RetransmitMult = mlc.RetransmitMult

	return &Discoverer{d: dd}, nil
}

// Start begins discovery of peers. An initial list of peers to connect to
// is provided. Discoverers will gossip the full set of peers with each
// other.
func (d *Discoverer) Start(peers []string) error {
	_, err := d.d.ml.Join(peers)
	if err != nil {
		level.Error(d.d.log).Log("msg", "failed to join memberlist", "err", err)
		return fmt.Errorf("failed to join memberlist: %w", err)
	}

	conflict, ok := d.d.conflictQueue.TryDequeue()
	if ok {
		conflict := conflict.(*memberlist.Node)
		return fmt.Errorf("failed to join memberlist: name conflict with %s", conflict.Address())
	}

	level.Info(d.d.log).Log("msg", "successfully joined memberlist")
	return nil
}

// Close calls Close on the attached Node and then stops the Discoverer.
func (d *Discoverer) Close() error {
	_ = d.d.ml.Leave(time.Second * 5)
	return d.d.ml.Shutdown()
}

type discovererDelegate struct {
	memberlist.TransmitLimitedQueue

	cfg *DiscovererConfig
	ml  *memberlist.Memberlist
	log log.Logger

	conflictQueue *queue.Queue

	node *Node
}

func (dd *discovererDelegate) NodeMeta(limit int) []byte {
	raw, err := messages.Encode(&messages.Metadata{
		Time:            lamport.Tick(),
		ApplicationAddr: dd.cfg.ApplicationAddr,
	})
	if err != nil {
		level.Error(dd.log).Log("msg", "failed to encode metadata", "err", err)
		return nil
	} else if len(raw) > limit {
		level.Error(dd.log).Log("msg", "not enough space to encode metadata", "size", len(raw), "limit", limit)
		return nil
	}
	return raw
}

func (dd *discovererDelegate) NotifyMsg(raw []byte) {
	// no-op: no user messages being sent
}

func (dd *discovererDelegate) LocalState(join bool) []byte {
	// no-op: no local state being sent
	return nil
}

func (dd *discovererDelegate) MergeRemoteState(raw []byte, join bool) {
	// no-op: no local state being sent
}

func (dd *discovererDelegate) NotifyConflict(existing, other *memberlist.Node) {
	dd.conflictQueue.Enqueue(other)
}

func (dd *discovererDelegate) NotifyJoin(node *memberlist.Node) {
	level.Info(dd.log).Log("msg", "new member in memberlist", "name", node.Name, "addr", node.Address())

	// Pass through to NotifyUpdate since a join and update event are handled
	// the same way.
	dd.NotifyUpdate(node)
}

func (dd *discovererDelegate) NotifyUpdate(node *memberlist.Node) {
	peer, err := convertNode(node, dd.cfg.Name)
	if err != nil || peer == nil {
		level.Warn(dd.log).Log("msg", "failed to convert node to cluster peer", "err", err)
		return
	}
	dd.node.AddPeer(*peer)
}

func convertNode(n *memberlist.Node, local string) (*Peer, error) {
	if n.Meta == nil {
		return nil, fmt.Errorf("ignoring peer with no metadata set")
	}

	buf, ty, err := messages.Validate(n.Meta)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	} else if ty != messages.TypeMetadata {
		return nil, fmt.Errorf("unexpected metadata type %q", ty)
	}

	var md messages.Metadata
	if err := messages.Decode(buf, &md); err != nil {
		return nil, fmt.Errorf("could not read metadata: %w", err)
	}
	lamport.Observe(md.Time)

	return &Peer{
		Name:            n.Name,
		GossipAddr:      n.Address(),
		ApplicationAddr: md.ApplicationAddr,
		Self:            n.Name == local,
	}, nil
}

func (dd *discovererDelegate) NotifyLeave(node *memberlist.Node) {
	level.Info(dd.log).Log("msg", "memberlist peer left", "name", node.Name, "addr", node.Address())
	dd.node.RemovePeer(node.Name)
}
