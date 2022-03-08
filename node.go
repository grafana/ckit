package ckit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/memberlist"
	"github.com/rfratto/ckit/chash"
	"github.com/rfratto/ckit/clientpool"
	"github.com/rfratto/ckit/internal/lamport"
	"github.com/rfratto/ckit/internal/memberlistgrpc"
	"github.com/rfratto/ckit/internal/messages"
	"github.com/rfratto/ckit/internal/queue"
	"google.golang.org/grpc"
)

var (
	// ErrNotRunning is returned by calling methods against Node when it has yet
	// to be started or after it was stopped.
	ErrNotRunning = errors.New("node not running")
)

// Config configures a Node within the cluster.
type Config struct {
	// Name of the Node. Must be unique across the cluster. Required.
	Name string

	// host:port address other nodes should use to connect to this Node.
	// Required.
	AdvertiseAddr string

	// Hashing algorithm to use when determining ownership for a key. Required.
	Hash chash.Hash

	// Optional logger to use.
	Log log.Logger

	// Optional client pool to use for establishing gRPC connctions to peers. A
	// client pool will be made if one is not provided here.
	Pool *clientpool.Pool
}

func (c *Config) validate() error {
	if len(c.Name) == 0 {
		return fmt.Errorf("node name is required")
	}

	if len(c.AdvertiseAddr) == 0 {
		return fmt.Errorf("advertise address is required")
	}

	if c.Hash == nil {
		return fmt.Errorf("hash function is required")
	}

	if c.Log == nil {
		c.Log = log.NewNopLogger()
	}

	if c.Pool == nil {
		var err error
		c.Pool, err = clientpool.New(clientpool.DefaultOptions, grpc.WithInsecure())
		if err != nil {
			return fmt.Errorf("failed to build default client pool: %w", err)
		}
	}

	return nil
}

// A Node is a participant in a cluster.
type Node struct {
	log                  log.Logger
	cfg                  Config
	ml                   *memberlist.Memberlist
	broadcasts           memberlist.TransmitLimitedQueue
	conflictQueue        *queue.Queue
	notifyObserversQueue *queue.Queue

	stateMut   sync.RWMutex
	runCancel  context.CancelFunc
	localState State

	observersMut sync.Mutex
	observers    []Observer

	// peerStates is updated any time a messages.State broadcast is received, and
	// may have keys for node names that do not exist in the peers map. These
	// keys get gradually cleaned up during local state synchronization.
	//
	// Use peers for the definitive list of current peers.

	// TODO(rfratto): should this block be replaced with a single struct that
	// supports updating peers in-place?

	peerMut    sync.RWMutex
	peerStates map[string]messages.State // State lookup for a node name
	peers      map[string]Peer           // Current list of peers & their states
	peerCache  []Peer                    // Slice version of peers; keep in sync with peers
}

// NewNode creates an unstarted Node to participulate in a cluster. An error
// will be returned if the provided config is invalid.
func NewNode(srv *grpc.Server, cfg Config) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	advertiseAddr, advertisePortString, err := net.SplitHostPort(cfg.AdvertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to read advertise address: %w", err)
	}
	advertisePort, err := net.LookupPort("tcp", advertisePortString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse advertise port: %w", err)
	}

	grpcTransport, err := memberlistgrpc.NewTransport(srv, memberlistgrpc.Options{
		Log:           cfg.Log,
		Pool:          cfg.Pool,
		PacketTimeout: 3 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to build transport: %w", err)
	}

	mlc := memberlist.DefaultLANConfig()
	mlc.Name = cfg.Name
	mlc.Transport = grpcTransport
	mlc.AdvertiseAddr = advertiseAddr
	mlc.AdvertisePort = advertisePort
	mlc.LogOutput = io.Discard

	n := &Node{
		log: cfg.Log,
		cfg: cfg,

		conflictQueue:        queue.New(1),
		notifyObserversQueue: queue.New(1),

		peerStates: make(map[string]messages.State),
		peers:      make(map[string]Peer),
	}

	nd := &nodeDelegate{Node: n}
	mlc.Events = nd
	mlc.Delegate = nd
	mlc.Conflict = nd

	ml, err := memberlist.Create(mlc)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}

	n.ml = ml
	n.broadcasts.NumNodes = ml.NumMembers
	n.broadcasts.RetransmitMult = mlc.RetransmitMult

	return n, nil
}

// Start runs the Node for clustering with a list of peers to connect to. peers
// can be an empty list if there is are no peers to connect to; other peers can
// then connect to this node in their own Start methods.
//
// Start may be called multiple times to reconnect to a different set of peers.
// Node will be set into StatePending every time Start is called.
func (n *Node) Start(peers []string) error {
	n.stateMut.Lock()
	defer n.stateMut.Unlock()

	// Force ourselves back into the pending state.
	if err := n.changeState(StatePending, nil); err != nil {
		return err
	}

	_, err := n.ml.Join(peers)
	if err != nil {
		return fmt.Errorf("failed to join memberlist: %w", err)
	}

	// Join won't return until we've done a push/pull; if there was a name
	// collision during the join, there will be an enqueued element in the
	// conflict queue.
	conflict, ok := n.conflictQueue.TryDequeue()
	if ok {
		_ = n.ml.Shutdown()

		conflict := conflict.(*memberlist.Node)
		return fmt.Errorf("failed to join memberlist: name conflict with %s", conflict.Address())
	}

	if n.runCancel == nil {
		ctx, cancel := context.WithCancel(context.Background())
		go n.run(ctx)
		n.runCancel = cancel
	}

	return nil
}

func (n *Node) run(ctx context.Context) {
	for {
		v, err := n.notifyObserversQueue.Dequeue(ctx)
		if err != nil {
			break
		}

		n.notifyObservers(v.([]Peer))
	}
}

// Stop stops the Node, removing it from the cluster. Callers should first
// first transition to StateTerminating to gracefully leave the cluster.
// Observers will no longer be notified about cluster changes after Stop
// returns.
//
// Stop may not be called more than once.
func (n *Node) Stop() error {
	n.stateMut.Lock()
	defer n.stateMut.Unlock()

	if n.runCancel != nil {
		n.runCancel()
		n.runCancel = nil
	}

	// TODO(rfratto): configurable leave timeout
	leaveTimeout := time.Second * 5
	if err := n.ml.Leave(leaveTimeout); err != nil {
		level.Error(n.log).Log("msg", "failed to broadcast leave message to cluster", "err", err)
	}
	return n.ml.Shutdown()
}

// CurrentState returns n's current State. Other nodes may not have the same
// State value for n as the current state propagates throughout the cluster.
func (n *Node) CurrentState() State {
	n.stateMut.RLock()
	defer n.stateMut.RUnlock()

	return n.localState
}

// ChangeState changes the state of the node. ChangeState will block until the
// message has been broadcast or until the provided ctx is canceled. Canceling
// the context does not stop the message from being broadcast; it just stops
// waiting for it.
//
// The "to" state must be valid to move to from the current state. Acceptable
// transitions are:
//
//   StatePending -> StateParticipant|StateViewer|StateTerminating|StateGone
//   StateParticipant -> StateTerminating|StateGone
//   StateViewer -> StateTerminating|StateGone
//   StateTerminating -> StateGone
func (n *Node) ChangeState(ctx context.Context, to State) error {
	n.stateMut.Lock()
	defer n.stateMut.Unlock()

	t := stateTransition{From: n.localState, To: to}
	if _, valid := validStateTransitions[t]; !valid {
		return ErrStateTransition(t)
	}

	return n.waitChangeState(ctx, to)
}

type stateTransition struct{ From, To State }

var validStateTransitions = map[stateTransition]struct{}{
	{StatePending, StateParticipant}:     {},
	{StatePending, StateViewer}:          {},
	{StatePending, StateTerminating}:     {},
	{StateParticipant, StateTerminating}: {},
	{StateViewer, StateTerminating}:      {},
}

func (n *Node) waitChangeState(ctx context.Context, to State) error {
	waitBroadcast := make(chan struct{}, 1)
	afterBroadcast := func() {
		select {
		case waitBroadcast <- struct{}{}:
		default:
		}
	}

	if err := n.changeState(to, afterBroadcast); err != nil {
		return err
	}

	// We need at least one remote peer to broadcast the state change to. If it's
	// just us, we can return immediately.
	n.peerMut.RLock()
	hasPeers := len(n.peers) > 1
	n.peerMut.RUnlock()

	if !hasPeers {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitBroadcast:
		return nil
	}
}

func (n *Node) changeState(to State, onDone func()) error {
	n.localState = to

	stateMsg := messages.State{
		NodeName: n.cfg.Name,
		NewState: int(to),
		Time:     lamport.Tick(),
	}

	// Treat the stateMsg as if it was received externally to track our own state
	// along with other nodes.
	n.handleStateMessage(stateMsg)

	bcast, err := messages.Broadcast(&stateMsg, onDone)
	if err != nil {
		return err
	}

	n.broadcasts.QueueBroadcast(bcast)
	return nil
}

func (n *Node) handleStateMessage(msg messages.State) {
	lamport.Observe(msg.Time)

	n.peerMut.Lock()
	defer n.peerMut.Unlock()

	curr, exist := n.peerStates[msg.NodeName]
	if exist && msg.Time <= curr.Time {
		// Ignore a state message if we have a newer one.
		return
	}

	n.peerStates[msg.NodeName] = msg

	if p, ok := n.peers[msg.NodeName]; ok {
		p.State = State(msg.NewState)
		n.peers[msg.NodeName] = p
		n.handlePeersChanged()
	}
}

// Peers returns all Peers currently known by n. The Peers list will include
// peers regardless of their current State. The returned slice should not be
// modified.
func (n *Node) Peers() []Peer {
	n.peerMut.RLock()
	defer n.peerMut.RUnlock()
	return n.peerCache
}

// handlePeersChanged should be called when the peers map is updated. The hash
// and peer cache will be updated before notifying observers that peers have
// changed.
//
// peerMut should be held when this function is called.
func (n *Node) handlePeersChanged() {
	var (
		nodeNames = make([]string, 0, len(n.peers))
		newPeers  = make([]Peer, 0, len(n.peers))
	)

	for nodeName, peer := range n.peers {
		newPeers = append(newPeers, peer)

		// nodeNames is used for updating the hash function. We want to ignore
		// any node which is not StateParticipant.
		if peer.State == StateParticipant {
			nodeNames = append(nodeNames, nodeName)
		}
	}

	// Sort both slices by name.
	sort.Slice(nodeNames, func(i, j int) bool {
		return nodeNames[i] < nodeNames[j]
	})
	sort.Slice(newPeers, func(i, j int) bool {
		return newPeers[i].Name < newPeers[j].Name
	})

	n.cfg.Hash.SetNodes(nodeNames)

	n.peerCache = newPeers
	n.notifyObserversQueue.Enqueue(newPeers)
}

// Observe registers o to be informed when the cluster changes. o will be
// notified when a new peer is discovered, an existing peer shuts down, or the
// state of a peer changes.
//
// o must call Peers to get the current set of peers, which may have changed
// since o was first notified. It is the responsibility of o to filter out
// Peers it is not interested in.
func (n *Node) Observe(o Observer) {
	n.observersMut.Lock()
	defer n.observersMut.Unlock()
	n.observers = append(n.observers, o)
}

func (n *Node) notifyObservers(peers []Peer) {
	n.observersMut.Lock()
	defer n.observersMut.Unlock()

	newObservers := make([]Observer, 0, len(n.observers))
	for _, o := range n.observers {
		rereg := o.NotifyPeersChanged(peers)
		if rereg {
			newObservers = append(newObservers, o)
		}
	}

	n.observers = newObservers
}

// Lookup gets the set of numOwners for key. A key can be created with a
// chash.KeyBuilder or by calling chash.Key.
//
// An error will be returned if there are less than numOwners peers being used
// for hashing.
func (n *Node) Lookup(key uint64, numOwners int) ([]Peer, error) {
	owners, err := n.cfg.Hash.Get(key, numOwners)
	if err != nil {
		return nil, err
	}

	n.peerMut.RLock()
	defer n.peerMut.RUnlock()

	res := make([]Peer, len(owners))
	for i, o := range owners {
		res[i] = n.peers[o]
	}
	return res, nil
}

// nodeDelegate is used to implement memberlist.*Delegate types without
// exposing their methods publicly.
type nodeDelegate struct {
	*Node
}

// Delegate types nodeDelegate should implement
var (
	_ memberlist.Delegate         = (*nodeDelegate)(nil)
	_ memberlist.EventDelegate    = (*nodeDelegate)(nil)
	_ memberlist.ConflictDelegate = (*nodeDelegate)(nil)
)

//
// memberlist.Delegate methods
//

func (nd *nodeDelegate) NodeMeta(limit int) []byte {
	// Nodes don't have any additional metadata to send; return nil.

	// TODO(rfratto): We should consider returning metadata that describes the
	// current hashing algorithm so nodes can decide if they should reject
	// mismatching algorithms.
	return nil
}

func (nd *nodeDelegate) NotifyMsg(raw []byte) {
	buf, ty, err := messages.Parse(raw)
	if err != nil {
		level.Error(nd.log).Log("msg", "failed to parse gossip message", "ty", ty, "err", err)
		return
	}

	switch ty {
	case messages.TypeState:
		var s messages.State
		if err := messages.Decode(buf, &s); err != nil {
			level.Error(nd.log).Log("msg", "failed to decode state message", "err", err)
			return
		}

		nd.handleStateMessage(s)
	default:
		level.Warn(nd.log).Log("msg", "unexpected gossip message", "ty", ty)
	}
}

func (nd *nodeDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	return nd.broadcasts.GetBroadcasts(overhead, limit)
}

func (nd *nodeDelegate) LocalState(join bool) []byte {
	nd.peerMut.RLock()
	defer nd.peerMut.RUnlock()

	ls := localState{
		NodeStates: make([]messages.State, 0, len(nd.peers)),
	}

	// Our local state will have one NodeState for each peer we're currently
	// aware of. This is different from each state we're aware of, since our
	// nodeStates map may track states for nodes we haven't seen.
	//
	// Returning states for known peers allows MergeRemoteState to clean up
	// nodeStates and remove entries for nodes that neither peer knows about.
	for p := range nd.peers {
		if s, hasState := nd.peerStates[p]; hasState {
			ls.NodeStates = append(ls.NodeStates, s)
			continue
		}

		ls.NodeStates = append(ls.NodeStates, messages.State{
			NodeName: p,
			NewState: int(StatePending),
			Time:     lamport.Time(0),
		})
	}

	bb, err := encodeLocalState(&ls)
	if err != nil {
		level.Error(nd.log).Log("msg", "failed to encode local state", "err", err)
		return nil
	}
	return bb
}

func (nd *nodeDelegate) MergeRemoteState(buf []byte, join bool) {
	rs, err := decodeLocalState(buf)
	if err != nil {
		level.Error(nd.log).Log("msg", "failed to decode remote state", "join", join, "err", err)
		return
	}

	nd.peerMut.Lock()
	defer nd.peerMut.Unlock()

	// Build a map of remote states by name to optimize lookups.
	remoteStates := make(map[string]messages.State, len(rs.NodeStates))

	// Merge in node states that the remote peer kept.
	for _, msg := range rs.NodeStates {
		// We don't use handleStateMessage here so we can defer recalculating peers
		// to the end of the merge.
		remoteStates[msg.NodeName] = msg

		curr, exist := nd.peerStates[msg.NodeName]
		if exist && msg.Time <= curr.Time {
			// Ignore a state message if we have a newer one.
			continue
		}

		nd.peerStates[msg.NodeName] = msg

		if p, ok := nd.peers[msg.NodeName]; ok {
			p.State = State(msg.NewState)
			nd.peers[msg.NodeName] = p
		}
	}

	// Clean up stale entries in peerStates.
	for nodeName := range nd.peerStates {
		// If nodeName exists, it is not a stale reference.
		if _, peerExists := nd.peers[nodeName]; peerExists {
			continue
		}

		// We don't know about this peer. If the remote state also doesn't have an
		// entry for this peer, we can treat it as stale and delete it.
		_, peerExistsRemote := remoteStates[nodeName]
		if !peerExistsRemote {
			delete(nd.peerStates, nodeName)
		}
	}

	nd.handlePeersChanged()
}

type localState struct {
	// NodeStates holds the set of states for all peers of a node. States may
	// have a lamport time of 0 for nodes that have not broadcast a state yet.
	NodeStates []messages.State
}

func encodeLocalState(ls *localState) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	var handle codec.MsgpackHandle
	enc := codec.NewEncoder(buf, &handle)
	err := enc.Encode(ls)
	return buf.Bytes(), err
}

func decodeLocalState(buf []byte) (*localState, error) {
	var ls localState

	r := bytes.NewReader(buf)
	var handle codec.MsgpackHandle
	dec := codec.NewDecoder(r, &handle)
	err := dec.Decode(&ls)
	return &ls, err
}

//
// memberlist.EventDelegate methods
//

func (nd *nodeDelegate) NotifyJoin(node *memberlist.Node) {
	nd.NotifyUpdate(node)
}

func (nd *nodeDelegate) NotifyLeave(node *memberlist.Node) {
	nd.peerMut.Lock()
	defer nd.peerMut.Unlock()

	nd.removePeer(node.Name)
}

func (nd *nodeDelegate) NotifyUpdate(node *memberlist.Node) {
	nd.peerMut.Lock()
	defer nd.peerMut.Unlock()

	p := Peer{
		Name:  node.Name,
		Addr:  node.Address(),
		Self:  node.Name == nd.cfg.Name,
		State: State(nd.peerStates[node.Name].NewState),
	}
	nd.updatePeer(p)
}

func (nd *nodeDelegate) updatePeer(p Peer) {
	nd.peers[p.Name] = p
	nd.handlePeersChanged()
}

func (nd *nodeDelegate) removePeer(name string) {
	delete(nd.peers, name)
	nd.handlePeersChanged()
}

//
// memberlist.ConflictDelegate methods
//

func (nd *nodeDelegate) NotifyConflict(existing, other *memberlist.Node) {
	nd.conflictQueue.Enqueue(other)
}
