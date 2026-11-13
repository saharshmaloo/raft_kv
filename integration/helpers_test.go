package integration

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"

	"github.com/saharshmaloo/raft_kv"
	"github.com/saharshmaloo/raft_kv/raft"
)

// Each directed link has its own RPC proxy, allowing asymmetric failures while
// leaving the servers' TCP transport and consensus clocks running normally.
type clusterLink struct{ from, to int }

type linkFault struct {
	blocked   bool
	dropRate  float64
	replyLoss float64
	maxDelay  time.Duration
}

type faultNetwork struct {
	mu        sync.Mutex
	random    *rand.Rand
	faults    map[clusterLink]linkFault
	listeners map[clusterLink]net.Listener
	conns     map[net.Conn]bool
	done      chan struct{}
	wg        sync.WaitGroup
}

type faultProxy struct {
	network *faultNetwork
	link    clusterLink
	address string
}

// Apply link faults around a real upstream RPC, including loss after the receiver processes it.
func (p *faultProxy) forward(method string, args, reply any) error {
	n := p.network
	n.mu.Lock()
	fault := n.faults[p.link]
	dropped := fault.blocked || n.random.Float64() < fault.dropRate
	lostReply := n.random.Float64() < fault.replyLoss
	var delay time.Duration
	if fault.maxDelay > 0 {
		delay = time.Duration(n.random.Int63n(int64(fault.maxDelay)))
	}
	n.mu.Unlock()
	if dropped {
		return fmt.Errorf("injected unreachable link %d -> %d", p.link.from, p.link.to)
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-n.done:
			return fmt.Errorf("fault network stopped")
		}
	}
	// A separate upstream connection per request lets a delayed request arrive
	// after a later request, including after a partition has healed.
	conn, err := net.DialTimeout("tcp", p.address, time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	if err := client.Call(method, args, reply); err != nil {
		return err
	}
	// A lost response leaves the receiver's state change intact, modeling an
	// ambiguous RPC failure rather than only requests that never arrived.
	if lostReply {
		return fmt.Errorf("injected lost reply %d -> %d", p.link.to, p.link.from)
	}
	return nil
}

// Route vote requests through the same fault injection path as heartbeats.
func (p *faultProxy) RequestVote(args raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	return p.forward("ConsensusModule.RequestVote", args, reply)
}

// Route leader contact through the proxy so delays and loss affect election timing.
func (p *faultProxy) AppendEntries(args raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	return p.forward("ConsensusModule.AppendEntries", args, reply)
}

// Give one directed peer link a TCP proxy whose connections are tracked for cleanup.
func (n *faultNetwork) addLink(t *testing.T, link clusterLink, address string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	n.listeners[link] = listener
	server := rpc.NewServer()
	if err := server.RegisterName("ConsensusModule", &faultProxy{n, link, address}); err != nil {
		t.Fatal(err)
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			n.mu.Lock()
			select {
			case <-n.done:
				n.mu.Unlock()
				conn.Close()
				return
			default:
			}
			n.conns[conn] = true
			n.wg.Add(1)
			n.mu.Unlock()
			go func() {
				defer n.wg.Done()
				server.ServeConn(conn)
				conn.Close()
				n.mu.Lock()
				delete(n.conns, conn)
				n.mu.Unlock()
			}()
		}
	}()
}

// Cancel injected delays and close proxy connections before waiting for handlers to finish.
func (n *faultNetwork) close() {
	n.mu.Lock()
	close(n.done)
	for _, listener := range n.listeners {
		listener.Close()
	}
	for conn := range n.conns {
		conn.Close()
	}
	n.mu.Unlock()
	n.wg.Wait()
}

type nodeObservation struct {
	id      int
	role    raft.CMState
	term    int
	stopped bool
}

type testCluster struct {
	t       *testing.T
	nodes   []*raftkv.Node
	network *faultNetwork
	done    chan struct{}
	watcher sync.WaitGroup
}

// Start a TCP cluster behind fault proxies and monitor sampled election safety until cleanup.
func newTestCluster(t *testing.T, size int, seed int64) *testCluster {
	t.Helper()
	t.Logf("fault seed=%d", seed)
	c := &testCluster{
		t: t, done: make(chan struct{}),
		network: &faultNetwork{
			random: rand.New(rand.NewSource(seed)), faults: make(map[clusterLink]linkFault),
			listeners: make(map[clusterLink]net.Listener), conns: make(map[net.Conn]bool),
			done: make(chan struct{}),
		},
	}
	t.Cleanup(func() {
		close(c.done)
		c.watcher.Wait()
		c.network.close()
		for _, node := range c.nodes {
			node.Shutdown()
		}
	})
	addresses := make([]string, size)
	for id := 0; id < size; id++ {
		var peers []int
		for peer := 0; peer < size; peer++ {
			if peer != id {
				peers = append(peers, peer)
			}
		}
		node := raftkv.NewNode(id, peers)
		c.nodes = append(c.nodes, node)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addresses[id] = listener.Addr().String()
		if err := node.Serve(listener); err != nil {
			listener.Close()
			t.Fatal(err)
		}
	}
	for from := range c.nodes {
		for to, address := range addresses {
			if from != to {
				c.network.addLink(t, clusterLink{from, to}, address)
			}
		}
	}
	c.heal()
	// Remember leadership across observations, so different nodes becoming
	// leader in the same term is a failure even if they are observed separately.
	c.watcher.Add(1)
	go func() {
		defer c.watcher.Done()
		leaders := make(map[int]int)
		terms := make(map[int]int)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-c.done:
				return
			case <-ticker.C:
				for _, node := range c.snapshot() {
					if node.term < terms[node.id] {
						t.Errorf("node %d term decreased from %d to %d", node.id, terms[node.id], node.term)
						return
					}
					terms[node.id] = node.term
					if node.stopped || node.role != raft.Leader {
						continue
					}
					if previous, ok := leaders[node.term]; ok && previous != node.id {
						t.Errorf("two leaders in term %d: nodes %d and %d", node.term, previous, node.id)
						return
					}
					leaders[node.term] = node.id
				}
			}
		}
	}()
	return c
}

// Read each node through its public status API; observations are not atomic across nodes.
func (c *testCluster) snapshot() []nodeObservation {
	result := make([]nodeObservation, len(c.nodes))
	for id, node := range c.nodes {
		status := node.Status()
		result[id] = nodeObservation{status.ID, status.State, status.Term, status.Stopped}
	}
	return result
}

// Exclude stopped nodes from recovery checks and reconnection attempts.
func (c *testCluster) activeIDs() []int {
	var ids []int
	for _, node := range c.snapshot() {
		if !node.stopped {
			ids = append(ids, node.id)
		}
	}
	return ids
}

// Clear faults and reconnect surviving nodes without changing configured membership.
func (c *testCluster) heal() {
	c.t.Helper()
	c.network.mu.Lock()
	c.network.faults = make(map[clusterLink]linkFault)
	c.network.mu.Unlock()
	active := c.activeIDs()
	for _, from := range active {
		for _, to := range active {
			if from != to {
				address := c.network.listeners[clusterLink{from, to}].Addr().String()
				if err := c.nodes[from].ConnectToPeer(to, address); err != nil {
					c.t.Fatal(err)
				}
			}
		}
	}
}

// Block communication across groups while preserving links inside each group.
func (c *testCluster) partition(groups ...[]int) {
	group := make(map[int]int)
	for index, ids := range groups {
		for _, id := range ids {
			group[id] = index
		}
	}
	c.network.mu.Lock()
	defer c.network.mu.Unlock()
	for link := range c.network.listeners {
		c.network.faults[link] = linkFault{blocked: group[link.from] != group[link.to]}
	}
}

// Close one outgoing peer connection to simulate an asymmetric outage.
func (c *testCluster) disconnect(from, to int) {
	c.nodes[from].DisconnectPeer(to)
}

// Require sustained agreement on one leader and term before declaring a component recovered.
func (c *testCluster) waitForLeader(ids []int, minTerm int) (int, int) {
	c.t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	lastLeader, lastTerm := -1, -1
	var stableSince time.Time
	for time.Now().Before(deadline) {
		states := c.snapshot()
		found := false
		for _, id := range ids {
			leader := states[id]
			if leader.stopped || leader.role != raft.Leader || leader.term < minTerm {
				continue
			}
			converged := true
			for _, peer := range ids {
				if peer != id && (states[peer].stopped || states[peer].role != raft.Follower || states[peer].term != leader.term) {
					converged = false
				}
			}
			if converged {
				found = true
				if id != lastLeader || leader.term != lastTerm {
					lastLeader, lastTerm = id, leader.term
					stableSince = time.Now()
				}
				// Allow delayed pre-heal requests to arrive before declaring
				// recovery; a momentary matching snapshot is insufficient.
				if time.Since(stableSince) >= 400*time.Millisecond {
					return id, leader.term
				}
				break
			}
		}
		if !found {
			lastLeader, lastTerm = -1, -1
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("nodes %v did not converge on a leader at term >= %d: %+v", ids, minTerm, c.snapshot())
	return -1, 0
}

// Keep observing roles and terms long enough to expose missing periodic heartbeats.
func (c *testCluster) assertStable(ids []int, leader, term int) {
	c.t.Helper()
	// Longer than two maximum election timeouts: stability requires heartbeats.
	deadline := time.Now().Add(650 * time.Millisecond)
	for time.Now().Before(deadline) {
		states := c.snapshot()
		for _, id := range ids {
			wantRole := raft.Follower
			if id == leader {
				wantRole = raft.Leader
			}
			if states[id].stopped || states[id].role != wantRole || states[id].term != term {
				c.t.Fatalf("leadership changed in healthy component %v: %+v", ids, states)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}
