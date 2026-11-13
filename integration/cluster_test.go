package integration

import (
	"fmt"
	"github.com/saharshmaloo/raft_kv/raft"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

// A majority replaces an isolated leader, and healing restores agreement across all nodes.
func TestClusterIntegrationPartitionAndHeal(t *testing.T) {
	c := newTestCluster(t, 5, 11)
	all := c.activeIDs()
	leader, term := c.waitForLeader(all, 1)
	minority := []int{leader, (leader + 1) % 5}
	var majority []int
	for _, id := range all {
		if id != minority[0] && id != minority[1] {
			majority = append(majority, id)
		}
	}
	c.partition(minority, majority)
	newLeader, newTerm := c.waitForLeader(majority, term+1)
	c.assertStable(majority, newLeader, newTerm)
	// The isolated leader may retain its role; terms distinguish it from the
	// majority's leader. Healing must bring every node into one term and leader.
	c.heal()
	leader, term = c.waitForLeader(all, newTerm)
	c.assertStable(all, leader, term)
}

// Partitions smaller than a majority cannot elect a new leader; healing restores progress.
func TestClusterIntegrationNoQuorum(t *testing.T) {
	c := newTestCluster(t, 5, 23)
	all := c.activeIDs()
	leader, term := c.waitForLeader(all, 1)
	c.partition([]int{0, 1}, []int{2, 3}, []int{4})
	deadline := time.Now().Add(900 * time.Millisecond)
	for time.Now().Before(deadline) {
		for _, node := range c.snapshot() {
			if node.role == raft.Leader && (node.id != leader || node.term != term) {
				t.Fatalf("node elected without a quorum: %+v", node)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.heal()
	leader, term = c.waitForLeader(all, term)
	c.assertStable(all, leader, term)
}

// Seeded network faults and node shutdowns must leave surviving majorities able to recover.
func TestClusterE2ERandomFailures(t *testing.T) {
	seeds := []int64{7, 29, 101}
	// Override the seed to explore another schedule or repeat a reported run.
	// Concurrent RPC arrival order still depends on the Go scheduler.
	if value := os.Getenv("RAFT_TEST_SEED"); value != "" {
		seed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		seeds = []int64{seed}
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			c := newTestCluster(t, 5, seed)
			random := rand.New(rand.NewSource(seed))
			leader, term := c.waitForLeader(c.activeIDs(), 1)
			for round := 0; round < 4; round++ {
				active := c.activeIDs()
				minTerm := term
				switch round {
				case 0:
					// Independently lose and delay requests on directed links.
					c.network.mu.Lock()
					for _, from := range active {
						for _, to := range active {
							if from != to {
								c.network.faults[clusterLink{from, to}] = linkFault{
									dropRate:  0.1 + random.Float64()*0.4,
									replyLoss: random.Float64() * 0.3,
									maxDelay:  time.Duration(50+random.Intn(350)) * time.Millisecond,
								}
							}
						}
					}
					c.network.mu.Unlock()
					t.Log("injecting request loss, reply loss, and delay")
				case 1:
					// A one-way outage exercises asymmetric reachability.
					from := random.Intn(len(active))
					to := (from + 1 + random.Intn(len(active)-1)) % len(active)
					c.disconnect(active[from], active[to])
					t.Logf("disconnecting %d -> %d", active[from], active[to])
				case 2:
					var followers []int
					for _, id := range active {
						if id != leader {
							followers = append(followers, id)
						}
					}
					victim := followers[random.Intn(len(followers))]
					c.nodes[victim].Shutdown()
					t.Logf("stopping follower %d", victim)
					c.assertStable(c.activeIDs(), leader, term)
				case 3:
					c.nodes[leader].Shutdown()
					t.Logf("stopping leader %d", leader)
					minTerm++
					// Failover must work on the surviving connections, before
					// the harness performs any reconnection.
					leader, term = c.waitForLeader(c.activeIDs(), minTerm)
				}
				time.Sleep(time.Duration(400+random.Intn(300)) * time.Millisecond)
				c.heal()
				leader, term = c.waitForLeader(c.activeIDs(), minTerm)
				c.assertStable(c.activeIDs(), leader, term)
			}
		})
	}
}
