// Sandbox harness: spins up a 3-node in-process Raft cluster and observes
// the election logic with logs.
//
// It does NOT assert full Raft correctness. It reports what happened
// (leader claims, split-brain sightings, stability, term growth) and
// fails only on a node crash or a total lack of election progress.
// Run it with: just sandbox
package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

const (
	sandboxNumNodes = 3
	sandboxBasePort = 50051
	// How often the harness snapshots node state.
	sandboxPollEvery = 50 * time.Millisecond
	// Total observation window.
	sandboxRunFor = 30 * time.Second
	// A single leader held this long counts as "stable".
	sandboxStableNeed = 5 * time.Second
)

func sandboxStateName(s NodeState) string {
	switch s {
	case FOLLOWER:
		return "FOLLOWER"
	case CANDIDATE:
		return "CANDIDATE"
	case LEADER:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

type sandboxSnapshot struct {
	state    NodeState
	term     int
	leaderID int
	votedFor int
}

func sandboxPoll(n *RaftNode) sandboxSnapshot {
	n.mu.Lock()
	defer n.mu.Unlock()
	return sandboxSnapshot{state: n.state, term: n.term, leaderID: n.leaderId, votedFor: n.votedFor}
}

// Fail fast with a clear message when a port is taken, instead of
// tripping the nil-listener panic in Run().
func sandboxRequirePortsFree(t *testing.T, ports ...int) {
	t.Helper()
	for _, p := range ports {
		lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
		if err != nil {
			t.Fatalf("sandbox port %d is already in use: %v (stop the other process first)", p, err)
		}
		_ = lis.Close()
	}
}

// sandboxWatchSignals arranges delivery of SIGINT/SIGTERM (e.g. Ctrl+C)
// to the returned channel so a run can shut down cleanly instead of
// dying mid-observation. Call the returned stop func when done.
func sandboxWatchSignals() (chan os.Signal, func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	return ch, func() { signal.Stop(ch) }
}

// sandboxWait sleeps for d unless a signal arrives first.
// It reports false when interrupted.
func sandboxWait(d time.Duration, sigs <-chan os.Signal) bool {
	select {
	case <-sigs:
		return false
	case <-time.After(d):
		return true
	}
}

func TestSandboxElection(t *testing.T) {
	ports := []int{sandboxBasePort, sandboxBasePort + 1, sandboxBasePort + 2}
	sandboxRequirePortsFree(t, ports...)

	var nodes []*RaftNode
	for i := 0; i < sandboxNumNodes; i++ {
		var peers []RaftPeer
		for j := 0; j < sandboxNumNodes; j++ {
			if j == i {
				continue
			}
			peers = append(peers, RaftPeer{id: j + 1, host: "127.0.0.1", port: ports[j]})
		}
		nodes = append(nodes, NewRaftNode(i+1, "127.0.0.1", ports[i], peers))
	}

	for _, n := range nodes {
		n := n
		go func() {
			if err := n.Run(); err != nil {
				t.Errorf("node %d: Run() returned: %v", n.id, err)
			}
		}()
	}
	sigs, stopSigs := sandboxWatchSignals()
	defer stopSigs()
	start := time.Now()

	// Let the gRPC servers come up before the first election timeout fires.
	if !sandboxWait(200*time.Millisecond, sigs) {
		t.Logf("[sandbox] interrupted during startup, exiting (process exit releases the ports)")
		t.Skip("interrupted by signal during startup")
	}

	prev := make([]sandboxSnapshot, len(nodes))
	for i, n := range nodes {
		prev[i] = sandboxPoll(n)
		t.Logf("[sandbox] node %d starts as %s term=%d", n.id, sandboxStateName(prev[i].state), prev[i].term)
	}

	deadline := time.Now().Add(sandboxRunFor)
	maxTerm := 0
	leaderClaims := map[int]int{} // node id -> times observed entering LEADER
	lastSingleLeader := 0
	lastSingleLeaderTerm := -1
	singleLeaderSince := time.Now()
	stableReported := false

	interrupted := false
	for time.Now().Before(deadline) {
		if !sandboxWait(sandboxPollEvery, sigs) {
			t.Logf("[sandbox] interrupt signal received, stopping observation...")
			interrupted = true
			break
		}
		cur := make([]sandboxSnapshot, len(nodes))
		for i, n := range nodes {
			cur[i] = sandboxPoll(n)
			if cur[i] != prev[i] {
				t.Logf("[sandbox] node %d: %s term=%d leader=%d votedFor=%d -> %s term=%d leader=%d votedFor=%d",
					n.id,
					sandboxStateName(prev[i].state), prev[i].term, prev[i].leaderID, prev[i].votedFor,
					sandboxStateName(cur[i].state), cur[i].term, cur[i].leaderID, cur[i].votedFor,
				)
				if cur[i].state == LEADER && prev[i].state != LEADER {
					leaderClaims[n.id]++
				}
				prev[i] = cur[i]
			}
			if cur[i].term > maxTerm {
				maxTerm = cur[i].term
			}
		}

		var leaders []int
		leaderTerm := -1
		for i, n := range nodes {
			if cur[i].state == LEADER {
				leaders = append(leaders, n.id)
				leaderTerm = cur[i].term
			}
		}
		switch len(leaders) {
		case 0:
			singleLeaderSince = time.Now()
			lastSingleLeader = 0
		case 1:
			if leaders[0] != lastSingleLeader || leaderTerm != lastSingleLeaderTerm {
				lastSingleLeader, lastSingleLeaderTerm = leaders[0], leaderTerm
				singleLeaderSince = time.Now()
			} else if !stableReported && time.Since(singleLeaderSince) >= sandboxStableNeed {
				t.Logf("[sandbox] STABLE LEADER: node %d for term %d (held %.0fs)",
					lastSingleLeader, lastSingleLeaderTerm, time.Since(singleLeaderSince).Seconds())
				stableReported = true
			}
		default:
			t.Logf("[sandbox] SPLIT BRAIN: %d simultaneous leaders %v", len(leaders), leaders)
			singleLeaderSince = time.Now()
			lastSingleLeader = 0
		}
	}

	t.Logf("[sandbox] SUMMARY: maxTerm=%d leaderClaims=%v stableLeaderReported=%v", maxTerm, leaderClaims, stableReported)
	for _, n := range nodes {
		s := sandboxPoll(n)
		t.Logf("[sandbox] final: node %d %s term=%d leader=%d votedFor=%d",
			n.id, sandboxStateName(s.state), s.term, s.leaderID, s.votedFor)
	}

	if interrupted {
		t.Logf("[sandbox] exiting after interrupt (process exit releases the ports)")
		t.Skipf("interrupted by signal after %v", time.Since(start).Round(time.Second))
	}
	if maxTerm == 0 {
		t.Fatalf("no election progress: max term stayed 0 (nodes may be deadlocked)")
	}
	if len(leaderClaims) == 0 {
		t.Fatalf("no node ever became leader within %v (terms advanced to %d but voting never converged)", sandboxRunFor, maxTerm)
	}
}
