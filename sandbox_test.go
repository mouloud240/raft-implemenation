// Sandbox harness: 5-node in-process Raft cluster exercising leader
// election, leader kill, and restart/rejoin. Run: just sandbox
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
	sandboxNumNodes = 5
	sandboxBasePort = 50051
	// How often the harness snapshots node state.
	sandboxPollEvery = 50 * time.Millisecond
	// Phase 1 budget: first stable leader.
	sandboxPhase1For = 30 * time.Second
	// A single leader held this long counts as "stable".
	sandboxStableNeed = 5 * time.Second
	// Phase 2: how long to watch the cluster after killing the leader.
	// No stability required here, just observation.
	sandboxKillObserve = 10 * time.Second
	// Phase 3 budget: stable leader after restart.
	sandboxPhase3For = 25 * time.Second
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

func sandboxSnapshotAll(nodes []*RaftNode) []sandboxSnapshot {
	snaps := make([]sandboxSnapshot, len(nodes))
	for i, n := range nodes {
		snaps[i] = sandboxPoll(n)
	}
	return snaps
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

// sandboxLiveLeaders returns the ids of non-excluded nodes currently in
// LEADER state. More than one means split brain.
func sandboxLiveLeaders(nodes []*RaftNode, snaps []sandboxSnapshot, dead map[int]bool) []int {
	var ids []int
	for i, n := range nodes {
		if dead[n.id] {
			continue
		}
		if snaps[i].state == LEADER {
			ids = append(ids, n.id)
		}
	}
	return ids
}

func sandboxMaxTerm(nodes []*RaftNode) int {
	max := 0
	for _, n := range nodes {
		if s := sandboxPoll(n); s.term > max {
			max = s.term
		}
	}
	return max
}

func sandboxDump(t *testing.T, nodes []*RaftNode, dead map[int]bool) {
	t.Helper()
	for _, n := range nodes {
		s := sandboxPoll(n)
		mark := ""
		if dead[n.id] {
			mark = " [dead/excluded]"
		}
		t.Logf("[sandbox] final: node %d %s term=%d leader=%d votedFor=%d%s",
			n.id, sandboxStateName(s.state), s.term, s.leaderID, s.votedFor, mark)
	}
}

// sandboxWaitStable polls until the same single live leader (excluding
// dead) holds for `need`, or the budget runs out. Calls t.Skip on
// interrupt, so callers only handle the timeout case.
func sandboxWaitStable(t *testing.T, tag string, nodes []*RaftNode, dead map[int]bool, budget, need time.Duration, sigs <-chan os.Signal) (int, int, bool) {
	t.Helper()
	prev := sandboxSnapshotAll(nodes)
	deadline := time.Now().Add(budget)
	lastSingle, lastTerm := 0, -1
	since := time.Now()
	for time.Now().Before(deadline) {
		if !sandboxWait(sandboxPollEvery, sigs) {
			t.Logf("[sandbox] %s: interrupted, shutting down (process exit releases the ports)", tag)
			t.Skip("interrupted by signal")
		}
		cur := sandboxSnapshotAll(nodes)
		for i, n := range nodes {
			if cur[i] != prev[i] {
				mark := ""
				if dead[n.id] {
					mark = " [dead/excluded]"
				}
				t.Logf("[sandbox] %s: node %d: %s term=%d leader=%d votedFor=%d -> %s term=%d leader=%d votedFor=%d%s",
					tag, n.id,
					sandboxStateName(prev[i].state), prev[i].term, prev[i].leaderID, prev[i].votedFor,
					sandboxStateName(cur[i].state), cur[i].term, cur[i].leaderID, cur[i].votedFor, mark)
				prev[i] = cur[i]
			}
		}
		leaders := sandboxLiveLeaders(nodes, cur, dead)
		switch len(leaders) {
		case 0:
			since, lastSingle = time.Now(), 0
		case 1:
			term := cur[sandboxIndexOf(nodes, leaders[0])].term
			if leaders[0] != lastSingle || term != lastTerm {
				lastSingle, lastTerm, since = leaders[0], term, time.Now()
			} else if time.Since(since) >= need {
				t.Logf("[sandbox] %s: STABLE LEADER: node %d for term %d (held %.0fs)",
					tag, lastSingle, lastTerm, time.Since(since).Seconds())
				return lastSingle, lastTerm, true
			}
		default:
			t.Logf("[sandbox] %s: SPLIT BRAIN: %d simultaneous live leaders %v", tag, len(leaders), leaders)
			since, lastSingle = time.Now(), 0
		}
	}
	return 0, 0, false
}

func sandboxIndexOf(nodes []*RaftNode, id int) int {
	for i, n := range nodes {
		if n.id == id {
			return i
		}
	}
	return -1
}

func TestSandboxElection(t *testing.T) {
	ports := make([]int, sandboxNumNodes)
	for i := range ports {
		ports[i] = sandboxBasePort + i
	}
	sandboxRequirePortsFree(t, ports...)

	nodes := make([]*RaftNode, 0, sandboxNumNodes)
	byID := map[int]*RaftNode{}
	for i := 0; i < sandboxNumNodes; i++ {
		var peers []RaftPeer
		for j := 0; j < sandboxNumNodes; j++ {
			if j == i {
				continue
			}
			peers = append(peers, RaftPeer{id: j + 1, host: "127.0.0.1", port: ports[j]})
		}
		n := NewRaftNode(i+1, "127.0.0.1", ports[i], peers)
		nodes = append(nodes, n)
		byID[n.id] = n
	}
	// The constructor does not initialize stopHeartbeatCh yet (nil in
	// production too — calling StopHeartBeat there would hang forever on
	// send). Initialize a buffered one per node so the kill phase can
	// signal heartbeat loops without ever blocking.
	// TODO(main.go): move `stopHeartbeatCh: make(chan struct{}, 1)` into NewRaftNode.
	for _, n := range nodes {
		n.stopHeartbeatCh = make(chan struct{}, 1)
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

	// Let the gRPC servers come up before the first election timeout fires.
	if !sandboxWait(200*time.Millisecond, sigs) {
		t.Logf("[sandbox] interrupted during startup, exiting (process exit releases the ports)")
		t.Skip("interrupted by signal during startup")
	}
	for _, n := range nodes {
		s := sandboxPoll(n)
		t.Logf("[sandbox] node %d starts as %s term=%d", n.id, sandboxStateName(s.state), s.term)
	}

	// Phase 1: first stable leader among all 5.
	leader, leaderTerm, ok := sandboxWaitStable(t, "phase1", nodes, nil, sandboxPhase1For, sandboxStableNeed, sigs)
	if !ok {
		sandboxDump(t, nodes, nil)
		t.Fatalf("phase 1: no stable leader within %v", sandboxPhase1For)
	}

	// Phase 2: kill the leader's server and watch the fallout. No
	// stability required here — terms will climb while the survivors
	// re-elect. The dead node is excluded from leader counting since
	// its in-memory state stays frozen at LEADER.
	leaderNode := byID[leader]
	if leaderNode.server == nil {
		t.Fatalf("phase 2: leader node %d has no server handle to stop", leader)
	}
	t.Logf("[sandbox] phase2: stopping leader node %d (term %d)", leader, leaderTerm)
	dead := map[int]bool{leader: true}
	termBefore := sandboxMaxTerm(nodes)
	leaderNode.server.Stop()
	// server.Stop() only closes the inbound side; the leader's heartbeat
	// senders are plain client calls that keep running (followers would
	// keep following a "dead" leader). Stop those loops too.
	// Deliberately not calling StopHeartBeat: it blocks, and a node can
	// hold more than one loop across terms. Spaced non-blocking sends
	// drain them all (a live loop consumes a pending signal within one
	// 50ms tick); the 60ms spacing guarantees consumption between sends.
	for i := 0; i < 3; i++ {
		select {
		case leaderNode.stopHeartbeatCh <- struct{}{}:
		default:
		}
		if !sandboxWait(60*time.Millisecond, sigs) {
			t.Logf("[sandbox] phase2: interrupted, exiting (process exit releases the ports)")
			t.Skip("interrupted by signal during kill")
		}
	}
	t.Logf("[sandbox] phase2: node %d stopped (server + heartbeats); observing %v (no stability required)", leader, sandboxKillObserve)
	if !sandboxWait(sandboxKillObserve, sigs) {
		t.Logf("[sandbox] phase2: interrupted, exiting (process exit releases the ports)")
		t.Skip("interrupted by signal during kill observation")
	}
	t.Logf("[sandbox] phase2: maxTerm %d -> %d during outage", termBefore, sandboxMaxTerm(nodes))

	// Phase 3: restart the killed node on the same port with its
	// surviving state. The cluster must converge on exactly one stable
	// leader at a term past the killed one (terms never decrease, and
	// any new winner increments past its own).
	t.Logf("[sandbox] phase3: restarting node %d", leader)
	delete(dead, leader)
	go func() {
		if err := leaderNode.Run(); err != nil {
			t.Errorf("restarted node %d: Run() returned: %v", leader, err)
		}
	}()
	if !sandboxWait(300*time.Millisecond, sigs) {
		t.Logf("[sandbox] phase3: interrupted, exiting (process exit releases the ports)")
		t.Skip("interrupted by signal during restart")
	}
	newLeader, newTerm, ok := sandboxWaitStable(t, "phase3", nodes, nil, sandboxPhase3For, sandboxStableNeed, sigs)
	sandboxDump(t, nodes, nil)
	if !ok {
		t.Fatalf("phase 3: no stable leader within %v after restart", sandboxPhase3For)
	}
	if newTerm <= leaderTerm {
		t.Fatalf("phase 3: new leader term %d did not advance past killed term %d", newTerm, leaderTerm)
	}
	t.Logf("[sandbox] SUMMARY: killed=%d@%d newLeader=%d@%d maxTerm=%d",
		leader, leaderTerm, newLeader, newTerm, sandboxMaxTerm(nodes))
}
