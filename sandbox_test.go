// Sandbox harness: in-process Raft clusters that exercise leader election,
// client commands (set/increment/delete) end to end, leader kill + failover,
// and restart/rejoin. Run: just sandbox (both), just sandbox-commands,
// just sandbox-election, or just sandbox-race (with -race).
package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	// 5-node election/failover sandbox.
	sandboxNumNodes = 5
	sandboxBasePort = 50051
	// Fast 3-node command sandbox (distinct ports so both tests can coexist).
	sandboxCmdNumNodes = 3
	sandboxCmdBasePort = 51051
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
	// Command sandbox: election budget.
	sandboxCmdElectionFor = 20 * time.Second
	// Per-command ceiling waiting for HandleAppendEntry to return (its
	// internal replication timeout is 5s, so this is the outer guard).
	sandboxCmdWait = 10 * time.Second
	// How long followers get to converge on the leader's state machine
	// after a committed command (heartbeats carry leaderCommit every
	// HEARTBEAT_INTERVAL, so this is generous).
	sandboxConvergeFor = 7 * time.Second
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

func sandboxOpName(op Operation) string {
	switch op {
	case SET:
		return "SET"
	case INCREMENT:
		return "INCREMENT"
	case DELETE:
		return "DELETE"
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

// sandboxFullSnapshot extends the role snapshot with the log/state-machine
// progress the command sandbox reasons about. stateMachine is a copy taken
// under the node's mutex.
type sandboxFullSnapshot struct {
	sandboxSnapshot
	commitIndex  int
	lastApplied  int
	logLen       int
	stateMachine map[string]int
}

func sandboxFullPoll(n *RaftNode) sandboxFullSnapshot {
	n.mu.Lock()
	defer n.mu.Unlock()
	sm := make(map[string]int, len(n.stateMachine))
	for k, v := range n.stateMachine {
		sm[k] = v
	}
	return sandboxFullSnapshot{
		sandboxSnapshot: sandboxSnapshot{state: n.state, term: n.term, leaderID: n.leaderId, votedFor: n.votedFor},
		commitIndex:     n.commitIndex,
		lastApplied:     n.lastApplied,
		logLen:          len(n.log),
		stateMachine:    sm,
	}
}

func sandboxFullSnapshotAll(nodes []*RaftNode) []sandboxFullSnapshot {
	snaps := make([]sandboxFullSnapshot, len(nodes))
	for i, n := range nodes {
		snaps[i] = sandboxFullPoll(n)
	}
	return snaps
}

func sandboxFormatStateMachine(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return "{" + strings.Join(parts, " ") + "}"
}

func sandboxMapsEqual(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
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
		s := sandboxFullPoll(n)
		mark := ""
		if dead[n.id] {
			mark = " [dead/excluded]"
		}
		t.Logf("[sandbox] final: node %d %s term=%d leader=%d votedFor=%d commit=%d applied=%d logLen=%d sm=%s%s",
			n.id, sandboxStateName(s.state), s.term, s.leaderID, s.votedFor,
			s.commitIndex, s.lastApplied, s.logLen, sandboxFormatStateMachine(s.stateMachine), mark)
	}
}

func sandboxIndexOf(nodes []*RaftNode, id int) int {
	for i, n := range nodes {
		if n.id == id {
			return i
		}
	}
	return -1
}

// sandboxStartCluster builds and starts an in-process cluster on distinct
// ports and waits for the gRPC servers to come up. Nodes keep running when
// the test returns (process exit releases the ports).
func sandboxStartCluster(t *testing.T, numNodes, basePort int, sigs <-chan os.Signal) ([]*RaftNode, map[int]*RaftNode) {
	t.Helper()
	// Isolate this run's persistent-state files: NewRaftNode loads any
	// existing state file for a node's id at boot, so a stale file left
	// by a previous run would break fresh elections.
	t.Setenv("RAFT_DATA_DIR", t.TempDir())
	ports := make([]int, numNodes)
	for i := range ports {
		ports[i] = basePort + i
	}
	sandboxRequirePortsFree(t, ports...)

	nodes := make([]*RaftNode, 0, numNodes)
	byID := map[int]*RaftNode{}
	for i := 0; i < numNodes; i++ {
		var peers []RaftPeer
		for j := 0; j < numNodes; j++ {
			if j == i {
				continue
			}
			peers = append(peers, RaftPeer{id: j + 1, host: "127.0.0.1", port: ports[j]})
		}
		n := NewRaftNode(i+1, "127.0.0.1", ports[i], peers)
		nodes = append(nodes, n)
		byID[n.id] = n
	}
	for _, n := range nodes {
		n := n
		go func() {
			if err := n.Run(); err != nil {
				t.Errorf("node %d: Run() returned: %v", n.id, err)
			}
		}()
	}
	if !sandboxWait(200*time.Millisecond, sigs) {
		t.Logf("[sandbox] interrupted during startup, exiting (process exit releases the ports)")
		t.Skip("interrupted by signal during startup")
	}
	for _, n := range nodes {
		s := sandboxPoll(n)
		t.Logf("[sandbox] node %d starts as %s term=%d", n.id, sandboxStateName(s.state), s.term)
	}
	return nodes, byID
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

// sandboxBatch is a scripted client workload plus the state machine the
// cluster must end up with once every command is committed and applied.
type sandboxBatch struct {
	name     string
	commands []ExecuteCommandClientRequest
	want     map[string]int
}

// sandboxBatch1 exercises every operation once:
// a = 1, then INCREMENT +4 → 5; b set then deleted; c = 7.
func sandboxBatch1() sandboxBatch {
	return sandboxBatch{
		name: "batch1",
		commands: []ExecuteCommandClientRequest{
			sandboxCmd("a", 1, SET),
			sandboxCmd("a", 4, INCREMENT),
			sandboxCmd("b", 10, SET),
			sandboxCmd("b", 1, DELETE),
			sandboxCmd("c", 7, SET),
		},
		want: map[string]int{"a": 5, "c": 7},
	}
}

// sandboxBatch2 runs on the NEW leader after the failover and assumes
// batch 1 committed everywhere: a = 5 → 6, plus e = 42 and f = 99.
func sandboxBatch2() sandboxBatch {
	return sandboxBatch{
		name: "batch2",
		commands: []ExecuteCommandClientRequest{
			sandboxCmd("a", 1, INCREMENT),
			sandboxCmd("e", 42, SET),
			sandboxCmd("f", 99, SET),
		},
		want: map[string]int{"a": 6, "c": 7, "e": 42, "f": 99},
	}
}

func sandboxCmd(key string, value int, op Operation) ExecuteCommandClientRequest {
	return ExecuteCommandClientRequest{Key: key, Value: value, Operation: op}
}

// sandboxSendCommand drives one client command through the real path:
// leader.HandleAppendEntry → log append → replication → quorum commit →
// apply. It blocks until the command returns (committed or timed out).
func sandboxSendCommand(t *testing.T, tag string, leader *RaftNode, req ExecuteCommandClientRequest, sigs <-chan os.Signal) {
	t.Helper()
	if s := sandboxPoll(leader); s.state != LEADER {
		t.Fatalf("%s: node %d is %s, not LEADER; refusing to send %s %s=%d",
			tag, leader.id, sandboxStateName(s.state), sandboxOpName(req.Operation), req.Key, req.Value)
	}
	done := make(chan struct{})
	start := time.Now()
	go func() {
		leader.HandleAppendEntry(req)
		close(done)
	}()
	select {
	case <-done:
		t.Logf("[sandbox] %s: %s %s=%d handled by leader %d in %v",
			tag, sandboxOpName(req.Operation), req.Key, req.Value, leader.id, time.Since(start).Round(time.Millisecond))
	case <-time.After(sandboxCmdWait):
		t.Fatalf("%s: command %s %s=%d did not return within %v (quorum never reached?)",
			tag, sandboxOpName(req.Operation), req.Key, req.Value, sandboxCmdWait)
	case <-sigs:
		t.Logf("[sandbox] %s: interrupted during command, exiting", tag)
		t.Skip("interrupted by signal during command")
	}
}

func sandboxRunBatch(t *testing.T, tag string, leader *RaftNode, b sandboxBatch, sigs <-chan os.Signal) {
	t.Helper()
	for _, cmd := range b.commands {
		sandboxSendCommand(t, tag, leader, cmd, sigs)
	}
}

// sandboxWaitStateMachines polls until every live node's state machine
// equals want, or the budget runs out. It returns the per-node match
// report; a timeout is NOT fatal here so callers can downgrade specific
// nodes (e.g. a restarted node that cannot catch up without log repair).
func sandboxWaitStateMachines(t *testing.T, tag string, nodes []*RaftNode, dead map[int]bool, want map[string]int, budget time.Duration, sigs <-chan os.Signal) map[int]bool {
	t.Helper()
	matches := make(map[int]bool, len(nodes))
	everMatched := make(map[int]bool)
	deadline := time.Now().Add(budget)
	for {
		snaps := sandboxFullSnapshotAll(nodes)
		allLiveMatch := true
		for i, n := range nodes {
			if dead[n.id] {
				continue
			}
			m := sandboxMapsEqual(snaps[i].stateMachine, want)
			if m && !everMatched[n.id] {
				everMatched[n.id] = true
				t.Logf("[sandbox] %s: node %d converged: sm=%s commit=%d applied=%d logLen=%d",
					tag, n.id, sandboxFormatStateMachine(snaps[i].stateMachine), snaps[i].commitIndex, snaps[i].lastApplied, snaps[i].logLen)
			}
			matches[n.id] = m
			if !m {
				allLiveMatch = false
			}
		}
		if allLiveMatch {
			return matches
		}
		if time.Now().After(deadline) {
			break
		}
		if !sandboxWait(sandboxPollEvery, sigs) {
			t.Logf("[sandbox] %s: interrupted, exiting (process exit releases the ports)", tag)
			t.Skip("interrupted by signal during convergence wait")
		}
	}
	snaps := sandboxFullSnapshotAll(nodes)
	for i, n := range nodes {
		if dead[n.id] || matches[n.id] {
			continue
		}
		t.Logf("[sandbox] %s: node %d did NOT converge: sm=%s want=%s commit=%d applied=%d logLen=%d",
			tag, n.id, sandboxFormatStateMachine(snaps[i].stateMachine), sandboxFormatStateMachine(want),
			snaps[i].commitIndex, snaps[i].lastApplied, snaps[i].logLen)
	}
	return matches
}

// TestPersistentStateRoundTrip validates the persistence plumbing in
// isolation: the storage DTO survives marshal→unmarshal, and save() →
// LoadPersistentState() round-trips a real file (term, votedFor, log),
// with repeated saves overwriting (not appending) snapshots.
func TestPersistentStateRoundTrip(t *testing.T) {
	ps := persistentState{
		term:     7,
		votedFor: 3,
		log: []LogEntry{
			{cmd: Command{key: "a", value: 1, operation: SET}, index: 0, term: 5},
			{cmd: Command{key: "b", value: 42, operation: INCREMENT}, index: 1, term: 7},
		},
	}

	// Marshal → unmarshal must preserve everything through the DTO.
	data, err := ps.ToStorageFormat()
	if err != nil {
		t.Fatalf("ToStorageFormat: %v", err)
	}
	back, err := PersistenStateFromStorageFormat(data)
	if err != nil {
		t.Fatalf("PersistenStateFromStorageFormat: %v", err)
	}
	if back.term != ps.term || back.votedFor != ps.votedFor {
		t.Fatalf("round trip: term/votedFor = %d/%d, want %d/%d", back.term, back.votedFor, ps.term, ps.votedFor)
	}
	if len(back.log) != len(ps.log) {
		t.Fatalf("round trip: log length %d, want %d", len(back.log), len(ps.log))
	}
	for i, want := range ps.log {
		got := back.log[i]
		if got.index != want.index || got.term != want.term || got.cmd != want.cmd {
			t.Fatalf("round trip: log[%d] = %+v, want %+v", i, got, want)
		}
	}

	// save → load must round-trip through a real file, and a second save
	// must overwrite the file (never append another snapshot).
	path := t.TempDir() + "/node_1_state.json"
	if err := ps.save(path); err != nil {
		t.Fatalf("save: %v", err)
	}
	ps.term = 9
	ps.log = append(ps.log, LogEntry{cmd: Command{key: "c", value: 3, operation: DELETE}, index: 2, term: 9})
	if err := ps.save(path); err != nil {
		t.Fatalf("second save: %v", err)
	}
	loaded, err := LoadPersistentState(path)
	if err != nil {
		t.Fatalf("LoadPersistentState: %v", err)
	}
	if loaded.term != 9 || loaded.votedFor != 3 || len(loaded.log) != 3 {
		t.Fatalf("load after second save: term=%d votedFor=%d logLen=%d, want 9/3/3",
			loaded.term, loaded.votedFor, len(loaded.log))
	}
	if loaded.log[0].cmd.key != "a" || loaded.log[0].cmd.value != 1 || loaded.log[0].cmd.operation != SET {
		t.Fatalf("load: log[0].cmd = %+v, want a=1 SET", loaded.log[0].cmd)
	}
	t.Logf("round trip OK: %s", string(data))
}

// TestSandboxCommands is the fast request-simulation sandbox: a 3-node
// cluster must elect a leader, replicate a client command batch through
// HandleAppendEntry, commit it, and converge every node's state machine
// via heartbeats carrying leaderCommit.
func TestSandboxCommands(t *testing.T) {
	sigs, stopSigs := sandboxWatchSignals()
	defer stopSigs()

	nodes, _ := sandboxStartCluster(t, sandboxCmdNumNodes, sandboxCmdBasePort, sigs)

	leader, _, ok := sandboxWaitStable(t, "cmd-elect", nodes, nil, sandboxCmdElectionFor, sandboxStableNeed, sigs)
	if !ok {
		sandboxDump(t, nodes, nil)
		t.Fatalf("commands: no stable leader within %v", sandboxCmdElectionFor)
	}
	leaderNode := nodes[sandboxIndexOf(nodes, leader)]

	batch := sandboxBatch1()
	sandboxRunBatch(t, "cmd-apply", leaderNode, batch, sigs)

	got := sandboxFullPoll(leaderNode).stateMachine
	if !sandboxMapsEqual(got, batch.want) {
		t.Fatalf("commands: leader %d state machine after batch: %s, want %s",
			leader, sandboxFormatStateMachine(got), sandboxFormatStateMachine(batch.want))
	}

	matches := sandboxWaitStateMachines(t, "cmd-converge", nodes, nil, batch.want, sandboxConvergeFor, sigs)
	sandboxDump(t, nodes, nil)
	for _, n := range nodes {
		if !matches[n.id] {
			t.Fatalf("commands: node %d failed to converge to %s", n.id, sandboxFormatStateMachine(batch.want))
		}
	}
	t.Logf("[sandbox] SUMMARY: %d commands committed and applied on all %d nodes (sm=%s)",
		len(batch.commands), len(nodes), sandboxFormatStateMachine(batch.want))
}

func TestSandboxElection(t *testing.T) {
	sigs, stopSigs := sandboxWatchSignals()
	defer stopSigs()

	nodes, byID := sandboxStartCluster(t, sandboxNumNodes, sandboxBasePort, sigs)

	// Phase 1: first stable leader among all 5.
	leader, leaderTerm, ok := sandboxWaitStable(t, "phase1", nodes, nil, sandboxPhase1For, sandboxStableNeed, sigs)
	if !ok {
		sandboxDump(t, nodes, nil)
		t.Fatalf("phase 1: no stable leader within %v", sandboxPhase1For)
	}
	leaderNode := byID[leader]

	// Phase 1.5: client commands on the leader. Every node is alive, so
	// the whole cluster must converge on the same state machine.
	batch1 := sandboxBatch1()
	t.Logf("[sandbox] phase1.5: sending %d commands to leader %d", len(batch1.commands), leader)
	sandboxRunBatch(t, "phase1.5", leaderNode, batch1, sigs)
	got := sandboxFullPoll(leaderNode).stateMachine
	if !sandboxMapsEqual(got, batch1.want) {
		t.Fatalf("phase 1.5: leader %d state machine after batch: %s, want %s",
			leader, sandboxFormatStateMachine(got), sandboxFormatStateMachine(batch1.want))
	}
	matches := sandboxWaitStateMachines(t, "phase1.5-converge", nodes, nil, batch1.want, sandboxConvergeFor, sigs)
	for _, n := range nodes {
		if !matches[n.id] {
			t.Fatalf("phase 1.5: node %d failed to converge to %s", n.id, sandboxFormatStateMachine(batch1.want))
		}
	}

	// Phase 2: kill the leader's server and watch the fallout. No
	// stability required here — terms will climb while the survivors
	// re-elect. The dead node is excluded from leader counting since
	// its in-memory state stays frozen at LEADER.
	if leaderNode.server == nil {
		t.Fatalf("phase 2: leader node %d has no server handle to stop", leader)
	}
	dead := map[int]bool{leader: true}
	termBefore := sandboxMaxTerm(nodes)
	t.Logf("[sandbox] phase2: stopping leader node %d (term %d)", leader, leaderTerm)
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

	// Phase 4: commands on the NEW leader. This checks that committed
	// batch-1 state survived the failover and that the new leader can
	// replicate fresh commands.
	newLeaderNode := byID[newLeader]
	batch2 := sandboxBatch2()
	t.Logf("[sandbox] phase4: sending %d commands to new leader %d", len(batch2.commands), newLeader)
	sandboxRunBatch(t, "phase4", newLeaderNode, batch2, sigs)
	gotNew := sandboxFullPoll(newLeaderNode).stateMachine
	if !sandboxMapsEqual(gotNew, batch2.want) {
		t.Fatalf("phase 4: new leader %d state machine after batch: %s, want %s",
			newLeader, sandboxFormatStateMachine(gotNew), sandboxFormatStateMachine(batch2.want))
	}
	// Hard requirement: every node that was never killed must converge.
	// The restarted node (== leader) is soft-checked in phase 5: heartbeat
	// AppendEntries never backfill a stale log (no log repair yet), so it
	// only catches up through batch-2 command replication itself.
	matches = sandboxWaitStateMachines(t, "phase4-converge", nodes, nil, batch2.want, sandboxConvergeFor, sigs)
	for _, n := range nodes {
		if n.id == leader || matches[n.id] {
			continue
		}
		t.Fatalf("phase 4: node %d failed to converge to %s", n.id, sandboxFormatStateMachine(batch2.want))
	}

	// Phase 5: the restarted node's state. Divergence (missing batch-2
	// entries) is a known limitation; corruption of committed batch-1
	// values is a hard failure.
	restartedSnap := sandboxFullPoll(leaderNode)
	if matches[leader] {
		t.Logf("[sandbox] phase5: restarted node %d fully caught up: %s",
			leader, sandboxFormatStateMachine(restartedSnap.stateMachine))
	} else {
		for k, wantV := range batch1.want {
			gotV, present := restartedSnap.stateMachine[k]
			if !present || gotV != wantV {
				t.Fatalf("phase 5: restarted node %d CORRUPTED committed state: key %s = %d (present=%v), want %d (full sm=%s)",
					leader, k, gotV, present, wantV, sandboxFormatStateMachine(restartedSnap.stateMachine))
			}
		}
		t.Logf("[sandbox] phase5: KNOWN LIMITATION: restarted node %d did not catch up (no log backfill); batch-1 committed state intact: %s",
			leader, sandboxFormatStateMachine(restartedSnap.stateMachine))
	}

	sandboxDump(t, nodes, nil)
	t.Logf("[sandbox] SUMMARY: killed=%d@%d newLeader=%d@%d maxTerm=%d final sm=%s",
		leader, leaderTerm, newLeader, newTerm, sandboxMaxTerm(nodes), sandboxFormatStateMachine(batch2.want))
}
