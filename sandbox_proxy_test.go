// Sandbox integration for the routing proxy: with every node's
// ProxyManager swapped for a recording double, the promotion path must
// publish exactly one route update per won election — from the winner
// itself, pointing at the winner's client-API address — and nothing
// else: no heartbeat re-pushes, no pushes on step-down, no pushes from
// followers. Direct REST hits answer 409 + leaderId on non-leaders
// (the fast-fail bridge for the proxy's stale window) and are accepted
// on the leader. Run: go test -run 'TestSandboxProxyRoute' -v .
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingProxy is a ProxyManager double that remembers every update
// the promotion path emits. It records under a mutex (not a channel) so
// tests can diff counts across phases without draining races.
type recordingProxy struct {
	mu      sync.Mutex
	updates []RouteUpdate
}

func newRecordingProxy() *recordingProxy { return &recordingProxy{} }

func (p *recordingProxy) RouteToLeader(_ context.Context, u RouteUpdate) error {
	p.mu.Lock()
	p.updates = append(p.updates, u)
	p.mu.Unlock()
	return nil
}

func (p *recordingProxy) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.updates)
}

func (p *recordingProxy) last(t *testing.T) RouteUpdate {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.updates) == 0 {
		t.Fatalf("expected at least one recorded route update, got none")
	}
	return p.updates[len(p.updates)-1]
}

// updatesFrom returns a locked copy of everything recorded from index
// `from` on, for failure messages.
func (p *recordingProxy) updatesFrom(from int) []RouteUpdate {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]RouteUpdate(nil), p.updates[from:]...)
}

// sandboxWaitForProxyUpdate polls until proxy emits a new update (past
// `before`), then requires it to point at wantAddr with a term strictly
// greater than minTerm (use -1 for the first push of the cluster).
func sandboxWaitForProxyUpdate(t *testing.T, tag string, p *recordingProxy, before int, wantAddr string, minTerm int, budget time.Duration, sigs <-chan os.Signal) RouteUpdate {
	t.Helper()
	deadline := time.Now().Add(budget)
	for {
		if p.count() > before {
			upd := p.last(t)
			if wantAddr != "" && upd.APIAddr != wantAddr {
				t.Fatalf("%s: route update points at %q, want %q", tag, upd.APIAddr, wantAddr)
			}
			if minTerm >= 0 && upd.Term <= minTerm {
				t.Fatalf("%s: route update term %d did not advance past %d", tag, upd.Term, minTerm)
			}
			return upd
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: no route update within %v (want addr %q)", tag, budget, wantAddr)
		}
		if !sandboxWait(50*time.Millisecond, sigs) {
			t.Skipf("%s: interrupted while waiting for route update", tag)
		}
	}
}

// proxySandboxPostSet drives one REST /set and returns the status code
// plus the decoded 409 body (zero value on other statuses).
func proxySandboxPostSet(t *testing.T, url string) (int, notLeaderResponse) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(`{"key":"proxy-sandbox","value":1,"operation":0}`))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	var body notLeaderResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

func TestSandboxProxyRouteFollowsLeader(t *testing.T) {
	sigs, stopSigs := sandboxWatchSignals()
	defer stopSigs()

	proxies := []*recordingProxy{newRecordingProxy(), newRecordingProxy(), newRecordingProxy()}
	nodes, byID := sandboxStartCluster(t, sandboxCmdNumNodes, sandboxCmdBasePort, sigs, func(i int) ProxyManager {
		return proxies[i]
	})

	// Phase A: first stable leader. The 5s hold is part of the routing
	// assertion that follows — any heartbeat-tick re-push would have
	// fired dozens of times inside it.
	ldr, leaderTerm, ok := sandboxWaitStable(t, "proxy-elect", nodes, nil, sandboxCmdElectionFor, sandboxStableNeed, sigs)
	if !ok {
		sandboxDump(t, nodes, nil)
		t.Fatalf("no stable leader within %v", sandboxCmdElectionFor)
	}
	leaderNode := byID[ldr]
	leaderIdx := sandboxIndexOf(nodes, ldr)
	firstPush := sandboxWaitForProxyUpdate(t, "first push", proxies[leaderIdx], 0, leaderNode.apiAddr(), -1, 5*time.Second, sigs)
	t.Logf("[sandbox] node %d pushed route to %s at term %d", ldr, firstPush.APIAddr, firstPush.Term)

	// Phase B: transition-only quiet window. Snapshot every proxy, wait
	// 1.5s (30 heartbeat ticks, several election timeouts), demand zero
	// new updates anywhere — proves route pushes are not periodic and
	// step-down does not publish.
	before := make([]int, len(proxies))
	for i, p := range proxies {
		before[i] = p.count()
	}
	if !sandboxWait(1500*time.Millisecond, sigs) {
		t.Skip("interrupted during quiet window")
	}
	for i, p := range proxies {
		if n := p.count(); n != before[i] {
			t.Fatalf("node %d pushed %d extra route update(s) with no election running (transition-only violated): %v",
				nodes[i].id, n-before[i], p.updatesFrom(before[i]))
		}
	}

	// Phase C: direct REST hits. A follower must answer 409 fast with
	// the leader hint; the leader must accept (202) — the leader-gate
	// true/false paths, exercised against the real muxes.
	var follower *RaftNode
	for _, n := range nodes {
		if n.id != ldr {
			follower = n
			break
		}
	}
	status, body := proxySandboxPostSet(t, "http://"+follower.apiAddr()+"/set")
	if status != http.StatusConflict {
		t.Fatalf("POST /set on follower %d: status %d, want 409", follower.id, status)
	}
	if body.LeaderId != ldr {
		t.Fatalf("409 body leaderId = %d, want %d (winner hint)", body.LeaderId, ldr)
	}
	t.Logf("[sandbox] follower %d answered 409 leaderId=%d", follower.id, body.LeaderId)

	status, _ = proxySandboxPostSet(t, "http://"+leaderNode.apiAddr()+"/set")
	if status != http.StatusAccepted {
		t.Fatalf("POST /set on leader %d: status %d, want 202", ldr, status)
	}

	// Phase D: kill the leader exactly like the election sandbox (gRPC
	// server + heartbeat loops, REST left running), then require a new
	// stable leader among the survivors and a second, term-advancing
	// route update from the NEW winner — the proxy must follow the
	// failover without any restart or manual nudge.
	dead := map[int]bool{ldr: true}
	t.Logf("[sandbox] killing leader node %d (term %d)", ldr, leaderTerm)
	leaderNode.server.Stop()
	for i := 0; i < 3; i++ {
		select {
		case leaderNode.stopHeartbeatCh <- struct{}{}:
		default:
		}
		if !sandboxWait(60*time.Millisecond, sigs) {
			t.Skip("interrupted during kill")
		}
	}

	newLdr, newTerm, ok := sandboxWaitStable(t, "proxy-failover", nodes, dead, 30*time.Second, sandboxStableNeed, sigs)
	sandboxDump(t, nodes, dead)
	if !ok {
		t.Fatalf("no stable leader within failover budget")
	}
	if newLdr == ldr {
		t.Fatalf("failover produced the same leader %d (it was supposed to be dead)", newLdr)
	}
	newLeaderIdx := sandboxIndexOf(nodes, newLdr)
	secondPush := sandboxWaitForProxyUpdate(t, "failover push", proxies[newLeaderIdx], before[newLeaderIdx], byID[newLdr].apiAddr(), firstPush.Term, 5*time.Second, sigs)
	t.Logf("[sandbox] SUMMARY: first route %s@%d, failover route %s@%d (leader %d -> %d, final term %d)",
		firstPush.APIAddr, firstPush.Term, secondPush.APIAddr, secondPush.Term, ldr, newLdr, newTerm)

	// Phase E: the failover was transition-only too — every proxy other
	// than the new winner must have stayed silent since the kill.
	for i, p := range proxies {
		if i == newLeaderIdx {
			continue
		}
		if n := p.count(); n != before[i] {
			t.Fatalf("non-winner node %d pushed %d route update(s) during failover: %v",
				nodes[i].id, n-before[i], p.updatesFrom(before[i]))
		}
	}
}
