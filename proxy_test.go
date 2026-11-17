// Transport tests for the Caddy route push: the admin API receives
// exactly POST /load with the rendered full-state document, retried
// failures absorb transient 5xx without the caller noticing, and an
// exhausted retry budget returns the last error without crashing.
package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingAdmin is a stand-in Caddy admin endpoint: it records every
// request (path + body) and can fail the first N calls with a 500.
type recordingAdmin struct {
	mu       sync.Mutex
	requests []adminRequest
	failN    int
}

type adminRequest struct{ path, body string }

func (a *recordingAdmin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	a.mu.Lock()
	a.requests = append(a.requests, adminRequest{path: r.URL.Path, body: string(body)})
	fail := a.failN > 0
	if fail {
		a.failN--
	}
	a.mu.Unlock()
	if fail {
		http.Error(w, "boom", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (a *recordingAdmin) seen(t *testing.T) []adminRequest {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]adminRequest(nil), a.requests...)
}

// newCaddyProxyManagerForTest wires the real caddyProxyManager against
// a test admin URL with fast retries, keeping the committed base as the
// document source (same artifact production renders).
func newCaddyProxyManagerForTest(t *testing.T, adminURL string, attempts int, backoff time.Duration) ProxyManager {
	t.Helper()
	spec := RouteSpec{Base: caddyBaseConfig, DialPath: defaultCaddyDialPath}
	if err := spec.Validate(); err != nil {
		t.Fatalf("committed base failed validation: %v", err)
	}
	return caddyProxyManager{
		spec: spec,
		apply: retryApplier{
			inner:    caddyApplier{adminURL: adminURL, client: &http.Client{Timeout: 2 * time.Second}},
			backoff:  backoff,
			attempts: attempts,
		},
	}
}

func TestProxyRouteToLeaderPostsFullState(t *testing.T) {
	admin := &recordingAdmin{}
	srv := httptest.NewServer(admin)
	defer srv.Close()

	mgr := newCaddyProxyManagerForTest(t, srv.URL+"/load", 1, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := mgr.RouteToLeader(ctx, RouteUpdate{LeaderId: 2, APIAddr: "127.0.0.1:8082", Term: 3}); err != nil {
		t.Fatalf("RouteToLeader: %v", err)
	}

	reqs := admin.seen(t)
	if len(reqs) != 1 {
		t.Fatalf("admin saw %d requests, want exactly 1", len(reqs))
	}
	if reqs[0].path != "/load" {
		t.Fatalf("admin path = %q, want /load (full-state replace, not a partial patch)", reqs[0].path)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(reqs[0].body), &doc); err != nil {
		t.Fatalf("posted body is not JSON: %v", err)
	}
	dial := doc["apps"].(map[string]any)["http"].(map[string]any)["servers"].(map[string]any)["srv0"].(map[string]any)["routes"].([]any)[0].(map[string]any)["handle"].([]any)[0].(map[string]any)["upstreams"].([]any)[0].(map[string]any)["dial"]
	if dial != "127.0.0.1:8082" {
		t.Fatalf("posted dial = %v, want 127.0.0.1:8082", dial)
	}
	// The posted document must be a complete Caddy config, not a fragment.
	if v, ok := doc["apps"].(map[string]any)["http"].(map[string]any)["servers"].(map[string]any)["srv0"].(map[string]any)["listen"]; !ok {
		t.Fatalf("posted doc lost the listen block (partial render?): %v", v)
	}
}

func TestProxyRetriesTransientFailures(t *testing.T) {
	admin := &recordingAdmin{failN: 2}
	srv := httptest.NewServer(admin)
	defer srv.Close()

	mgr := newCaddyProxyManagerForTest(t, srv.URL+"/load", 3, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := mgr.RouteToLeader(ctx, RouteUpdate{LeaderId: 1, APIAddr: "127.0.0.1:8081", Term: 2}); err != nil {
		t.Fatalf("RouteToLeader with 5xx,5xx,200 should succeed: %v", err)
	}
	if reqs := admin.seen(t); len(reqs) != 3 {
		t.Fatalf("admin saw %d requests, want 3 (two failures then success)", len(reqs))
	}
}

func TestProxyRetriesExhausted(t *testing.T) {
	admin := &recordingAdmin{failN: 100}
	srv := httptest.NewServer(admin)
	defer srv.Close()

	mgr := newCaddyProxyManagerForTest(t, srv.URL+"/load", 2, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := mgr.RouteToLeader(ctx, RouteUpdate{LeaderId: 1, APIAddr: "127.0.0.1:8081", Term: 2})
	if err == nil {
		t.Fatal("RouteToLeader succeeded against a permanently failing admin, want error")
	}
	if reqs := admin.seen(t); len(reqs) != 2 {
		t.Fatalf("admin saw %d requests, want 2 (attempts budget exhausted)", len(reqs))
	}
}

// The factory must refuse to push with a broken dial path: it degrades
// to the noop backend (which the promotion path tolerates) instead of
// crashing or pushing garbage at every election.
func TestProxyFactoryDegradesToNoopOnBadSpec(t *testing.T) {
	t.Setenv("RAFT_PROXY", "caddy")
	t.Setenv("RAFT_PROXY_DIAL_PATH", "/not/a/real/pointer")
	mgr := NewProxyManager()
	if _, ok := mgr.(NoopProxyManager); !ok {
		t.Fatalf("NewProxyManager with an invalid spec returned %T, want NoopProxyManager", mgr)
	}
}

func TestProxyFactoryDefaultsToNoop(t *testing.T) {
	t.Setenv("RAFT_PROXY", "")
	mgr := NewProxyManager()
	if _, ok := mgr.(NoopProxyManager); !ok {
		t.Fatalf("NewProxyManager default returned %T, want NoopProxyManager", mgr)
	}
	if err := mgr.RouteToLeader(context.Background(), RouteUpdate{LeaderId: 1, APIAddr: "h:1", Term: 1}); err != nil {
		t.Fatalf("noop RouteToLeader: %v", err)
	}
}

// Render must not leak mutations into the embedded base: two pushes in
// a row must each start from the pristine artifact.
func TestProxyRenderDoesNotMutateBase(t *testing.T) {
	spec := RouteSpec{Base: caddyBaseConfig, DialPath: defaultCaddyDialPath}
	first, err := spec.Render(RouteUpdate{LeaderId: 1, APIAddr: "127.0.0.1:8081", Term: 1})
	if err != nil {
		t.Fatalf("first Render: %v", err)
	}
	second, err := spec.Render(RouteUpdate{LeaderId: 2, APIAddr: "127.0.0.1:8082", Term: 2})
	if err != nil {
		t.Fatalf("second Render: %v", err)
	}
	if !strings.Contains(string(first), "127.0.0.1:8081") || strings.Contains(string(first), "127.0.0.1:8082") {
		t.Fatalf("first render contaminated by the second: %s", first)
	}
	if !strings.Contains(string(second), "127.0.0.1:8082") {
		t.Fatalf("second render did not apply its own update: %s", second)
	}
}
