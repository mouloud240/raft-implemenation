package main

import (
	"context"
	_ "embed"
	"log/slog"
	"net/http"
	"time"
)

// RouteUpdate is everything a routing backend needs to point client
// traffic at the current leader.
type RouteUpdate struct {
	LeaderId int
	// APIAddr is the client-API host:port the proxy must forward to —
	// never the gRPC port: the proxy fronts the client surface only,
	// Raft's internal RPCs stay peer-to-peer.
	APIAddr string
	// Term rides along for observability now, and as a ready ordering
	// guard if out-of-order pushes ever become real (laggy disks on a
	// future multi-host setup), without changing the interface.
	Term int
}

// ProxyManager installs the leader route. Contract: full-state +
// idempotent + best-effort — RouteToLeader replaces the backend's whole
// routing state each call; failures are retried inside the
// implementation and are never fatal (the REST 409 NotLeader answer
// keeps direct clients working while the route is stale). Also: exactly
// one writer exists (the promotion transition in promotSelfToLeader),
// so an honest but deposed leader can never fight the new leader over
// the route — routing truth has a single source.
type ProxyManager interface {
	RouteToLeader(ctx context.Context, u RouteUpdate) error
}

// NoopProxyManager exercises the promotion path with no side effects:
// the default backend for tests and pre-deploy runs
// (RAFT_PROXY=caddy opts into the real push).
type NoopProxyManager struct{}

func (NoopProxyManager) RouteToLeader(_ context.Context, u RouteUpdate) error {
	slog.Info("route update (noop proxy)", "leader_id", u.LeaderId, "addr", u.APIAddr, "term", u.Term)
	return nil
}

// defaultCaddyDialPath locates the reverse_proxy upstream inside the
// adapted deploy/Caddyfile. If the Caddyfile shape changes (extra
// routes, matchers, ...), override with RAFT_PROXY_DIAL_PATH — the Go
// code never needs to learn the new shape.
const defaultCaddyDialPath = "/apps/http/servers/srv0/routes/0/handle/0/upstreams/0/dial"

// caddyBaseConfig is the caddy-adapt compiled artifact of
// deploy/Caddyfile (the human-readable source of truth; regenerate with
// `just gen-caddy`).
//
//go:embed deploy/caddy.json
var caddyBaseConfig []byte

// NewProxyManager picks the routing backend from the environment.
// RAFT_PROXY=caddy pushes leader routes to the local Caddy admin API;
// anything else (including unset) is the noop backend.
func NewProxyManager() ProxyManager {
	if envOr("RAFT_PROXY", "noop") != "caddy" {
		return NoopProxyManager{}
	}
	spec := RouteSpec{
		Base:     caddyBaseConfig,
		DialPath: envOr("RAFT_PROXY_DIAL_PATH", defaultCaddyDialPath),
	}
	if err := spec.Validate(); err != nil {
		slog.Error("routing spec invalid; proxy updates disabled (direct clients keep the 409 fallback)", "error", err)
		return NoopProxyManager{}
	}
	return caddyProxyManager{
		spec: spec,
		apply: retryApplier{
			inner: caddyApplier{
				adminURL: envOr("RAFT_PROXY_ADMIN_URL", "http://127.0.0.1:2019/load"),
				client:   &http.Client{Timeout: 2 * time.Second},
			},
			attempts: 3,
			backoff:  200 * time.Millisecond,
		},
	}
}

// caddyProxyManager is the only type aware of both halves: the generic
// document spec and the Caddy transport. Swapping to another backend
// (envoy xDS file subscription, traefik file provider) means writing
// another type with this same two-call shape and adding a case to
// NewProxyManager — nothing else moves.
type caddyProxyManager struct {
	apply RouteApplier
	spec  RouteSpec
}

func (m caddyProxyManager) RouteToLeader(ctx context.Context, u RouteUpdate) error {
	doc, err := m.spec.Render(u)
	if err != nil {
		return err
	}
	return m.apply.Apply(ctx, doc)
}
