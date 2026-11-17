package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// RouteApplier installs a complete routing document. Contract: atomic,
// full-state, idempotent — every call replaces the backend's ENTIRE
// routing state with doc, never a partial mutation. Implementations
// decide only WHERE the document goes; what it says is RouteSpec's job
// and how often it's retried is the retryApplier decorator's job.
type RouteApplier interface {
	Apply(ctx context.Context, doc []byte) error
}

// caddyApplier installs a Caddy config document through the admin API's
// /load endpoint (full-state replace, idempotent by design). NOTE: the
// Caddy admin endpoint (default 127.0.0.1:2019) has NO authentication;
// it must never be exposed beyond the host running Caddy.
type caddyApplier struct {
	adminURL string // default http://127.0.0.1:2019/load
	client   *http.Client
}

func (c caddyApplier) Apply(ctx context.Context, doc []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.adminURL, bytes.NewReader(doc))
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		//Echo a bounded slice of the rejection reason: Caddy validates
		//config on load and its error text is the difference between a
		//5s debug and a 5h one.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("caddy admin returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return nil
}
