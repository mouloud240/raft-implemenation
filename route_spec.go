package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// RouteSpec renders a complete routing document from an immutable base
// by setting exactly one JSON-pointer addressable field (RFC 6901). It
// knows JSON and nothing else: the document's shape lives entirely in
// the committed base file (deploy/Caddyfile -> deploy/caddy.json), so
// changing the routing shape never touches Go code, and swapping proxy
// backends reuses this same patcher with a different pointer.
type RouteSpec struct {
	// Base is the full document exactly as the proxy backend accepts
	// it. It is never mutated by Render.
	Base []byte
	// DialPath is the JSON pointer of the field holding the upstream
	// address, e.g. "/apps/http/servers/srv0/routes/0/handle/0/
	// upstreams/0/dial".
	DialPath string
}

// Render returns a fresh full-state routing document with the upstream
// set to u.APIAddr. Decoding uses json.Number so every value survives
// the round trip unchanged (large integers keep their literal text);
// the rendered payload differs from the base in the dial leaf alone.
func (s RouteSpec) Render(u RouteUpdate) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(s.Base))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("base routing document: %w", err)
	}
	if err := setJSONPointer(&doc, s.DialPath, u.APIAddr); err != nil {
		return nil, fmt.Errorf("set %q: %w", s.DialPath, err)
	}
	out, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Validate renders against a throwaway update, so a wrong DialPath
// fails loudly at boot instead of silently no-op-ing during an election
// months later.
func (s RouteSpec) Validate() error {
	_, err := s.Render(RouteUpdate{LeaderId: 0, APIAddr: "127.0.0.1:1", Term: 0})
	return err
}

// setJSONPointer walks doc along a JSON pointer and replaces the leaf.
// It descends only into members/indices that already exist: a wrong
// path must error, not silently build structure the proxy would then
// reject at reload time (that's what boot-time Validate relies on).
// Composite values (maps, slices) are reference types, so the final
// write mutates the original document even after the walk copied
// headers along the way.
func setJSONPointer(doc *any, pointer string, value any) error {
	if pointer == "" {
		*doc = value // whole-document replace
		return nil
	}
	tokens := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
	cur := doc
	for i, raw := range tokens {
		// RFC 6901 escapes: ~1 is "/", ~0 is "~".
		tok := strings.ReplaceAll(strings.ReplaceAll(raw, "~1", "/"), "~0", "~")
		last := i == len(tokens)-1
		switch node := (*cur).(type) {
		case map[string]any:
			if _, ok := node[tok]; !ok {
				return fmt.Errorf("member %q not found", tok)
			}
			if last {
				node[tok] = value
				return nil
			}
			next := node[tok]
			cur = &next
		case []any:
			idx, err := strconv.Atoi(tok)
			if err != nil {
				return fmt.Errorf("array token %q is not an index", tok)
			}
			if idx < 0 || idx >= len(node) {
				return fmt.Errorf("array index %d out of range (len %d)", idx, len(node))
			}
			if last {
				node[idx] = value
				return nil
			}
			next := node[idx]
			cur = &next
		default:
			return fmt.Errorf("token %q descends into scalar %T", tok, *cur)
		}
	}
	return nil
}
