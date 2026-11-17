// RouteSpec unit tests: the generic document patcher must change exactly
// the pointer leaf, preserve every other value through the round trip
// (json.Number keeps large-integer literals intact), and fail loudly on
// wrong paths so boot-time Validate can catch them.
package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestRouteSpecRenderChangesOnlyDialLeaf(t *testing.T) {
	base := []byte(`{"a":{"b":[10,{"dial":"old"}]},"keep":{"n":149994930281,"list":[1,2]}}`)
	spec := RouteSpec{Base: base, DialPath: "/a/b/1/dial"}

	got, err := spec.Render(RouteUpdate{LeaderId: 2, APIAddr: "127.0.0.1:8082", Term: 5})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	var gotDoc, wantDoc any
	if err := json.Unmarshal(got, &gotDoc); err != nil {
		t.Fatalf("rendered doc is not JSON: %v", err)
	}
	want := []byte(`{"a":{"b":[10,{"dial":"127.0.0.1:8082"}]},"keep":{"n":149994930281,"list":[1,2]}}`)
	if err := json.Unmarshal(want, &wantDoc); err != nil {
		t.Fatalf("want doc is not JSON: %v", err)
	}
	if !reflect.DeepEqual(gotDoc, wantDoc) {
		t.Fatalf("rendered doc = %s, want %s (only the dial leaf should differ from the base)", got, want)
	}
}

// With UseNumber, integers that overflow float64 must keep their exact
// literal text through decode -> set -> marshal. A plain unmarshal to
// any would round them through float64 and corrupt the document.
func TestRouteSpecRenderPreservesLargeNumbers(t *testing.T) {
	huge := "123456789012345678901234567890123456789"
	base := []byte(`{"n":` + huge + `,"dial":"old"}`)
	spec := RouteSpec{Base: base, DialPath: "/dial"}

	got, err := spec.Render(RouteUpdate{APIAddr: "h:1"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !strings.Contains(string(got), `"n":`+huge) {
		t.Fatalf("large integer literal corrupted: %s", got)
	}
	if !strings.Contains(string(got), `"dial":"h:1"`) {
		t.Fatalf("dial leaf not set: %s", got)
	}
}

func TestRouteSpecBadPathsFail(t *testing.T) {
	base := []byte(`{"a":{"b":["x","y"]},"s":"scalar"}`)
	cases := []struct {
		name   string
		path   string
		errHas string
	}{
		{"missing member", "/nope/deeper", `member "nope" not found`},
		{"trailing member on scalar", "/s/x", "descends into scalar"},
		{"array index past end", "/a/b/9", "out of range"},
		{"array token not numeric", "/a/b/zero", "not an index"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := RouteSpec{Base: base, DialPath: tc.path}
			_, err := spec.Render(RouteUpdate{APIAddr: "h:1"})
			if err == nil {
				t.Fatalf("Render(%q) succeeded, want error containing %q", tc.path, tc.errHas)
			}
			if !strings.Contains(err.Error(), tc.errHas) {
				t.Fatalf("Render(%q) error = %v, want it to contain %q", tc.path, err, tc.errHas)
			}
		})
	}
}

// The committed Caddy artifact must stay patchable at the default
// pointer: this locks deploy/caddy.json and proxy.go's default dial
// path against silent drift (regenerating the artifact with
// `just gen-caddy` keeps this green; renaming the route shape does not).
func TestValidateCommittedCaddyBase(t *testing.T) {
	spec := RouteSpec{Base: caddyBaseConfig, DialPath: defaultCaddyDialPath}
	if err := spec.Validate(); err != nil {
		t.Fatalf("committed base does not patch at %s: %v", defaultCaddyDialPath, err)
	}

	got, err := spec.Render(RouteUpdate{LeaderId: 3, APIAddr: "127.0.0.1:8083", Term: 9})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(got, &doc); err != nil {
		t.Fatalf("rendered doc is not JSON: %v", err)
	}
	srv := doc["apps"].(map[string]any)["http"].(map[string]any)["servers"].(map[string]any)["srv0"].(map[string]any)
	handler := srv["routes"].([]any)[0].(map[string]any)["handle"].([]any)[0].(map[string]any)
	if handler["handler"] != "reverse_proxy" {
		t.Fatalf("patched route handler = %v, want reverse_proxy (artifact shape drifted)", handler["handler"])
	}
	if got := handler["upstreams"].([]any)[0].(map[string]any)["dial"]; got != "127.0.0.1:8083" {
		t.Fatalf("patched dial = %v, want 127.0.0.1:8083", got)
	}
}
