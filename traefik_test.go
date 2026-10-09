package main

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"
)

func TestTraefikConfig(t *testing.T) {
	u := func(s string) *url.URL { x, _ := url.Parse(s); return x }
	active := &Version{ID: "new222", Name: "web-new222", Upstream: u("http://10.43.0.12:3000")}
	old := &Version{ID: "old111", Name: "web-old111", Upstream: u("http://10.43.0.11:3000")}
	r := &Routes{Serving: active, Active: active, byID: map[string]*Version{"new222": active, "old111": old}}
	o := TraefikOptions{Rule: "Host(`www.example.com`)", EntryPoints: []string{"websecure"}, CertResolver: "le", Middlewares: []string{"sec@file"}}

	c := TraefikConfig(r, o)
	def := c.HTTP.Routers["skew-default"]
	if def.Rule != "Host(`www.example.com`)" || def.Priority != priorityDefault || def.TLS["certResolver"] != "le" || def.Middlewares[0] != "sec@file" {
		t.Fatalf("default router: %+v", def)
	}
	if got := c.HTTP.Services["skew-default"].LoadBalancer.Servers[0].URL; got != "http://10.43.0.12:3000" {
		t.Fatalf("default → %s, want the active version", got)
	}
	pin := c.HTTP.Routers["skew-old111"]
	want := "(Host(`www.example.com`)) && (Header(`" + DeploymentIDHeader + "`, `old111`) || Query(`dpl`, `old111`))"
	if pin.Rule != want || pin.Priority <= def.Priority {
		t.Fatalf("pinned router: %+v", pin)
	}
	if got := c.HTTP.Services["skew-old111"].LoadBalancer.Servers[0].URL; got != "http://10.43.0.11:3000" {
		t.Fatalf("old111 → %s", got)
	}
	if len(c.HTTP.Routers) != 3 { // default + one per routable version
		t.Fatalf("routers: %v", c.HTTP.Routers)
	}

	// A version that is no longer routable (retired, revoked) gets no router: its id falls
	// through to the default one, like the proxy's fallback.
	r2 := &Routes{Serving: active, Active: active, byID: map[string]*Version{"new222": active}}
	if _, ok := TraefikConfig(r2, o).HTTP.Routers["skew-old111"]; ok {
		t.Fatal("a non-routable version still has a router")
	}

	// Nothing ready: the default service has no servers, so Traefik answers 503.
	empty := TraefikConfig(&Routes{byID: map[string]*Version{}}, o)
	if n := len(empty.HTTP.Services["skew-default"].LoadBalancer.Servers); n != 0 {
		t.Fatalf("no version ready but %d servers", n)
	}
	b, _ := json.Marshal(empty)
	if !strings.Contains(string(b), `"servers":[]`) {
		t.Fatalf("servers must be an empty list, not null: %s", b)
	}
}

func TestParseFlagsTraefikMode(t *testing.T) {
	if _, err := parseFlags([]string{"-mode=traefik"}); err == nil {
		t.Fatal("traefik mode without a rule must be refused")
	}
	c, err := parseFlags([]string{"-mode=traefik", "-traefik-rule=Host(`a.b`)", "-traefik-middlewares=x@file,y@file"})
	if err != nil || len(c.traefik.Middlewares) != 2 || c.traefik.EntryPoints[0] != "websecure" {
		t.Fatalf("%+v %v", c.traefik, err)
	}
	if _, err := parseFlags([]string{"-mode=nope"}); err == nil {
		t.Fatal("unknown mode accepted")
	}
}
