package main

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testRoutes() RoutesConfig {
	return RoutesConfig{Prefix: "skew", Routes: []RouteTemplate{
		{Name: "trusted", Rule: "Host(`www.example.com`) && ClientIP(`10.0.0.0/8`)", Priority: 1000, Middlewares: []string{"strip@file"}},
		{Name: "static", Rule: "Host(`www.example.com`) && PathPrefix(`/_next/static/`)", Priority: 500, Middlewares: []string{"strip@file", "edge-static@file"}},
		{Name: "web", Rule: "Host(`www.example.com`)", Priority: 100, EntryPoints: []string{"websecure"}, Middlewares: []string{"strip@file", "edge-web@file"}, TLS: map[string]any{}},
	}}
}

func TestTraefikConfig(t *testing.T) {
	u := func(s string) *url.URL { x, _ := url.Parse(s); return x }
	active := &Version{ID: "new222", Name: "web-new222", Upstream: u("http://10.43.0.12:3000")}
	old := &Version{ID: "old111", Name: "web-old111", Upstream: u("http://10.43.0.11:3000")}
	r := &Routes{Serving: active, Active: active, byID: map[string]*Version{"new222": active, "old111": old}}
	rc := testRoutes()

	c := TraefikConfig(r, rc)
	// 3 templates × (default + 1 pinned: the serving version has none) = 6 routers;
	// services: active + one per other version.
	if len(c.HTTP.Routers) != 6 || len(c.HTTP.Services) != 2 {
		t.Fatalf("routers %d services %d", len(c.HTTP.Routers), len(c.HTTP.Services))
	}
	def := c.HTTP.Routers["skew-static"]
	if def.Service != "skew-active" || def.Priority != 500 || strings.Join(def.Middlewares, ",") != "strip@file,edge-static@file" {
		t.Fatalf("default static router: %+v", def)
	}
	pin := c.HTTP.Routers["skew-static-old111"]
	want := "(Host(`www.example.com`) && PathPrefix(`/_next/static/`)) && (Header(`" + DeploymentIDHeader + "`, `old111`) || Query(`dpl`, `old111`))"
	if pin.Rule != want || pin.Priority != 501 || pin.Service != "skew-v-old111" || strings.Join(pin.Middlewares, ",") != "strip@file,edge-static@file" {
		t.Fatalf("pinned static router keeps the template's middlewares: %+v", pin)
	}
	// Every pinned router stays below the next template up, so priorities interleave correctly.
	if c.HTTP.Routers["skew-static-old111"].Priority >= c.HTTP.Routers["skew-trusted"].Priority {
		t.Fatal("a pinned static router overtook the trusted route")
	}
	// The serving version's requests stay on the default router (one rate-limit bucket).
	if _, ok := c.HTTP.Routers["skew-web-new222"]; ok {
		t.Fatal("the serving version must not get a pinned router")
	}
	if w := c.HTTP.Routers["skew-web-old111"]; w.TLS == nil || w.EntryPoints[0] != "websecure" {
		t.Fatalf("pinned router lost TLS/entrypoints: %+v", w)
	}
	if got := c.HTTP.Services["skew-v-old111"].LoadBalancer.Servers[0].URL; got != "http://10.43.0.11:3000" {
		t.Fatalf("old111 → %s", got)
	}
	if got := c.HTTP.Services["skew-active"].LoadBalancer.Servers[0].URL; got != "http://10.43.0.12:3000" {
		t.Fatalf("active → %s", got)
	}

	// Not routable any more (retired, revoked): no pinned routers, its id falls through.
	r2 := &Routes{Serving: active, Active: active, byID: map[string]*Version{"new222": active}}
	if _, ok := TraefikConfig(r2, rc).HTTP.Routers["skew-web-old111"]; ok {
		t.Fatal("a non-routable version still has a router")
	}
	// Nothing ready: no servers, Traefik answers 503.
	empty := TraefikConfig(&Routes{byID: map[string]*Version{}}, rc)
	b, _ := json.Marshal(empty.HTTP.Services["skew-active"])
	if !strings.Contains(string(b), `"servers":[]`) {
		t.Fatalf("servers must be an empty list, not null: %s", b)
	}
}

func TestRoutesValidation(t *testing.T) {
	ok := testRoutes()
	if err := ok.validate(); err != nil {
		t.Fatal(err)
	}
	for name, rc := range map[string]RoutesConfig{
		"no routes":          {},
		"priorities 1 apart": {Routes: []RouteTemplate{{Name: "a", Rule: "Host(`a`)", Priority: 100}, {Name: "b", Rule: "Host(`b`)", Priority: 101}}},
		"same priority":      {Routes: []RouteTemplate{{Name: "a", Rule: "Host(`a`)", Priority: 100}, {Name: "b", Rule: "Host(`b`)", Priority: 100}}},
		"duplicate name":     {Routes: []RouteTemplate{{Name: "a", Rule: "Host(`a`)", Priority: 100}, {Name: "a", Rule: "Host(`b`)", Priority: 200}}},
		"no rule":            {Routes: []RouteTemplate{{Name: "a", Priority: 100}}},
		"bad name":           {Routes: []RouteTemplate{{Name: "A B", Rule: "Host(`a`)", Priority: 100}}},
	} {
		if err := rc.validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestLoadRoutesAndFlags(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "routes.json")
	b, _ := json.Marshal(testRoutes())
	if err := os.WriteFile(f, b, 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := parseFlags([]string{"-routes=" + f})
	if err != nil || len(c.routes.Routes) != 3 {
		t.Fatalf("%+v %v", c.routes, err)
	}
	c, err = parseFlags([]string{"-traefik-rule=Host(`a.b`)", "-traefik-middlewares=x@file,y@file", "-traefik-certresolver=le"})
	if err != nil || len(c.routes.Routes) != 1 || len(c.routes.Routes[0].Middlewares) != 2 || c.routes.Routes[0].TLS["certResolver"] != "le" {
		t.Fatalf("%+v %v", c.routes, err)
	}
	if _, err := parseFlags(nil); err == nil {
		t.Fatal("no routes must be refused")
	}
	if _, err := parseFlags([]string{"-routes=" + f, "-traefik-rule=Host(`a`)"}); err == nil {
		t.Fatal("both -routes and -traefik-rule must be refused")
	}
}
