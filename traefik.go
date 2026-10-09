package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Traefik mode (-mode=traefik): the router stops carrying traffic. Instead it serves, on its
// admin port, the dynamic configuration for Traefik's HTTP provider, built from the same routing
// table the proxy reads:
//
//   - one router per routable version, matching its id in the x-deployment-id header or the
//     ?dpl= query parameter, pointing straight at that version's Service;
//   - one default router, lower priority, for everything else (no id, a retired or revoked id),
//     pointing at the serving version;
//   - with no version ready, the default router points at a service with no servers, so Traefik
//     answers 503, the same as the proxy does.
//
// Requests then go Traefik → version with no extra hop. Pointer moves, retention and revocation
// work as before; they show up in Traefik on its next poll of /traefik. Every replica serves the
// same configuration (it comes from the same pure plan), and if every replica is down Traefik
// keeps the last configuration it fetched, so routing goes on, frozen, until one is back.

// TraefikOptions shape the routers: which requests they cover and how Traefik serves them.
type TraefikOptions struct {
	Rule         string   // the site's own match, e.g. Host(`www.example.com`); required
	EntryPoints  []string // e.g. websecure
	Middlewares  []string // e.g. security-headers@file
	CertResolver string   // e.g. le; empty = no tls block
	TLS          bool     // tls: {} with the default certificate when no resolver is set
	Prefix       string   // router and service name prefix
}

type traefikDynamic struct {
	HTTP traefikHTTP `json:"http"`
}

type traefikHTTP struct {
	Routers  map[string]traefikRouter  `json:"routers"`
	Services map[string]traefikService `json:"services"`
}

type traefikRouter struct {
	Rule        string         `json:"rule"`
	Priority    int            `json:"priority"`
	EntryPoints []string       `json:"entryPoints,omitempty"`
	Middlewares []string       `json:"middlewares,omitempty"`
	Service     string         `json:"service"`
	TLS         map[string]any `json:"tls,omitempty"`
}

type traefikService struct {
	LoadBalancer traefikLB `json:"loadBalancer"`
}

type traefikLB struct {
	Servers        []traefikServer `json:"servers"`
	PassHostHeader bool            `json:"passHostHeader"`
}

type traefikServer struct {
	URL string `json:"url"`
}

// Priorities: a pinned router must beat the default one for the same host.
const (
	priorityPinned  = 20000
	priorityDefault = 10000
)

var nameUnsafe = regexp.MustCompile(`[^a-zA-Z0-9-]`)

// TraefikConfig turns a routing table into Traefik dynamic configuration.
func TraefikConfig(r *Routes, o TraefikOptions) traefikDynamic {
	if o.Prefix == "" {
		o.Prefix = "skew"
	}
	out := traefikDynamic{HTTP: traefikHTTP{Routers: map[string]traefikRouter{}, Services: map[string]traefikService{}}}
	router := func(rule string, prio int, svc string) traefikRouter {
		rt := traefikRouter{Rule: rule, Priority: prio, EntryPoints: o.EntryPoints, Middlewares: o.Middlewares, Service: svc}
		switch {
		case o.CertResolver != "":
			rt.TLS = map[string]any{"certResolver": o.CertResolver}
		case o.TLS:
			rt.TLS = map[string]any{}
		}
		return rt
	}
	service := func(v *Version) traefikService {
		lb := traefikLB{PassHostHeader: true, Servers: []traefikServer{}}
		if v != nil && v.Upstream != nil {
			lb.Servers = append(lb.Servers, traefikServer{URL: v.Upstream.String()})
		}
		return traefikService{LoadBalancer: lb}
	}
	name := func(s string) string { return o.Prefix + "-" + nameUnsafe.ReplaceAllString(s, "-") }

	// Default: unpinned traffic, unknown, retired and revoked ids.
	def := name("default")
	out.HTTP.Routers[def] = router(o.Rule, priorityDefault, def)
	var serving *Version
	if r != nil {
		serving = r.Serving
	}
	out.HTTP.Services[def] = service(serving)

	if r == nil {
		return out
	}
	ids := make([]string, 0, len(r.byID))
	for id := range r.byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		v := r.byID[id]
		n := name(id)
		// Ids are valid label values (validID), so they can't break out of the backticks.
		rule := fmt.Sprintf("(%s) && (Header(`%s`, `%s`) || Query(`dpl`, `%s`))", o.Rule, DeploymentIDHeader, id, id)
		out.HTTP.Routers[n] = router(rule, priorityPinned, n)
		out.HTTP.Services[n] = service(v)
	}
	return out
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
