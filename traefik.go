package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// skew-router carries no traffic. It serves, on its admin port, the dynamic configuration for
// Traefik's HTTP provider (GET /traefik), built from the routing table:
//
//   - for every route template (the site's own routes: the catch-all, a static-assets route with
//     its own rate limit, a route for internal clients…), a default router with the template's
//     rule, priority and middlewares, pointing at the serving version;
//   - for every other routable version and every template, a pinned router: the template's rule
//     AND the version's id in the x-deployment-id header or ?dpl=, one priority above the
//     default, same middlewares, pointing straight at that version's Service. The serving
//     version gets none: the default router already sends its requests there, and leaving it
//     out keeps those requests on the default router's middleware instances. Traefik builds a
//     middleware per router, so in-memory rate limits count per router: with a pinned router
//     for the serving version, a browser on the current build would split its requests across
//     two buckets. This way only tabs on an older build (for its routable hour) count apart;
//   - a service "<prefix>-active" any other route can use (`skew-active@http`), e.g. a redirect.
//
// Requests go Traefik → version with no extra hop. A retired or revoked id has no pinned router,
// so it falls through to the default one, and with no version ready the services have no
// servers: Traefik answers 503. Every replica serves the same configuration (it comes from the
// same pure plan), and if every replica is down Traefik keeps the last one it fetched.
//
// Pinned routers sit at priority+1, so leave a gap of at least 2 between template priorities.

// RouteTemplate is one of the site's routes, as you'd write it in Traefik.
type RouteTemplate struct {
	Name        string         `json:"name"`
	Rule        string         `json:"rule"`
	Priority    int            `json:"priority"`
	EntryPoints []string       `json:"entryPoints,omitempty"`
	Middlewares []string       `json:"middlewares,omitempty"`
	TLS         map[string]any `json:"tls,omitempty"`
}

// RoutesConfig is the -routes file.
type RoutesConfig struct {
	Prefix string          `json:"prefix,omitempty"` // router and service name prefix (default skew)
	Routes []RouteTemplate `json:"routes"`
}

var templateName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,40}$`)

// LoadRoutes reads and validates a -routes file.
func LoadRoutes(path string) (RoutesConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return RoutesConfig{}, err
	}
	var c RoutesConfig
	if err := json.Unmarshal(b, &c); err != nil {
		return c, fmt.Errorf("%s: %w", path, err)
	}
	return c, c.validate()
}

func (c *RoutesConfig) validate() error {
	if c.Prefix == "" {
		c.Prefix = "skew"
	}
	if !templateName.MatchString(c.Prefix) {
		return fmt.Errorf("prefix %q: lowercase letters, digits and dashes", c.Prefix)
	}
	if len(c.Routes) == 0 {
		return errors.New("no routes")
	}
	seen := map[string]bool{}
	prios := map[int]string{}
	for _, r := range c.Routes {
		if !templateName.MatchString(r.Name) || seen[r.Name] {
			return fmt.Errorf("route name %q: lowercase letters, digits and dashes, unique", r.Name)
		}
		seen[r.Name] = true
		if strings.TrimSpace(r.Rule) == "" || r.Priority <= 0 {
			return fmt.Errorf("route %s: rule and a positive priority are required", r.Name)
		}
		// The pinned router sits at priority+1: it must not tie or overtake another template.
		for p, other := range prios {
			if d := r.Priority - p; d >= -1 && d <= 1 {
				return fmt.Errorf("routes %s and %s: priorities %d and %d leave no room for pinned routers (keep them at least 2 apart)", other, r.Name, p, r.Priority)
			}
		}
		prios[r.Priority] = r.Name
	}
	return nil
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

var nameUnsafe = regexp.MustCompile(`[^a-zA-Z0-9-]`)

// TraefikConfig turns a routing table into Traefik dynamic configuration.
func TraefikConfig(r *Routes, c RoutesConfig) traefikDynamic {
	out := traefikDynamic{HTTP: traefikHTTP{Routers: map[string]traefikRouter{}, Services: map[string]traefikService{}}}
	service := func(v *Version) traefikService {
		lb := traefikLB{PassHostHeader: true, Servers: []traefikServer{}}
		if v != nil && v.Upstream != nil {
			lb.Servers = append(lb.Servers, traefikServer{URL: v.Upstream.String()})
		}
		return traefikService{LoadBalancer: lb}
	}
	active := c.Prefix + "-active"
	var serving *Version
	var ids []string
	if r != nil {
		serving = r.Serving
		for id := range r.byID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if serving != nil { // no pinned router or service for the serving version (see above)
		ids = slices.DeleteFunc(ids, func(id string) bool { return id == serving.ID })
	}
	out.HTTP.Services[active] = service(serving)
	for _, id := range ids {
		out.HTTP.Services[c.Prefix+"-v-"+nameUnsafe.ReplaceAllString(id, "-")] = service(r.byID[id])
	}

	for _, t := range c.Routes {
		router := func(rule string, prio int, svc string) traefikRouter {
			return traefikRouter{Rule: rule, Priority: prio, EntryPoints: t.EntryPoints, Middlewares: t.Middlewares, Service: svc, TLS: t.TLS}
		}
		out.HTTP.Routers[c.Prefix+"-"+t.Name] = router(t.Rule, t.Priority, active)
		for _, id := range ids {
			n := c.Prefix + "-v-" + nameUnsafe.ReplaceAllString(id, "-")
			// Ids are valid label values (validID), so they can't break out of the backticks.
			rule := fmt.Sprintf("(%s) && (Header(`%s`, `%s`) || Query(`dpl`, `%s`))", t.Rule, DeploymentIDHeader, id, id)
			out.HTTP.Routers[c.Prefix+"-"+t.Name+"-"+nameUnsafe.ReplaceAllString(id, "-")] = router(rule, t.Priority+1, n)
		}
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
