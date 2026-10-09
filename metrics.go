package main

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
)

// Outcome is how a request with a given id is routed (Routes.Pick); the generated Traefik
// routes encode the same decision.
type Outcome int

const (
	OutcomeUnpinned    Outcome = iota // no deployment id → serving (active) version
	OutcomeLatest                     // id of the serving (active) version
	OutcomePinned                     // id of an older, still-alive version (skew avoided)
	OutcomeFallback                   // unknown or retired id → serving version
	OutcomeUnavailable                // no ready version at all → 503
	numOutcomes
)

var outcomeNames = [numOutcomes]string{"unpinned", "latest", "pinned", "fallback", "unavailable"}

func (o Outcome) String() string { return outcomeNames[o] }

// Metrics is a tiny Prometheus text-format exporter; the handful of
// series here doesn't justify the client library.
type Metrics struct {
	Retired  atomic.Uint64
	Revoked  atomic.Uint64
	routable atomic.Int64

	mu     sync.Mutex
	active string
}

func (m *Metrics) SetRoutable(n int) { m.routable.Store(int64(n)) }

func (m *Metrics) SetActive(name string) {
	m.mu.Lock()
	m.active = name
	m.mu.Unlock()
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	m.write(w)
}

func (m *Metrics) write(w io.Writer) {
	fmt.Fprintln(w, "# HELP skew_router_versions_routable Web versions currently receiving traffic.")
	fmt.Fprintln(w, "# TYPE skew_router_versions_routable gauge")
	fmt.Fprintf(w, "skew_router_versions_routable %d\n", m.routable.Load())
	fmt.Fprintln(w, "# HELP skew_router_versions_retired_total Web versions scaled to 0 or deleted by retention.")
	fmt.Fprintln(w, "# TYPE skew_router_versions_retired_total counter")
	fmt.Fprintf(w, "skew_router_versions_retired_total %d\n", m.Retired.Load())
	fmt.Fprintln(w, "# HELP skew_router_versions_revoked_total Web versions revoked by a security fix (unrouted at once, then deleted).")
	fmt.Fprintln(w, "# TYPE skew_router_versions_revoked_total counter")
	fmt.Fprintf(w, "skew_router_versions_revoked_total %d\n", m.Revoked.Load())
	m.mu.Lock()
	active := m.active
	m.mu.Unlock()
	if active != "" {
		fmt.Fprintln(w, "# HELP skew_router_active_info The version the pointer designates (unpinned traffic).")
		fmt.Fprintln(w, "# TYPE skew_router_active_info gauge")
		fmt.Fprintf(w, "skew_router_active_info{version=%q} 1\n", active)
	}
}
