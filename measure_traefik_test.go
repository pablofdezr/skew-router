package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestMeasureTraefik checks the generated routes against a real Traefik, then measures them.
//
// Correctness: two route templates, a static-assets one (priority 500) and a catch-all (100),
// each with its own middleware that stamps the response (X-Route: static / web). A pinned
// request must reach its version AND keep its template's middleware.
//
// Throughput: skew routes vs a plain Traefik route straight to the version (no skew protection).
//
// Run Traefik first, on the host network, polling this test's config endpoint:
//
//	docker run -d --name bench-traefik --network host traefik:v3.6 \
//	  --entryPoints.bench.address=127.0.0.1:18000 \
//	  --providers.http.endpoint=http://127.0.0.1:19090/traefik --providers.http.pollInterval=1s
//	SKEW_TRAEFIK=127.0.0.1:18000 go test -run TestMeasureTraefik -v -count=1
func TestMeasureTraefik(t *testing.T) {
	traefikAddr := os.Getenv("SKEW_TRAEFIK")
	if traefikAddr == "" {
		t.Skip("set SKEW_TRAEFIK=host:port of a Traefik polling 127.0.0.1:19090/traefik")
	}
	n := 40000
	if v, err := strconv.Atoi(os.Getenv("SKEW_BENCH_N")); err == nil && v > 0 {
		n = v
	}
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	t0 := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	oldSrv := upstream(t, "old", release)
	newSrv := upstream(t, "new", release)
	h := newHarness(t, t0.Add(2*time.Minute),
		newVersion(t, "web-old", "old111", t0, oldSrv, true),
		newVersion(t, "web-new", "new222", t0.Add(time.Minute), newSrv, true))
	h.reconcile()

	rc := RoutesConfig{Routes: []RouteTemplate{
		{Name: "static", Rule: "Host(`app.test`) && PathPrefix(`/_next/static/`)", Priority: 500, EntryPoints: []string{"bench"}, Middlewares: []string{"mark-static"}},
		{Name: "web", Rule: "Host(`app.test`)", Priority: 100, EntryPoints: []string{"bench"}, Middlewares: []string{"mark-web"}},
	}}
	if err := rc.validate(); err != nil {
		t.Fatal(err)
	}
	type mw struct {
		Headers struct {
			CustomResponseHeaders map[string]string `json:"customResponseHeaders"`
		} `json:"headers"`
	}
	stamp := func(v string) mw {
		var m mw
		m.Headers.CustomResponseHeaders = map[string]string{"X-Route": v}
		return m
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /traefik", func(w http.ResponseWriter, _ *http.Request) {
		c := TraefikConfig(h.ctrl.Routes(), rc)
		// The baseline: the same Traefik, straight to the active version, no skew routes.
		c.HTTP.Routers["baseline"] = traefikRouter{Rule: "Host(`base.test`)", Priority: 1, EntryPoints: []string{"bench"}, Service: "baseline"}
		c.HTTP.Services["baseline"] = traefikService{LoadBalancer: traefikLB{PassHostHeader: true, Servers: []traefikServer{{URL: newSrv.URL}}}}
		out := map[string]any{"http": map[string]any{
			"routers": c.HTTP.Routers, "services": c.HTTP.Services,
			"middlewares": map[string]mw{"mark-static": stamp("static"), "mark-web": stamp("web")},
		}}
		_ = json.NewEncoder(w).Encode(out)
	})
	l, err := net.Listen("tcp", "127.0.0.1:19090")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: mux}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })

	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 256}}
	get := func(host, path string, hdr map[string]string) (*http.Response, error) {
		req, _ := http.NewRequest(http.MethodGet, "http://"+traefikAddr+path, nil)
		req.Host = host
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		return client.Do(req)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if res, err := get("app.test", "/", map[string]string{DeploymentIDHeader: "old111"}); err == nil {
			res.Body.Close()
			if res.Header.Get("X-Version") == "old" {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("Traefik never loaded the routes from :19090/traefik")
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, c := range []struct {
		path        string
		hdr         map[string]string
		version     string
		route       string
		description string
	}{
		{"/", nil, "new", "web", "document load → active, catch-all middleware"},
		{"/", map[string]string{DeploymentIDHeader: "old111"}, "old", "web", "pinned navigation → its version, catch-all middleware"},
		{"/_next/static/chunk.js?dpl=old111", nil, "old", "static", "pinned asset → its version, static middleware"},
		{"/_next/static/chunk.js", nil, "new", "static", "unpinned asset → active, static middleware"},
		{"/", map[string]string{DeploymentIDHeader: "gone99"}, "new", "web", "unknown id → active"},
		{"/", map[string]string{DeploymentIDHeader: "new222"}, "new", "web", "the active build's own id → active, default router"},
	} {
		res, err := get("app.test", c.path, c.hdr)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.Header.Get("X-Version") != c.version || res.Header.Get("X-Route") != c.route {
			t.Fatalf("%s: got version %q route %q", c.description, res.Header.Get("X-Version"), res.Header.Get("X-Route"))
		}
		fmt.Printf("ok  %s\n", c.description)
	}

	fmt.Printf("\n%s/%s, %d CPUs, %d requests per case, 32 concurrent clients, through Traefik\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), n)
	fmt.Printf("%-44s %10s %9s %9s\n", "path", "req/s", "p50", "p99")
	for _, c := range []struct {
		name, host string
		hdr        map[string]string
	}{
		{"Traefik → version, no skew protection", "base.test", nil},
		{"Traefik → version, skew routes (pinned)", "app.test", map[string]string{DeploymentIDHeader: "old111"}},
	} {
		const workers = 32
		per := n / workers
		lat := make([][]time.Duration, workers)
		var wg sync.WaitGroup
		start := time.Now()
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				lat[w] = make([]time.Duration, 0, per)
				for i := 0; i < per; i++ {
					s := time.Now()
					res, err := get(c.host, "/", c.hdr)
					if err != nil {
						t.Error(err)
						return
					}
					io.Copy(io.Discard, res.Body)
					res.Body.Close()
					lat[w] = append(lat[w], time.Since(s))
				}
			}(w)
		}
		wg.Wait()
		el := time.Since(start)
		var all []time.Duration
		for _, l := range lat {
			all = append(all, l...)
		}
		sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
		q := func(p float64) time.Duration { return all[int(float64(len(all)-1)*p)] }
		fmt.Printf("%-44s %10.0f %9s %9s\n", c.name, float64(len(all))/el.Seconds(), us(q(0.5)), us(q(0.99)))
	}
}

func us(d time.Duration) string { return fmt.Sprintf("%.0f µs", float64(d.Microseconds())) }
