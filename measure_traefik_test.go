package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestMeasureTraefik compares the two modes behind a real Traefik:
//
//	proxy:   Traefik → skew-router → version
//	traefik: Traefik → version (the router only serves Traefik's routes)
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

	// The config the router serves in traefik mode, plus one extra router that sends
	// Host(router.test) through the router itself: the proxy-mode path, through the same Traefik.
	opts := TraefikOptions{Rule: "Host(`app.test`)", EntryPoints: []string{"bench"}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /traefik", func(w http.ResponseWriter, _ *http.Request) {
		c := TraefikConfig(h.ctrl.Routes(), opts)
		c.HTTP.Routers["via-router"] = traefikRouter{Rule: "Host(`router.test`)", Priority: 1, EntryPoints: []string{"bench"}, Service: "via-router"}
		c.HTTP.Services["via-router"] = traefikService{LoadBalancer: traefikLB{PassHostHeader: true, Servers: []traefikServer{{URL: h.router.URL}}}}
		_ = json.NewEncoder(w).Encode(c)
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
	// Wait until Traefik has loaded the routes, and check they route like the proxy does.
	deadline := time.Now().Add(30 * time.Second)
	for {
		res, err := get("app.test", "/", map[string]string{DeploymentIDHeader: "old111"})
		if err == nil {
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
	check := func(host, path string, hdr map[string]string, want string) {
		res, err := get(host, path, hdr)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if got := res.Header.Get("X-Version"); got != want {
			t.Fatalf("%s %s %v → %q, want %q", host, path, hdr, got, want)
		}
	}
	check("app.test", "/", nil, "new")                                             // unpinned → active
	check("app.test", "/x.js?dpl=old111", nil, "old")                              // pinned by query
	check("app.test", "/", map[string]string{DeploymentIDHeader: "gone99"}, "new") // unknown → active
	check("router.test", "/", map[string]string{DeploymentIDHeader: "old111"}, "old")

	fmt.Printf("\n%s/%s, %d CPUs, %d requests per case, 32 concurrent clients, through Traefik\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), n)
	fmt.Printf("%-40s %10s %9s %9s\n", "path", "req/s", "p50", "p99")
	for _, c := range []struct {
		name, host string
		hdr        map[string]string
	}{
		{"traefik mode: Traefik → version", "app.test", map[string]string{DeploymentIDHeader: "old111"}},
		{"proxy mode: Traefik → router → version", "router.test", map[string]string{DeploymentIDHeader: "old111"}},
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
		fmt.Printf("%-40s %10.0f %9s %9s\n", c.name, float64(len(all))/el.Seconds(), us(q(0.5)), us(q(0.99)))
	}
	_ = url.URL{}
}
