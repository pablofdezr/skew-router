package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestMeasureOverhead measures what the router adds to a request: the same request sent straight
// to a web version and through the router, on loopback, so the network is the same in both and
// the difference is the router's own work (reading the id, picking the version, proxying).
//
//	SKEW_BENCH=1 go test -run TestMeasureOverhead -v -count=1
//	SKEW_BENCH_N=20000 sets the number of requests per case (default 10000).
func TestMeasureOverhead(t *testing.T) {
	if os.Getenv("SKEW_BENCH") == "" {
		t.Skip("set SKEW_BENCH=1 to measure")
	}
	n := 10000
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

	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64}}
	do := func(url string, hdr map[string]string) time.Duration {
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		start := time.Now()
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, res.Body)
		res.Body.Close()
		return time.Since(start)
	}
	cases := []struct {
		name string
		url  string
		hdr  map[string]string
	}{
		{"direct to the version", newSrv.URL + "/", nil},
		{"router, no id (unpinned)", h.router.URL + "/", nil},
		{"router, pinned by header", h.router.URL + "/", map[string]string{DeploymentIDHeader: "old111"}},
		{"router, pinned by ?dpl=", h.router.URL + "/_next/static/chunk.js?dpl=old111", nil},
	}
	fmt.Printf("\n%s/%s, %d CPUs, %d sequential requests per case (keep-alive)\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), n)
	fmt.Printf("%-28s %9s %9s %9s\n", "case", "p50", "p90", "p99")
	for _, c := range cases {
		for i := 0; i < 500; i++ { // warm connections
			do(c.url, c.hdr)
		}
		d := make([]time.Duration, n)
		for i := range d {
			d[i] = do(c.url, c.hdr)
		}
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		q := func(p float64) time.Duration { return d[int(float64(len(d)-1)*p)] }
		fmt.Printf("%-28s %9s %9s %9s\n", c.name, us(q(0.50)), us(q(0.90)), us(q(0.99)))
	}

	// Throughput with 32 concurrent clients, direct and through the router.
	for _, c := range []struct{ name, url string }{{"direct", newSrv.URL + "/"}, {"router", h.router.URL + "/"}} {
		const workers = 32
		var wg sync.WaitGroup
		start := time.Now()
		per := n / workers
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < per; i++ {
					do(c.url, nil)
				}
			}()
		}
		wg.Wait()
		el := time.Since(start)
		fmt.Printf("throughput %-8s %6.0f req/s (32 concurrent clients)\n", c.name, float64(per*workers)/el.Seconds())
	}
}

func us(d time.Duration) string { return fmt.Sprintf("%.0f µs", float64(d.Microseconds())) }

// BenchmarkPick is the routing decision alone: reading nothing, just choosing a version.
func BenchmarkPick(b *testing.B) {
	r := &Routes{Serving: &Version{ID: "new222"}, byID: map[string]*Version{"old111": {ID: "old111"}, "new222": {ID: "new222"}}}
	ids := []string{"", "new222", "old111", "gone99"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Pick(ids[i%len(ids)])
	}
}

// BenchmarkDeploymentID is reading the id off a request (header, then ?dpl=).
func BenchmarkDeploymentID(b *testing.B) {
	req, _ := http.NewRequest(http.MethodGet, "http://x/_next/static/chunks/main.js?dpl=old111", nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deploymentID(req)
	}
}
