package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// BenchmarkProxyParallel is the proxy alone under concurrent load: a fixed routing table, one
// backend, many client goroutines. Profile it with -cpuprofile to see where a request's CPU goes.
func BenchmarkProxyParallel(b *testing.B) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "hello")
	}))
	defer backend.Close()
	u, _ := url.Parse(backend.URL)
	v := &Version{ID: "new222", Name: "web-new222", Upstream: u}
	routes := &Routes{Serving: v, Active: v, byID: map[string]*Version{"new222": v}}
	p := NewProxy(func() *Routes { return routes }, &Metrics{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	front := httptest.NewServer(p)
	defer front.Close()
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 256}}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res, err := client.Get(front.URL + "/")
			if err != nil {
				b.Fatal(err)
			}
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
		}
	})
}
