package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

// DeploymentIDHeader is what Next sends on client navigations, prefetches
// and Server Actions once `deploymentId` is set in next.config.
const DeploymentIDHeader = "X-Deployment-Id"

// deploymentID extracts the build a request belongs to: the header Next's
// client router sends, else the `?dpl=` query Next appends to every
// /_next/static asset URL (and next/image URLs). Values that can't be a
// label value are ignored, i.e. treated as unpinned.
func deploymentID(r *http.Request) string {
	id := r.Header.Get(DeploymentIDHeader)
	if id == "" && strings.Contains(r.URL.RawQuery, "dpl=") {
		id = r.URL.Query().Get("dpl")
	}
	if !validID(id) {
		return ""
	}
	return id
}

// validID matches a Kubernetes label value (what the id is stored as).
func validID(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		alnum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if !alnum && !((c == '-' || c == '_' || c == '.') && i > 0 && i < len(s)-1) {
			return false
		}
	}
	return true
}

type upstreamKey struct{}

// Proxy routes each request to the web version its deployment id pins it
// to, or to the latest version. Bodies are streamed both ways: no
// buffering, flushed immediately (RSC streaming, SSE), and connection
// upgrades (WebSocket) are tunnelled by httputil.ReverseProxy itself.
type Proxy struct {
	routes  func() *Routes
	metrics *Metrics
	log     *slog.Logger
	rp      *httputil.ReverseProxy
}

func NewProxy(routes func() *Routes, m *Metrics, log *slog.Logger) *Proxy {
	p := &Proxy{routes: routes, metrics: m, log: log}
	p.rp = &httputil.ReverseProxy{
		Rewrite:       rewrite,
		Transport:     retryDial{next: newTransport(), attempts: 5, wait: 200 * time.Millisecond},
		FlushInterval: -1,
		ErrorHandler:  p.proxyError,
	}
	return p
}

func newTransport() *http.Transport {
	return &http.Transport{
		Proxy: nil, // in-cluster hop: never via an env-configured proxy
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 128,
		IdleConnTimeout:     90 * time.Second,
		// Time to first byte of the response headers. Streaming SSR sends
		// headers early, so this only trips on a hung upstream.
		ResponseHeaderTimeout: 2 * time.Minute,
		ExpectContinueTimeout: 1 * time.Second,
		// Pass Accept-Encoding through untouched; Next compresses.
		DisableCompression: true,
	}
}

// retryDial retries a request whose connection could not be opened. A
// dial error means nothing reached the upstream, so any method is safe to
// resend. It covers the instant a new version becomes active: its pods are
// ready, but kube-proxy may not have programmed its Service on this node
// yet, and the first connections are refused (one 502 on 2026-10-04).
type retryDial struct {
	next     http.RoundTripper
	attempts int
	wait     time.Duration
}

func (t retryDial) RoundTrip(r *http.Request) (*http.Response, error) {
	var err error
	for i := 0; i < t.attempts; i++ {
		var res *http.Response
		res, err = t.next.RoundTrip(r)
		if err == nil || !isDialError(err) {
			return res, err
		}
		select {
		case <-r.Context().Done():
			return nil, err
		case <-time.After(t.wait):
		}
	}
	return nil, err
}

func isDialError(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

// rewrite points the outbound request at the chosen version while keeping
// what the client and Traefik sent: the Host header and the
// X-Forwarded-* chain (the web app trusts the first X-Forwarded-For
// entry, which Traefik set). ReverseProxy strips X-Forwarded-For/-Host/
// -Proto from the outbound request before calling Rewrite; copy them back
// unchanged rather than appending this hop.
func rewrite(pr *httputil.ProxyRequest) {
	target := pr.In.Context().Value(upstreamKey{}).(*url.URL)
	pr.SetURL(target)
	pr.Out.Host = pr.In.Host
	for _, h := range []string{"X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
		if v, ok := pr.In.Header[h]; ok {
			pr.Out.Header[h] = v
		}
	}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	v, outcome := p.routes().Pick(deploymentID(r))
	p.metrics.Inc(outcome)
	if v == nil {
		http.Error(w, "no web version available", http.StatusServiceUnavailable)
		return
	}
	ctx := context.WithValue(r.Context(), upstreamKey{}, v.Upstream)
	p.rp.ServeHTTP(w, r.WithContext(ctx))
}

func (p *Proxy) proxyError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, context.Canceled) {
		// The client went away; nothing to answer and nothing to log.
		w.WriteHeader(499)
		return
	}
	target := r.Context().Value(upstreamKey{}).(*url.URL)
	p.log.Warn("upstream error", "upstream", target.Host, "path", r.URL.Path, "err", err)
	w.WriteHeader(http.StatusBadGateway)
}
