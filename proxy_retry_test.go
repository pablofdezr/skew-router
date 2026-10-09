package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// A refused connection is retried until the upstream comes up.
func TestRetryDialRecoversFromRefusedConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listening: connections are refused

	var hits atomic.Int32
	go func() {
		time.Sleep(300 * time.Millisecond)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return
		}
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusOK)
		})}
		_ = srv.Serve(l)
	}()

	rt := retryDial{next: newTransport(), attempts: 10, wait: 100 * time.Millisecond}
	req := httptest.NewRequest(http.MethodGet, "http://"+addr+"/es", nil)
	req.RequestURI = ""
	res, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("expected a response after retrying, got %v", err)
	}
	if res.StatusCode != http.StatusOK || hits.Load() != 1 {
		t.Fatalf("status %d, hits %d", res.StatusCode, hits.Load())
	}
}

// Errors other than a failed dial are returned at once.
func TestRetryDialDoesNotRetryOtherErrors(t *testing.T) {
	var calls int
	rt := retryDial{next: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return nil, http.ErrHandlerTimeout
	}), attempts: 5, wait: time.Millisecond}
	req := httptest.NewRequest(http.MethodGet, "http://example.test/", nil)
	if _, err := rt.RoundTrip(req); err == nil || calls != 1 {
		t.Fatalf("calls %d err %v", calls, err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
