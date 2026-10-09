package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

// End-to-end: real Discovery (informers) + Controller + KubeVersions
// against a fake clientset, and the real proxy in front of httptest
// upstreams standing in for two or three web versions.

const (
	testNS    = "web"
	testLabel = "skew-router.dev/version"
)

// upstream is a fake web version: it answers with its own name and
// echoes what the router forwarded, streams on /stream, and speaks a
// minimal upgrade protocol on /ws.
func upstream(t *testing.T, name string, release <-chan struct{}) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/stream", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, "first-%s\n", name)
		w.(http.Flusher).Flush()
		<-release
		fmt.Fprintf(w, "second-%s\n", name)
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			http.Error(w, "upgrade required", http.StatusUpgradeRequired)
			return
		}
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		fmt.Fprint(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		rw.Flush()
		line, _ := rw.ReadString('\n')
		fmt.Fprintf(rw, "%s:%s", name, line)
		rw.Flush()
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Version", name)
		fmt.Fprintf(w, "host=%s xff=%s proto=%s", r.Host, r.Header.Get("X-Forwarded-For"), r.Header.Get("X-Forwarded-Proto"))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// version holds the objects examples/web-version.yaml would produce for
// one upstream, so tests can create and flip them.
type version struct {
	svc   *corev1.Service
	slice *discoveryv1.EndpointSlice
	dep   *appsv1.Deployment
}

func newVersion(t *testing.T, name, id string, promoted time.Time, srv *httptest.Server, ready bool) version {
	t.Helper()
	u, _ := url.Parse(srv.URL)
	host, portStr, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(portStr)
	lbl := map[string]string{"app": "web", testLabel: id}
	one := int32(1)
	return version{
		svc: &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: testNS, Labels: lbl, UID: types.UID("svc-" + name),
				Annotations:       map[string]string{AnnPromotedAt: promoted.Format(time.RFC3339)},
				CreationTimestamp: metav1.NewTime(promoted),
			},
			Spec: corev1.ServiceSpec{ClusterIP: host, Ports: []corev1.ServicePort{{Name: "http", Port: int32(port)}}},
		},
		slice: &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name: name + "-abcde", Namespace: testNS,
				Labels: map[string]string{discoveryv1.LabelServiceName: name},
			},
			Endpoints: []discoveryv1.Endpoint{{Addresses: []string{host}, Conditions: discoveryv1.EndpointConditions{Ready: &ready}}},
		},
		dep: &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: lbl, UID: types.UID("dep-" + name)},
			Spec:       appsv1.DeploymentSpec{Replicas: &one},
		},
	}
}

type harness struct {
	t      *testing.T
	ctx    context.Context
	client *fake.Clientset
	ctrl   *Controller
	clk    *clock
	router *httptest.Server
}

func newHarness(t *testing.T, at time.Time, versions ...version) *harness {
	t.Helper()
	client := fake.NewClientset()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := &harness{t: t, ctx: ctx, client: client, clk: &clock{at}}
	for _, v := range versions {
		h.create(v)
	}
	disc, err := NewDiscovery(client, DiscoveryConfig{
		Namespace: testNS, Selector: labels.SelectorFromSet(labels.Set{"app": "web"}),
		VersionLabel: testLabel, PortName: "http",
	}, func() {})
	if err != nil {
		t.Fatal(err)
	}
	disc.Start(ctx.Done())
	h.waitFor("informer sync", disc.Synced)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := &Metrics{}
	h.ctrl = &Controller{
		Source:  disc,
		Writer:  &KubeVersions{Client: client, Namespace: testNS, VersionLabel: testLabel},
		Policy:  Policy{TTL: time.Hour, MaxVersions: 2},
		Drain:   10 * time.Second,
		Now:     h.clk.now,
		Log:     log,
		Metrics: m,
	}
	h.router = httptest.NewServer(pickProxy(h.ctrl.Routes))
	t.Cleanup(h.router.Close)
	return h
}

func (h *harness) create(v version) {
	h.t.Helper()
	if _, err := h.client.CoreV1().Services(testNS).Create(h.ctx, v.svc, metav1.CreateOptions{}); err != nil {
		h.t.Fatal(err)
	}
	if _, err := h.client.DiscoveryV1().EndpointSlices(testNS).Create(h.ctx, v.slice, metav1.CreateOptions{}); err != nil {
		h.t.Fatal(err)
	}
	if _, err := h.client.AppsV1().Deployments(testNS).Create(h.ctx, v.dep, metav1.CreateOptions{}); err != nil {
		h.t.Fatal(err)
	}
}

// setReady flips a version's endpoints, as kubelet + the EndpointSlice
// controller would when its pods come up or go away.
func (h *harness) setReady(name string, ready bool) {
	h.t.Helper()
	sl, err := h.client.DiscoveryV1().EndpointSlices(testNS).Get(h.ctx, name+"-abcde", metav1.GetOptions{})
	if err != nil {
		h.t.Fatal(err)
	}
	sl.Endpoints[0].Conditions.Ready = &ready
	if _, err := h.client.DiscoveryV1().EndpointSlices(testNS).Update(h.ctx, sl, metav1.UpdateOptions{}); err != nil {
		h.t.Fatal(err)
	}
	h.waitFor("readiness of "+name, func() bool { v, ok := h.version(name); return ok && v.Ready == ready })
}

// reconcile runs one pass, then waits until the informers reflect the
// pointer it wrote — as the next pass would see it in production.
func (h *harness) reconcile() {
	h.ctrl.Reconcile(h.ctx)
	if d := h.ctrl.Routes(); d != nil && d.Active != nil {
		want := d.Active.ID
		h.waitFor("pointer "+want, func() bool { return h.pointer() == want })
	}
}

func (h *harness) pointer() string { return h.ctrl.Source.Pointer() }

func (h *harness) version(name string) (Version, bool) {
	for _, v := range h.ctrl.Source.Versions() {
		if v.Name == name {
			return v, true
		}
	}
	return Version{}, false
}

func (h *harness) waitFor(what string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (h *harness) get(path string, hdr map[string]string) string {
	h.t.Helper()
	req, _ := http.NewRequest(http.MethodGet, h.router.URL+path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, res.Body)
	return res.Header.Get("X-Version")
}

func (h *harness) deployment(name string) (*appsv1.Deployment, bool) {
	d, err := h.client.AppsV1().Deployments(testNS).Get(h.ctx, name, metav1.GetOptions{})
	return d, err == nil
}

func TestEndToEndRouting(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	h := newHarness(t, t0.Add(2*time.Minute),
		newVersion(t, "web-old", "old111", t0, upstream(t, "old", release), true),
		newVersion(t, "web-new", "new222", t0.Add(time.Minute), upstream(t, "new", release), true),
	)
	h.reconcile()

	t.Run("pointer persisted to the newest ready version", func(t *testing.T) {
		cm, err := h.client.CoreV1().ConfigMaps(testNS).Get(h.ctx, PointerConfigMap, metav1.GetOptions{})
		if err != nil || cm.Data[PointerKeyID] != "new222" || cm.Data[PointerKeyName] != "web-new" {
			t.Fatalf("pointer ConfigMap = %v, %v", cm, err)
		}
	})
	t.Run("pinned asset goes to the old version", func(t *testing.T) {
		if v := h.get("/_next/static/chunks/app.js?dpl=old111", nil); v != "old" {
			t.Fatalf("got %q, want old", v)
		}
	})
	t.Run("x-deployment-id header pins too", func(t *testing.T) {
		if v := h.get("/es/hoteles?_rsc=1", map[string]string{"X-Deployment-Id": "old111"}); v != "old" {
			t.Fatalf("got %q, want old", v)
		}
	})
	t.Run("document navigation goes to the active version", func(t *testing.T) {
		if v := h.get("/es", nil); v != "new" {
			t.Fatalf("got %q, want new", v)
		}
	})
	t.Run("unknown id goes to the active version", func(t *testing.T) {
		if v := h.get("/_next/static/x.js?dpl=nope", nil); v != "new" {
			t.Fatalf("got %q, want new", v)
		}
	})
	t.Run("leader records superseded-at on the Service", func(t *testing.T) {
		h.waitFor("superseded-at", func() bool { v, _ := h.version("web-old"); return !v.SupersededAt.IsZero() })
		svc, _ := h.client.CoreV1().Services(testNS).Get(h.ctx, "web-old", metav1.GetOptions{})
		if svc.Annotations[AnnSupersededAt] != t0.Add(2*time.Minute).Format(time.RFC3339) {
			t.Fatalf("superseded-at = %q", svc.Annotations[AnnSupersededAt])
		}
		if _, ok := svc.Annotations[AnnPromotedAt]; !ok {
			t.Fatal("merge patch dropped the existing annotations")
		}
	})
	t.Run("past TTL the previous version is unrouted, then scaled to 0 and kept", func(t *testing.T) {
		h.clk.t = t0.Add(time.Hour + 2*time.Minute)
		h.reconcile()
		if v := h.get("/_next/static/chunks/app.js?dpl=old111", nil); v != "new" {
			t.Fatalf("version past TTL still served: %q", v)
		}
		h.waitFor("expired-at", func() bool { v, _ := h.version("web-old"); return !v.ExpiredAt.IsZero() })
		h.clk.t = h.clk.t.Add(9 * time.Second)
		h.reconcile()
		if d, _ := h.deployment("web-old"); *d.Spec.Replicas != 1 {
			t.Fatal("scaled down before the drain")
		}
		h.clk.t = h.clk.t.Add(time.Second)
		h.reconcile()
		d, ok := h.deployment("web-old")
		if !ok {
			t.Fatal("previous version deleted; it must be kept at 0 replicas")
		}
		if *d.Spec.Replicas != 0 || d.Annotations[AnnScaledFrom] != "1" {
			t.Fatalf("replicas = %d, scaled-from = %q", *d.Spec.Replicas, d.Annotations[AnnScaledFrom])
		}
		h.waitFor("scaled-down-at", func() bool { v, _ := h.version("web-old"); return !v.ScaledDownAt.IsZero() })
		if _, ok := h.deployment("web-new"); !ok {
			t.Fatal("active deployment was deleted")
		}
	})
}

func TestEndToEndDeployThenInstantRollback(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	a := newVersion(t, "web-a", "aaa", t0, upstream(t, "a", release), true)
	b := newVersion(t, "web-b", "bbb", t0.Add(time.Minute), upstream(t, "b", release), true)
	h := newHarness(t, t0.Add(2*time.Minute), a, b)
	h.reconcile()
	if h.pointer() != "bbb" {
		t.Fatalf("pointer = %q, want bbb", h.pointer())
	}

	// Deploy c: created unready — the pointer must not move.
	c := newVersion(t, "web-c", "ccc", t0.Add(3*time.Minute), upstream(t, "c", release), false)
	h.create(c)
	h.waitFor("web-c discovered", func() bool { _, ok := h.version("web-c"); return ok })
	h.clk.t = t0.Add(4 * time.Minute)
	h.reconcile()
	if h.pointer() != "bbb" || h.get("/", nil) != "b" {
		t.Fatalf("pointer moved to an unready version: %q", h.pointer())
	}
	if v := h.get("/x.js?dpl=aaa", nil); v != "a" {
		t.Fatalf("a should still be pinnable during the rollout: %q", v)
	}

	// c ready: pointer → c; b is the previous; a (3rd) is deleted after the drain.
	h.setReady("web-c", true)
	h.reconcile()
	if h.pointer() != "ccc" || h.get("/", nil) != "c" {
		t.Fatalf("pointer = %q, want ccc", h.pointer())
	}
	if v := h.get("/x.js?dpl=bbb", nil); v != "b" {
		t.Fatalf("previous version b must stay pinnable: %q", v)
	}
	if v := h.get("/x.js?dpl=aaa", nil); v != "c" {
		t.Fatalf("a beyond max-versions must fall back: %q", v)
	}
	h.waitFor("expired-at on web-a", func() bool { v, _ := h.version("web-a"); return !v.ExpiredAt.IsZero() })
	h.clk.t = h.clk.t.Add(10 * time.Second)
	h.reconcile()
	if _, ok := h.deployment("web-a"); ok {
		t.Fatal("web-a not deleted after the drain")
	}

	// TTL passes: b is scaled to 0.
	h.waitFor("superseded-at on web-b", func() bool { v, _ := h.version("web-b"); return !v.SupersededAt.IsZero() })
	h.clk.t = h.clk.t.Add(time.Hour)
	h.reconcile()
	h.waitFor("expired-at on web-b", func() bool { v, _ := h.version("web-b"); return !v.ExpiredAt.IsZero() })
	h.clk.t = h.clk.t.Add(10 * time.Second)
	h.reconcile()
	if d, _ := h.deployment("web-b"); *d.Spec.Replicas != 0 {
		t.Fatal("previous version not scaled to 0 after the TTL")
	}
	h.setReady("web-b", false) // its pods are gone
	h.waitFor("scaled-down-at on web-b", func() bool { v, _ := h.version("web-b"); return !v.ScaledDownAt.IsZero() })

	// Instant rollback to b, as the deploy tooling's promote does it: stamp
	// promoted-at and clear the timers, scale back up, wait for ready.
	h.clk.t = h.clk.t.Add(time.Minute)
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q,%q:null,%q:null,%q:null}}}`,
		AnnPromotedAt, h.clk.t.Format(time.RFC3339), AnnSupersededAt, AnnExpiredAt, AnnScaledDownAt)
	if _, err := h.client.CoreV1().Services(testNS).Patch(h.ctx, "web-b", types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.client.AppsV1().Deployments(testNS).Patch(h.ctx, "web-b", types.MergePatchType, []byte(`{"spec":{"replicas":1}}`), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	h.waitFor("promotion seen", func() bool { v, _ := h.version("web-b"); return v.PromotedAt.Equal(h.clk.t) })
	h.reconcile()
	if h.pointer() != "ccc" {
		t.Fatal("pointer moved before the rollback target had ready endpoints")
	}
	if d, _ := h.deployment("web-b"); *d.Spec.Replicas != 1 {
		t.Fatal("router scaled the rollback target back down while it was starting")
	}
	h.setReady("web-b", true)
	h.reconcile()
	if h.pointer() != "bbb" || h.get("/", nil) != "b" {
		t.Fatalf("after rollback pointer = %q, serving %q", h.pointer(), h.get("/", nil))
	}
	if v := h.get("/x.js?dpl=ccc", nil); v != "c" {
		t.Fatalf("rolled-back-from version must stay pinnable within the TTL: %q", v)
	}
}

// A security fix shipped with WEB_REVOKE_OLDER=1: the new version's
// Service carries revokes-older from its creation.
func TestEndToEndSecurityFixRevokesOlder(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	h := newHarness(t, t0.Add(2*time.Minute),
		newVersion(t, "web-a", "aaa", t0, upstream(t, "a", release), true),
		newVersion(t, "web-b", "bbb", t0.Add(time.Minute), upstream(t, "b", release), true),
	)
	h.reconcile()

	fix := newVersion(t, "web-fix", "fff", t0.Add(3*time.Minute), upstream(t, "fix", release), false)
	fix.svc.Annotations[AnnRevokesOlder] = "true"
	fix.dep.Annotations = map[string]string{AnnRevokesOlder: "true"}
	h.create(fix)
	h.waitFor("web-fix discovered", func() bool { v, ok := h.version("web-fix"); return ok && v.RevokesOlder })
	h.clk.t = t0.Add(4 * time.Minute)
	h.reconcile()
	if h.pointer() != "bbb" || h.get("/", nil) != "b" {
		t.Fatalf("pointer moved before the fix was ready: %q", h.pointer())
	}
	if v := h.get("/x.js?dpl=aaa", nil); v != "a" {
		t.Fatalf("older versions must stay routable until the fix is ready: %q", v)
	}

	h.setReady("web-fix", true)
	h.reconcile()
	if h.pointer() != "fff" || h.get("/", nil) != "fix" {
		t.Fatalf("pointer = %q, want fff", h.pointer())
	}
	for _, id := range []string{"aaa", "bbb"} {
		if v := h.get("/_next/static/chunks/app.js?dpl="+id, nil); v != "fix" {
			t.Fatalf("?dpl=%s served by %q, want the fix", id, v)
		}
		if v := h.get("/es?_rsc=1", map[string]string{"X-Deployment-Id": id}); v != "fix" {
			t.Fatalf("x-deployment-id %s served by %q, want the fix", id, v)
		}
	}
	h.waitFor("revoked-at on web-b", func() bool { v, _ := h.version("web-b"); return !v.RevokedAt.IsZero() })
	h.clk.t = h.clk.t.Add(10 * time.Second)
	h.reconcile()
	for _, n := range []string{"web-a", "web-b"} {
		if _, ok := h.deployment(n); ok {
			t.Fatalf("%s not deleted after the drain", n)
		}
	}
	if _, ok := h.deployment("web-fix"); !ok {
		t.Fatal("the fix was deleted")
	}
}

// promote.sh --revoke-older after the fact: the annotation lands on the version that is
// already active.
func TestEndToEndRevokeOlderAfterTheFact(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	h := newHarness(t, t0.Add(2*time.Minute),
		newVersion(t, "web-a", "aaa", t0, upstream(t, "a", release), true),
		newVersion(t, "web-b", "bbb", t0.Add(time.Minute), upstream(t, "b", release), true),
	)
	h.reconcile()
	if v := h.get("/x.js?dpl=aaa", nil); v != "a" {
		t.Fatalf("previous version not pinnable before the revocation: %q", v)
	}

	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:"true"}}}`, AnnRevokesOlder)
	if _, err := h.client.CoreV1().Services(testNS).Patch(h.ctx, "web-b", types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		t.Fatal(err)
	}
	h.waitFor("revokes-older seen", func() bool { v, _ := h.version("web-b"); return v.RevokesOlder })
	h.reconcile()
	if v := h.get("/x.js?dpl=aaa", nil); v != "b" {
		t.Fatalf("revoked version still served a pinned request: %q", v)
	}
	h.waitFor("revoked-at on web-a", func() bool { v, _ := h.version("web-a"); return !v.RevokedAt.IsZero() })
	h.clk.t = h.clk.t.Add(10 * time.Second)
	h.reconcile()
	if _, ok := h.deployment("web-a"); ok {
		t.Fatal("web-a not deleted after the drain")
	}
}

func TestKubeVersionsRefusesForeignObjects(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "core-api", Namespace: testNS, Labels: map[string]string{"app": "core-api"}}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "core-api", Namespace: testNS}},
	)
	k := &KubeVersions{Client: client, Namespace: testNS, VersionLabel: testLabel}
	if err := k.Retire(ctx, Version{Name: "core-api", ID: "x"}); err == nil {
		t.Fatal("retired an object without the version label")
	}
	if err := k.ScaleDown(ctx, Version{Name: "core-api", ID: "x"}); err == nil {
		t.Fatal("scaled an object without the version label")
	}
	if _, err := client.AppsV1().Deployments(testNS).Get(ctx, "core-api", metav1.GetOptions{}); err != nil {
		t.Fatal("foreign deployment was deleted")
	}

	// Label present but with another id: also refused.
	k.Client = fake.NewClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web-a", Namespace: testNS, Labels: map[string]string{testLabel: "other"}}},
	)
	if err := k.Retire(ctx, Version{Name: "web-a", ID: "aaa"}); err == nil {
		t.Fatal("retired an object labelled with a different id")
	}

	// Missing objects are fine (another replica already deleted them).
	k.Client = fake.NewClientset()
	if err := k.Retire(ctx, Version{Name: "web-gone", ID: "gone"}); err != nil {
		t.Fatalf("missing objects: %v", err)
	}
}

func TestLeaderElectionPicksOneReplica(t *testing.T) {
	client := fake.NewClientset()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	var a, b atomic.Bool
	go runLeaderElection(ctx, client, testNS, "skew-router", "pod-a", &a, func() {}, log)
	go runLeaderElection(ctx, client, testNS, "skew-router", "pod-b", &b, func() {}, log)
	deadline := time.Now().Add(5 * time.Second)
	for !a.Load() && !b.Load() {
		if time.Now().After(deadline) {
			t.Fatal("nobody became leader")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i := 0; i < 50; i++ {
		if a.Load() && b.Load() {
			t.Fatal("two leaders at once")
		}
		time.Sleep(10 * time.Millisecond)
	}
	lease, err := client.CoordinationV1().Leases(testNS).Get(ctx, "skew-router", metav1.GetOptions{})
	if err != nil || lease.Spec.HolderIdentity == nil {
		t.Fatalf("lease not held: %v", err)
	}
}

// pickProxy stands in for Traefik in these tests: it sends each request where the generated
// routes would (Pick is the same decision: the id from the header or ?dpl=, a pinned version if
// it is routable, the serving one otherwise, 503 with none). TestMeasureTraefik checks the
// generated routes against a real Traefik.
func pickProxy(routes func() *Routes) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, _ := routes().Pick(deploymentID(r))
		if v == nil {
			http.Error(w, "no web version available", http.StatusServiceUnavailable)
			return
		}
		rp := &httputil.ReverseProxy{Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(v.Upstream)
			pr.Out.Host = pr.In.Host
		}}
		rp.ServeHTTP(w, r)
	})
}
