package main

import (
	"context"
	"io"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"
)

type fakeSource struct {
	synced   bool
	pointer  string
	versions []Version
}

func (f *fakeSource) Synced() bool        { return f.synced }
func (f *fakeSource) Pointer() string     { return f.pointer }
func (f *fakeSource) Versions() []Version { return append([]Version(nil), f.versions...) }

func (f *fakeSource) find(name string) *Version {
	for i := range f.versions {
		if f.versions[i].Name == name {
			return &f.versions[i]
		}
	}
	return nil
}

// fakeWriter applies writes to the source, as the informers would reflect
// them from the cluster.
type fakeWriter struct {
	src                *fakeSource
	retired, scaled    []string
	writes, pointerSet int
}

func (f *fakeWriter) SetPointer(_ context.Context, v Version, _ time.Time) error {
	f.pointerSet++
	f.src.pointer = v.ID
	return nil
}

func (f *fakeWriter) Annotate(_ context.Context, v Version, ann map[string]string) error {
	f.writes++
	x := f.src.find(v.Name)
	if x == nil {
		return nil
	}
	for k, val := range ann {
		var t time.Time
		if val != "" {
			t, _ = time.Parse(time.RFC3339, val)
		}
		switch k {
		case AnnSupersededAt:
			x.SupersededAt = t
		case AnnExpiredAt:
			x.ExpiredAt = t
		case AnnScaledDownAt:
			x.ScaledDownAt = t
		case AnnRevokedAt:
			x.RevokedAt = t
		}
	}
	return nil
}

func (f *fakeWriter) ScaleDown(_ context.Context, v Version) error {
	f.scaled = append(f.scaled, v.Name)
	if x := f.src.find(v.Name); x != nil {
		x.Ready = false // no pods, no ready endpoints
	}
	return nil
}

func (f *fakeWriter) Retire(_ context.Context, v Version) error {
	f.retired = append(f.retired, v.Name)
	kept := f.src.versions[:0]
	for _, x := range f.src.versions {
		if x.Name != v.Name {
			kept = append(kept, x)
		}
	}
	f.src.versions = kept
	return nil
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestController(src *fakeSource, clk *clock) (*Controller, *fakeWriter) {
	w := &fakeWriter{src: src}
	return &Controller{
		Source:  src,
		Writer:  w,
		Policy:  Policy{TTL: time.Hour, MaxVersions: 2},
		Drain:   10 * time.Second,
		Now:     clk.now,
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: &Metrics{},
	}, w
}

func serving(c *Controller) string { return name(c.Routes().Serving) }

func TestControllerWaitsForSync(t *testing.T) {
	src := &fakeSource{versions: []Version{ver("a", 0, true)}}
	c, w := newTestController(src, &clock{t0})
	c.Reconcile(context.Background())
	if c.Routes() != nil || w.writes+w.pointerSet != 0 {
		t.Fatal("acted before the cache synced")
	}
	src.synced = true
	c.Reconcile(context.Background())
	if serving(c) != "a" || src.pointer != "id-a" {
		t.Fatalf("after sync: serving %q, pointer %q", serving(c), src.pointer)
	}
}

func TestControllerPointerMovesOnlyToReadyVersion(t *testing.T) {
	src := &fakeSource{synced: true, pointer: "id-a", versions: []Version{ver("a", 0, true)}}
	clk := &clock{t0.Add(time.Minute)}
	c, _ := newTestController(src, clk)
	ctx := context.Background()

	src.versions = append(src.versions, ver("b", 2*time.Minute, false)) // rolling out
	clk.advance(2 * time.Minute)
	c.Reconcile(ctx)
	if src.pointer != "id-a" || serving(c) != "a" {
		t.Fatalf("pointer moved to an unready version: pointer %q serving %q", src.pointer, serving(c))
	}
	src.find("b").Ready = true
	c.Reconcile(ctx)
	if src.pointer != "id-b" || serving(c) != "b" {
		t.Fatalf("pointer %q serving %q, want b", src.pointer, serving(c))
	}
	if src.find("a").SupersededAt.IsZero() {
		t.Fatal("superseded-at not recorded for the previous version")
	}
	if v, o := c.Routes().Pick("id-a"); v.Name != "a" || o != OutcomePinned {
		t.Fatalf("previous must stay pinnable, got %s/%s", v.Name, o)
	}
}

func TestControllerTTLScalesDownAndKeepsPrevious(t *testing.T) {
	src := &fakeSource{synced: true, pointer: "id-b", versions: []Version{ver("a", 0, true), ver("b", time.Minute, true)}}
	clk := &clock{t0.Add(2 * time.Minute)}
	c, w := newTestController(src, clk)
	ctx := context.Background()
	c.Reconcile(ctx) // superseded-at(a) = t0+2m

	clk.t = t0.Add(time.Hour + 2*time.Minute)
	c.Reconcile(ctx)
	if v, o := c.Routes().Pick("id-a"); v.Name != "b" || o != OutcomeFallback {
		t.Fatalf("a past TTL must fall back to the active version, got %s/%s", v.Name, o)
	}
	clk.advance(9 * time.Second)
	c.Reconcile(ctx)
	if len(w.scaled) != 0 {
		t.Fatalf("scaled before the drain: %v", w.scaled)
	}
	clk.advance(time.Second)
	c.Reconcile(ctx)
	if len(w.scaled) != 1 || w.scaled[0] != "a" || len(w.retired) != 0 {
		t.Fatalf("scaled %v retired %v, want a scaled and nothing deleted", w.scaled, w.retired)
	}
	if src.find("a").ScaledDownAt.IsZero() {
		t.Fatal("scaled-down-at not recorded")
	}
	// Days later it is still there, at 0, for an instant rollback.
	clk.advance(72 * time.Hour)
	c.Reconcile(ctx)
	if src.find("a") == nil || len(w.scaled) != 1 {
		t.Fatalf("previous version touched again: scaled %v retired %v", w.scaled, w.retired)
	}
}

func TestControllerThirdVersionIsDeleted(t *testing.T) {
	src := &fakeSource{synced: true, pointer: "id-b", versions: []Version{
		scaledDown(ver("a", 0, true), time.Hour), ver("b", 2*time.Hour, true),
	}}
	clk := &clock{t0.Add(3 * time.Hour)}
	c, w := newTestController(src, clk)
	ctx := context.Background()
	c.Reconcile(ctx)

	src.versions = append(src.versions, ver("c", 3*time.Hour, true))
	clk.advance(time.Minute)
	c.Reconcile(ctx)
	if src.pointer != "id-c" {
		t.Fatalf("pointer = %q, want c", src.pointer)
	}
	if v, _ := c.Routes().Pick("id-b"); v.Name != "b" {
		t.Fatal("b (now previous) must stay pinnable")
	}
	clk.advance(10 * time.Second)
	c.Reconcile(ctx)
	if len(w.retired) != 1 || w.retired[0] != "a" {
		t.Fatalf("retired = %v, want [a]", w.retired)
	}
	if src.find("b") == nil {
		t.Fatal("previous version b was deleted")
	}
}

func TestControllerTimersSurviveRestart(t *testing.T) {
	// State a previous leader left behind: a superseded 59m ago.
	src := &fakeSource{synced: true, pointer: "id-b", versions: []Version{
		superseded(ver("a", 0, true), time.Minute), ver("b", time.Minute, true),
	}}
	clk := &clock{t0.Add(time.Hour)}
	c, w := newTestController(src, clk) // fresh process, no memory
	c.Reconcile(context.Background())
	if v, _ := c.Routes().Pick("id-a"); v.Name != "a" {
		t.Fatal("a unrouted early after restart")
	}
	clk.advance(time.Minute)
	c.Reconcile(context.Background())
	if v, _ := c.Routes().Pick("id-a"); v.Name != "b" {
		t.Fatal("restart reset the TTL: a still routed past superseded-at + 1h")
	}
	clk.advance(10 * time.Second)
	c.Reconcile(context.Background())
	if len(w.scaled) != 1 {
		t.Fatalf("scaled = %v, want [a]", w.scaled)
	}
}

func TestControllerOnlyLeaderWrites(t *testing.T) {
	src := &fakeSource{synced: true, pointer: "id-b", versions: []Version{
		superseded(ver("a", 0, true), time.Minute), ver("b", time.Minute, true), ver("c", 2*time.Minute, true),
	}}
	clk := &clock{t0.Add(3 * time.Minute)}
	c, w := newTestController(src, clk)
	leader := false
	c.Leader = func() bool { return leader }

	for i := 0; i < 3; i++ {
		c.Reconcile(context.Background())
		clk.advance(time.Hour)
	}
	if w.writes+w.pointerSet != 0 || len(w.retired)+len(w.scaled) != 0 {
		t.Fatalf("follower wrote: annotations %d pointer %d retired %v scaled %v", w.writes, w.pointerSet, w.retired, w.scaled)
	}
	// Followers route from the same plan: c is ready and newest, so it
	// serves even before the leader persists the pointer.
	if serving(c) != "c" {
		t.Fatalf("follower serving %q, want c", serving(c))
	}
	if v, _ := c.Routes().Pick("id-a"); v.Name != "c" {
		t.Fatal("follower routed to a version beyond max-versions")
	}

	leader = true
	c.Reconcile(context.Background())
	if src.pointer != "id-c" {
		t.Fatalf("new leader did not persist the pointer: %q", src.pointer)
	}
	clk.advance(10 * time.Second)
	c.Reconcile(context.Background())
	if len(w.retired) != 1 || w.retired[0] != "a" {
		t.Fatalf("new leader retired %v, want [a]", w.retired)
	}
}

func TestControllerNeverTouchesTheServingVersion(t *testing.T) {
	// The active version b is down for hours; a (previous) serves.
	src := &fakeSource{synced: true, pointer: "id-b", versions: []Version{
		superseded(ver("a", 0, true), time.Minute), ver("b", time.Minute, false),
	}}
	clk := &clock{t0.Add(10 * time.Hour)}
	c, w := newTestController(src, clk)
	for i := 0; i < 3; i++ {
		c.Reconcile(context.Background())
		clk.advance(time.Hour)
	}
	if len(w.scaled)+len(w.retired) != 0 || src.pointer != "id-b" {
		t.Fatalf("scaled %v retired %v pointer %q", w.scaled, w.retired, src.pointer)
	}
	if serving(c) != "a" {
		t.Fatalf("serving %q, want the ready previous version", serving(c))
	}

	src.versions = []Version{ver("b", 0, false)}
	c.Reconcile(context.Background())
	if v, o := c.Routes().Pick(""); v != nil || o != OutcomeUnavailable {
		t.Fatalf("expected 503 routing, got %v/%s", v, o)
	}
}

func TestControllerDryRun(t *testing.T) {
	src := &fakeSource{synced: true, pointer: "id-c", versions: []Version{
		ver("a", 0, true), ver("b", time.Minute, true), ver("c", 2*time.Minute, true),
	}}
	clk := &clock{t0.Add(3 * time.Minute)}
	c, w := newTestController(src, clk)
	c.DryRun = true
	c.Reconcile(context.Background())
	clk.advance(time.Minute)
	c.Reconcile(context.Background())
	if len(w.retired)+len(w.scaled) != 0 {
		t.Fatalf("dry-run retired %v scaled %v", w.retired, w.scaled)
	}
	if v, _ := c.Routes().Pick("id-a"); v.Name != "c" {
		t.Fatal("dry-run must still stop routing to versions past retention")
	}
}

// What an instant rollback does through the deploy tooling's promote,
// seen from the router.
func TestControllerInstantRollback(t *testing.T) {
	t.Run("previous still running: one pass moves the pointer", func(t *testing.T) {
		src := &fakeSource{synced: true, pointer: "id-b", versions: []Version{
			superseded(ver("a", 0, true), time.Minute), ver("b", time.Minute, true),
		}}
		clk := &clock{t0.Add(10 * time.Minute)}
		c, _ := newTestController(src, clk)
		c.Reconcile(context.Background())

		src.find("a").PromotedAt = clk.t // promote
		c.Reconcile(context.Background())
		if src.pointer != "id-a" || serving(c) != "a" {
			t.Fatalf("pointer %q serving %q, want a", src.pointer, serving(c))
		}
		if !src.find("a").SupersededAt.IsZero() {
			t.Fatal("rollback target kept its superseded-at")
		}
		if src.find("b").SupersededAt.IsZero() {
			t.Fatal("rolled-back-from version not marked superseded")
		}
	})

	t.Run("previous scaled to 0: pointer waits until it is ready", func(t *testing.T) {
		src := &fakeSource{synced: true, pointer: "id-b", versions: []Version{
			scaledDown(ver("a", 0, true), time.Hour), ver("b", 30*time.Minute, true),
		}}
		clk := &clock{t0.Add(5 * time.Hour)}
		c, w := newTestController(src, clk)
		c.Reconcile(context.Background())

		// promote: stamp promoted-at, clear the timers, scale back up.
		a := src.find("a")
		a.PromotedAt, a.SupersededAt, a.ExpiredAt, a.ScaledDownAt = clk.t, time.Time{}, time.Time{}, time.Time{}
		for i := 0; i < 3; i++ {
			clk.advance(10 * time.Second)
			c.Reconcile(context.Background())
		}
		if src.pointer != "id-b" || serving(c) != "b" {
			t.Fatalf("pointer moved to a version with no ready endpoints: %q", src.pointer)
		}
		if len(w.scaled)+len(w.retired) != 0 {
			t.Fatalf("rollback target scaled/retired while starting: %v %v", w.scaled, w.retired)
		}
		src.find("a").Ready = true // pods up
		c.Reconcile(context.Background())
		if src.pointer != "id-a" || serving(c) != "a" {
			t.Fatalf("pointer %q serving %q, want a", src.pointer, serving(c))
		}
	})
}

// A security fix: the active version carries revokes-older.
func TestControllerRevokesOlderVersions(t *testing.T) {
	t.Run("unrouted at once, deleted after the drain, nothing kept", func(t *testing.T) {
		src := &fakeSource{synced: true, pointer: "id-b", versions: []Version{
			scaledDown(ver("a", 0, true), time.Hour), ver("b", 2*time.Hour, true),
		}}
		clk := &clock{t0.Add(3 * time.Hour)}
		c, w := newTestController(src, clk)
		ctx := context.Background()
		c.Reconcile(ctx)

		src.versions = append(src.versions, revokes(ver("c", 3*time.Hour, true)))
		clk.advance(time.Minute)
		c.Reconcile(ctx)
		if src.pointer != "id-c" || serving(c) != "c" {
			t.Fatalf("pointer %q serving %q, want c", src.pointer, serving(c))
		}
		for _, id := range []string{"id-a", "id-b"} {
			if v, o := c.Routes().Pick(id); v.Name != "c" || o != OutcomeFallback {
				t.Fatalf("Pick(%s) = %s/%s, want the revoking version", id, v.Name, o)
			}
		}
		if src.find("b").RevokedAt.IsZero() || src.find("a").RevokedAt.IsZero() {
			t.Fatal("revoked-at not recorded")
		}
		if got := c.Metrics.Revoked.Load(); got != 2 {
			t.Fatalf("revoked metric = %d, want 2", got)
		}
		if len(w.retired)+len(w.scaled) != 0 {
			t.Fatalf("deleted before the drain: retired %v scaled %v", w.retired, w.scaled)
		}
		clk.advance(10 * time.Second)
		c.Reconcile(ctx)
		if sort.Strings(w.retired); strings.Join(w.retired, ",") != "a,b" {
			t.Fatalf("retired %v, want a and b", w.retired)
		}
		if len(w.scaled) != 0 {
			t.Fatalf("a revoked version was scaled down (kept) instead of deleted: %v", w.scaled)
		}
		if got := c.Metrics.Revoked.Load(); got != 2 {
			t.Fatalf("revoked metric = %d after the deletes, want 2", got)
		}
	})

	t.Run("not while the revoking version is unready", func(t *testing.T) {
		src := &fakeSource{synced: true, pointer: "id-b", versions: []Version{
			superseded(ver("a", 0, true), time.Minute), revokes(ver("b", time.Minute, false)),
		}}
		clk := &clock{t0.Add(10 * time.Minute)}
		c, w := newTestController(src, clk)
		for i := 0; i < 3; i++ {
			c.Reconcile(context.Background())
			clk.advance(time.Minute)
		}
		if serving(c) != "a" || len(w.retired)+len(w.scaled) != 0 || !src.find("a").RevokedAt.IsZero() {
			t.Fatalf("serving %q retired %v scaled %v: revoked before the fix served", serving(c), w.retired, w.scaled)
		}
		src.find("b").Ready = true
		c.Reconcile(context.Background())
		if v, _ := c.Routes().Pick("id-a"); v.Name != "b" {
			t.Fatal("a still routed once the revoking version is ready")
		}
		clk.advance(10 * time.Second)
		c.Reconcile(context.Background())
		if len(w.retired) != 1 || w.retired[0] != "a" {
			t.Fatalf("retired %v, want [a]", w.retired)
		}
	})

	t.Run("a normal deploy on top: the revoking version gets normal retention", func(t *testing.T) {
		src := &fakeSource{synced: true, pointer: "id-b", versions: []Version{
			revoked(ver("a", 0, true), 2*time.Hour), revokes(ver("b", time.Hour, true)),
		}}
		clk := &clock{t0.Add(2 * time.Hour)}
		c, w := newTestController(src, clk)
		ctx := context.Background()
		c.Reconcile(ctx)

		src.versions = append(src.versions, ver("c", 3*time.Hour, true))
		clk.t = t0.Add(3*time.Hour + time.Minute)
		c.Reconcile(ctx)
		if src.pointer != "id-c" {
			t.Fatalf("pointer = %q, want c", src.pointer)
		}
		if v, o := c.Routes().Pick("id-b"); v.Name != "b" || o != OutcomePinned {
			t.Fatalf("the superseded revoking version must stay pinnable within its TTL, got %s/%s", v.Name, o)
		}
		if v, _ := c.Routes().Pick("id-a"); v.Name != "c" {
			t.Fatal("a revoked version became routable again")
		}
		if len(w.retired) != 1 || w.retired[0] != "a" {
			t.Fatalf("retired %v, want [a]", w.retired)
		}
		clk.advance(time.Hour)
		c.Reconcile(ctx)
		clk.advance(10 * time.Second)
		c.Reconcile(ctx)
		if len(w.scaled) != 1 || w.scaled[0] != "b" || src.find("b") == nil {
			t.Fatalf("scaled %v: b must be scaled to 0 and kept like any previous version", w.scaled)
		}
	})
}
