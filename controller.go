package main

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// Routes is the immutable routing table the proxy reads on every request.
type Routes struct {
	Serving *Version            // unpinned traffic (normally the active version)
	Active  *Version            // the pointer's version, for /versions
	byID    map[string]*Version // routable versions: ready and not unrouted
}

// Pick returns the version a request carrying deployment id `id` goes to,
// and how it was chosen.
func (r *Routes) Pick(id string) (*Version, Outcome) {
	if r == nil || r.Serving == nil {
		return nil, OutcomeUnavailable
	}
	switch {
	case id == "":
		return r.Serving, OutcomeUnpinned
	case id == r.Serving.ID:
		return r.Serving, OutcomeLatest
	}
	if v, ok := r.byID[id]; ok {
		return v, OutcomePinned
	}
	return r.Serving, OutcomeFallback
}

// VersionSource is what the controller observes (Discovery in production).
type VersionSource interface {
	Synced() bool
	Versions() []Version
	Pointer() string // active deployment id from the pointer ConfigMap
}

// VersionWriter changes the cluster (KubeVersions in production). Every
// operation must be idempotent: a new leader may repeat the last one.
type VersionWriter interface {
	// SetPointer makes v the active version.
	SetPointer(ctx context.Context, v Version, at time.Time) error
	// Annotate sets (non-empty value) or removes (empty value) annotations
	// on the version's Service.
	Annotate(ctx context.Context, v Version, ann map[string]string) error
	// ScaleDown scales the version's Deployment to 0, keeping the objects.
	ScaleDown(ctx context.Context, v Version) error
	// Retire deletes the version's Deployment and Service.
	Retire(ctx context.Context, v Version) error
}

// Controller turns cluster state into the routing table and, on the leader
// only, persists the pointer and the retention bookkeeping.
//
// Every replica routes, from cluster state alone (Plan is deterministic),
// so all of them converge on the same table within one watch event. The
// leader moves the pointer, stamps superseded-at when a version stops
// being active (the TTL counts from it) and expired-at when it is past
// retention — every replica stops routing to it at that moment — and a
// drain period later scales it to 0 (the previous version, kept for
// instant rollback) or deletes it (anything older). A revoking active
// version (security fix, AnnRevokesOlder) skips the TTL: every older
// version is unrouted at once, stamped revoked-at, and deleted after the
// drain.
type Controller struct {
	Source  VersionSource
	Writer  VersionWriter
	Policy  Policy
	Drain   time.Duration
	DryRun  bool
	Leader  func() bool // nil = always leader
	Now     func() time.Time
	Log     *slog.Logger
	Metrics *Metrics

	routes                              atomic.Pointer[Routes]
	lastActive, lastServing, lastRevoke string
}

// Routes returns the current routing table (nil until the first sync).
func (c *Controller) Routes() *Routes { return c.routes.Load() }

// Run reconciles on every trigger and every interval until ctx is done.
func (c *Controller) Run(ctx context.Context, trigger <-chan struct{}, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		c.Reconcile(ctx)
		select {
		case <-ctx.Done():
			return
		case <-trigger:
		case <-t.C:
		}
	}
}

// Reconcile performs one pass. Not safe for concurrent use; Run calls it
// from a single goroutine.
func (c *Controller) Reconcile(ctx context.Context) {
	// Before the caches are synced the observed set may be partial: an
	// incomplete list would make a live version look gone, or an old one
	// look like the active one. Keep whatever table we had (nil at boot).
	if !c.Source.Synced() {
		return
	}
	now := c.Now()
	versions := c.Source.Versions()
	pointer := c.Source.Pointer()
	d := Plan(versions, pointer, now, c.Policy)
	c.publish(versions, d)
	if c.Leader == nil || c.Leader() {
		c.maintain(ctx, versions, pointer, d, now)
	}
}

// maintain persists the pointer and the retention timers, and scales down
// or deletes expired versions. Every write is derived from the current
// state, so a new leader picks up where the previous one stopped.
func (c *Controller) maintain(ctx context.Context, versions []Version, pointer string, d Decision, now time.Time) {
	if d.Promote != nil && d.Promote.ID != pointer {
		if err := c.write(ctx, func(ctx context.Context) error { return c.Writer.SetPointer(ctx, *d.Promote, now) }); err != nil {
			c.Log.Error("move pointer failed; retrying next pass", "to", d.Promote.Name, "err", err)
		} else {
			c.Log.Info("pointer moved", "from", pointer, "to", d.Promote.ID, "version", d.Promote.Name)
		}
	}

	stamp := now.UTC().Format(time.RFC3339)
	for _, v := range versions {
		ann := map[string]string{}
		revoked := false
		switch {
		case d.Active != nil && v.Name == d.Active.Name:
			// Active again (rollback): its old timers no longer apply.
			for _, k := range []string{AnnSupersededAt, AnnExpiredAt, AnnScaledDownAt} {
				if v.annotated(k) {
					ann[k] = ""
				}
			}
		case d.Unrouted[v.Name]:
			if v.SupersededAt.IsZero() {
				ann[AnnSupersededAt] = stamp
			}
			if d.Revoked[v.Name] && v.RevokedAt.IsZero() {
				ann[AnnRevokedAt] = stamp
				revoked = true
			}
			switch {
			case v.ExpiredAt.IsZero():
				ann[AnnExpiredAt] = stamp
				if !d.Revoked[v.Name] {
					c.Log.Info("version past retention; no longer routed", "version", v.Name, "drain", c.Drain.String())
				}
			case now.Sub(v.ExpiredAt) < c.Drain, revoked:
				// Draining; or just revoked: record that first, delete on
				// the next pass.
			case d.Delete[v.Name]:
				c.act(ctx, "delete", v, c.Writer.Retire)
				continue
			case d.ScaleDown[v.Name]:
				if c.act(ctx, "scale to 0", v, c.Writer.ScaleDown) {
					ann[AnnScaledDownAt] = stamp
				}
			}
		case d.Older[v.Name]:
			if v.SupersededAt.IsZero() {
				ann[AnnSupersededAt] = stamp
				c.Log.Info("version superseded", "version", v.Name, "ttl", c.Policy.TTL.String())
			}
			if !v.ExpiredAt.IsZero() {
				ann[AnnExpiredAt] = "" // back within retention (order changed)
			}
		}
		if len(ann) == 0 {
			continue
		}
		if err := c.write(ctx, func(ctx context.Context) error { return c.Writer.Annotate(ctx, v, ann) }); err != nil {
			c.Log.Error("annotate version failed; retrying next pass", "version", v.Name, "err", err)
			continue
		}
		if revoked {
			c.Metrics.Revoked.Add(1)
			c.Log.Warn("version revoked by a security fix; no longer routed, deleting after the drain",
				"version", v.Name, "id", v.ID, "revoked_by", d.Active.Name, "drain", c.Drain.String())
		}
	}
}

// act runs a destructive action unless in dry-run; reports success.
func (c *Controller) act(ctx context.Context, what string, v Version, fn func(context.Context, Version) error) bool {
	if c.DryRun {
		c.Log.Info("dry-run: would "+what, "version", v.Name, "id", v.ID)
		return false
	}
	if err := c.write(ctx, func(ctx context.Context) error { return fn(ctx, v) }); err != nil {
		c.Log.Error(what+" failed; retrying next pass", "version", v.Name, "err", err)
		return false
	}
	c.Metrics.Retired.Add(1)
	c.Log.Info("version "+what, "version", v.Name, "id", v.ID)
	return true
}

func (c *Controller) write(ctx context.Context, fn func(context.Context) error) error {
	wctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return fn(wctx)
}

func (v Version) annotated(key string) bool {
	switch key {
	case AnnSupersededAt:
		return !v.SupersededAt.IsZero()
	case AnnExpiredAt:
		return !v.ExpiredAt.IsZero()
	case AnnScaledDownAt:
		return !v.ScaledDownAt.IsZero()
	}
	return false
}

func (c *Controller) publish(versions []Version, d Decision) {
	r := &Routes{Serving: d.Serving, Active: d.Active, byID: map[string]*Version{}}
	// Oldest first, so when two objects claim one id the newest wins.
	sorted := append([]Version(nil), versions...)
	sort.SliceStable(sorted, func(i, j int) bool { return newer(sorted[j], sorted[i]) })
	for i := range sorted {
		v := sorted[i]
		if v.Ready && !d.Unrouted[v.Name] && v.RevokedAt.IsZero() {
			r.byID[v.ID] = &v
		}
	}
	c.routes.Store(r)
	c.Metrics.SetRoutable(len(r.byID))

	active, serving := "", ""
	if d.Active != nil {
		active = d.Active.Name
	}
	if d.Serving != nil {
		serving = d.Serving.Name
	}
	if active != c.lastActive {
		c.Log.Info("active version changed", "from", c.lastActive, "to", active)
		c.lastActive = active
		c.Metrics.SetActive(active)
	}
	if serving != c.lastServing {
		switch {
		case serving == "":
			c.Log.Error("no ready web version; serving 503", "active", active)
		case serving != active:
			c.Log.Warn("active version has no ready endpoints; serving the newest ready one",
				"active", active, "serving", serving)
		default:
			c.Log.Info("serving the active version", "version", serving)
		}
		c.lastServing = serving
	}

	revoke := ""
	switch {
	case d.Revoking:
		revoke = "revoking:" + active
	case d.Active != nil && d.Active.RevokesOlder:
		revoke = "pending:" + active
	}
	if revoke != c.lastRevoke {
		switch {
		case d.Revoking:
			c.Log.Warn("older versions revoked: no longer routed; pinned tabs go to the active version",
				"active", active, "revoked", strings.Join(slices.Sorted(maps.Keys(d.Revoked)), ","))
		case revoke != "":
			c.Log.Warn("revocation pending: the revoking active version has no ready endpoints; older versions keep serving until it does",
				"active", active, "serving", serving)
		}
		c.lastRevoke = revoke
	}
}
