package main

import (
	"net/url"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// Cluster state the router reads and writes. promoted-at is written by your
// deploy tooling (deploy, rollback; see examples/promote.sh); the pointer
// ConfigMap and the other annotations by the leading router replica, so
// routing and the retention timers survive router restarts and leader
// changes.
const (
	PointerKeyID   = "active"
	PointerKeyName = "activeName"
	PointerKeyAt   = "updatedAt"

	// DefaultAnnotationPrefix is the prefix of every annotation below,
	// changed with -annotation-prefix.
	DefaultAnnotationPrefix = "skew-router.dev"
)

// PointerConfigMap holds the active version: the one unpinned traffic goes
// to. Only the leader writes it, and only to a ready version. Changed with
// -pointer-configmap.
var PointerConfigMap = "web-routing"

var (
	// When the version was made the intended latest (a request to move
	// the pointer: deploy or rollback).
	AnnPromotedAt string
	// When the leader first saw the version older than the active one.
	// The TTL counts from here.
	AnnSupersededAt string
	// When the leader stopped routing to the version (past retention).
	// The scale-down or delete happens a drain period later.
	AnnExpiredAt string
	// When the leader scaled the version to 0 (kept for instant rollback).
	AnnScaledDownAt string
	// On the Deployment: its replica count before the scale-down, so a
	// rollback restores it.
	AnnScaledFrom string

	// "true" on a version that fixes a security bug: once it is active
	// and ready, every older version stops being routable at once and is
	// deleted (no TTL, nothing kept for rollback). Written by the deploy
	// tooling with the promotion (examples/promote.sh --revoke-older); a
	// later promotion without it resets it to "false".
	AnnRevokesOlder string
	// When the leader revoked the version (older than a revoking active
	// one). Sticky: a revoked version is never routed or served again,
	// even after the revoking version is itself superseded, until a
	// promotion clears it.
	AnnRevokedAt string
)

func init() { SetAnnotationPrefix(DefaultAnnotationPrefix) }

// SetAnnotationPrefix sets the prefix of every annotation the router reads
// and writes (e.g. "example.com" → "example.com/promoted-at").
func SetAnnotationPrefix(p string) {
	AnnPromotedAt = p + "/promoted-at"
	AnnSupersededAt = p + "/superseded-at"
	AnnExpiredAt = p + "/expired-at"
	AnnScaledDownAt = p + "/scaled-down-at"
	AnnScaledFrom = p + "/scaled-from"
	AnnRevokesOlder = p + "/revokes-older"
	AnnRevokedAt = p + "/revoked-at"
}

// Version is one deployed web build: a Service `web-<id>` (plus the
// Deployment of the same name) labelled with the build's deployment id.
type Version struct {
	ID           string    // deployment id baked into the build (label value)
	Name         string    // Service name == Deployment name
	UID          types.UID // Service UID, for write preconditions
	RV           string    // Service resourceVersion, for write preconditions
	Upstream     *url.URL  // the Service's ClusterIP:port
	PromotedAt   time.Time // AnnPromotedAt
	CreatedAt    time.Time // Service creation time, fallback for PromotedAt
	SupersededAt time.Time // AnnSupersededAt (zero = not recorded)
	ExpiredAt    time.Time // AnnExpiredAt (zero = not recorded)
	ScaledDownAt time.Time // AnnScaledDownAt (zero = running)
	RevokedAt    time.Time // AnnRevokedAt (zero = not revoked)
	RevokesOlder bool      // AnnRevokesOlder == "true"
	Ready        bool      // has at least one ready endpoint
}

// order is the instant a version was last promoted. A rollback re-stamps
// PromotedAt, so the rollback target sorts newest.
func (v Version) order() time.Time {
	if !v.PromotedAt.IsZero() {
		return v.PromotedAt
	}
	return v.CreatedAt
}

func newer(a, b Version) bool {
	if !a.order().Equal(b.order()) {
		return a.order().After(b.order())
	}
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.After(b.CreatedAt)
	}
	return a.Name > b.Name
}

// Policy is the retention the operator chose. An older version stays
// routable for TTL after it stopped being active, then is scaled to 0 but
// kept (instant rollback); MaxVersions bounds how many versions exist at
// all, active included — the oldest beyond it are deleted.
type Policy struct {
	TTL         time.Duration
	MaxVersions int
}

// Decision is the outcome of Plan for one point in time.
type Decision struct {
	// Active is the pointer's version (after applying Promote).
	Active *Version
	// Serving is where unpinned traffic goes: Active when it is ready,
	// else the newest ready version (the pointer is never moved to keep
	// the site up; the active version is expected back).
	Serving *Version
	// Promote, when set, is where the leader must move the pointer: the
	// newest-promoted ready version, when it is newer than the active one.
	Promote *Version
	// Older holds the names of versions older than Active.
	Older map[string]bool
	// Unrouted: older versions past retention; nobody routes to them.
	Unrouted map[string]bool
	// ScaleDown: unrouted versions to scale to 0 and keep (after drain).
	ScaleDown map[string]bool
	// Delete: unrouted versions beyond MaxVersions, and revoked ones
	// (after drain).
	Delete map[string]bool
	// Revoking: Active carries AnnRevokesOlder and is the one serving, so
	// every version older than it is revoked.
	Revoking bool
	// Revoked: older versions unrouted and deleted because of a
	// revocation (this one, or one recorded earlier as AnnRevokedAt).
	Revoked map[string]bool
}

// Plan derives routing and retention from cluster state (versions + the
// pointer) and the clock. It is a pure function, so every router replica
// computes the same table on its own; the leader only persists the result.
//
//   - The pointer moves only forward, to the newest-promoted version with
//     ready endpoints. A version that is unready is never made active;
//     an active version that goes unready keeps the pointer (it is served
//     around, not replaced).
//   - Versions newer than the active one (a rollout or a rollback target
//     still starting) are left alone: never deleted, never scaled down.
//   - Versions older than the active one, newest first: the first
//     MaxVersions-1 are kept — routable until TTL after SupersededAt, then
//     scaled to 0 — and the rest are deleted.
//   - The version serving unpinned traffic is never touched.
//   - Revocation (a security fix): when the active version revokes older
//     ones and is ready, every version older than it is unrouted and
//     deleted at once, regardless of TTL and MaxVersions. While the active
//     version is not ready (so an older one serves), nothing is revoked
//     yet: keeping the site up wins until the fixed version serves.
//     Revoked versions stay revoked (AnnRevokedAt): never routed, never
//     the fallback for unpinned traffic, even once the revoking version is
//     superseded — a 503 beats serving a build with a known hole.
func Plan(versions []Version, pointer string, now time.Time, p Policy) Decision {
	d := Decision{
		Older: map[string]bool{}, Unrouted: map[string]bool{},
		ScaleDown: map[string]bool{}, Delete: map[string]bool{},
		Revoked: map[string]bool{},
	}
	sorted := append([]Version(nil), versions...)
	sort.SliceStable(sorted, func(i, j int) bool { return newer(sorted[i], sorted[j]) })

	var active, newestReady *Version
	for i := range sorted {
		v := &sorted[i]
		if pointer != "" && v.ID == pointer && active == nil {
			active = v
		}
		if v.Ready && v.RevokedAt.IsZero() && newestReady == nil {
			newestReady = v
		}
	}
	if newestReady != nil && (active == nil || newer(*newestReady, *active)) {
		d.Promote = newestReady
		active = newestReady
	}
	if active == nil {
		return d // nothing ready and no pointer: touch nothing
	}
	d.Active = active
	d.Serving = active
	if !active.Ready {
		d.Serving = newestReady // may be nil: 503
	}
	d.Revoking = active.RevokesOlder && active.Ready

	kept := 0
	for _, v := range sorted {
		if !newer(*active, v) {
			continue // the active version, or newer than it
		}
		d.Older[v.Name] = true
		if d.Revoking || !v.RevokedAt.IsZero() {
			d.Revoked[v.Name] = true
			d.Unrouted[v.Name] = true
			d.Delete[v.Name] = true
			continue // not counted: nothing revoked is kept
		}
		if d.Serving != nil && v.Name == d.Serving.Name {
			kept++
			continue // keeping the site up beats retention
		}
		switch {
		case kept >= p.MaxVersions-1:
			d.Unrouted[v.Name] = true
			d.Delete[v.Name] = true
		case !v.ScaledDownAt.IsZero():
			kept++
			d.Unrouted[v.Name] = true // kept at 0 replicas
		case !v.SupersededAt.IsZero() && now.Sub(v.SupersededAt) >= p.TTL:
			kept++
			d.Unrouted[v.Name] = true
			d.ScaleDown[v.Name] = true
		default:
			kept++
		}
	}
	return d
}
