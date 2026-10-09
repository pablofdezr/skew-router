package main

import (
	"io"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

func ver(name string, promoted time.Duration, ready bool) Version {
	return Version{ID: "id-" + name, Name: name, PromotedAt: t0.Add(promoted), CreatedAt: t0.Add(promoted), Ready: ready}
}

// superseded returns v with the leader's superseded-at recorded.
func superseded(v Version, at time.Duration) Version {
	v.SupersededAt = t0.Add(at)
	return v
}

// scaledDown returns v as the leader leaves it after the TTL: 0 replicas.
func scaledDown(v Version, at time.Duration) Version {
	v.SupersededAt = t0
	v.ExpiredAt = t0.Add(at)
	v.ScaledDownAt = t0.Add(at)
	v.Ready = false
	return v
}

// revokes returns v carrying the security-fix annotation.
func revokes(v Version) Version {
	v.RevokesOlder = true
	return v
}

// revoked returns v as the leader leaves it once a revocation reached it.
func revoked(v Version, at time.Duration) Version {
	v.SupersededAt = t0.Add(at)
	v.ExpiredAt = t0.Add(at)
	v.RevokedAt = t0.Add(at)
	return v
}

func names(m map[string]bool) string {
	var out []string
	for n := range m {
		out = append(out, n)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

func name(v *Version) string {
	if v == nil {
		return ""
	}
	return v.Name
}

func TestPlan(t *testing.T) {
	p := Policy{TTL: time.Hour, MaxVersions: 2}
	tests := []struct {
		name     string
		versions []Version
		pointer  string
		now      time.Duration // since t0
		// expectations
		active, serving, promote string
		older, unrouted          string
		scaleDown, del           string
		revoked                  string
	}{
		{
			name:     "nothing ready and no pointer: touch nothing",
			versions: []Version{ver("a", 0, false), ver("b", time.Minute, false)},
			now:      3 * time.Hour,
		},
		{
			name:     "first ready version becomes the pointer",
			versions: []Version{ver("a", 0, true)},
			now:      time.Minute,
			active:   "a", serving: "a", promote: "a",
		},
		{
			name:     "a new ready version takes the pointer; previous kept",
			versions: []Version{ver("a", 0, true), ver("b", 10*time.Minute, true)},
			pointer:  "id-a",
			now:      11 * time.Minute,
			active:   "b", serving: "b", promote: "b",
			older: "a",
		},
		{
			name:     "previous stays routable within TTL of superseded-at",
			versions: []Version{superseded(ver("a", 0, true), 10*time.Minute), ver("b", 10*time.Minute, true)},
			pointer:  "id-b",
			now:      69 * time.Minute,
			active:   "b", serving: "b",
			older: "a",
		},
		{
			name:     "previous past TTL: unrouted and scaled to 0, not deleted",
			versions: []Version{superseded(ver("a", 0, true), 10*time.Minute), ver("b", 10*time.Minute, true)},
			pointer:  "id-b",
			now:      70 * time.Minute,
			active:   "b", serving: "b",
			older: "a", unrouted: "a", scaleDown: "a",
		},
		{
			name:     "TTL waits for the leader to record superseded-at",
			versions: []Version{ver("a", 0, true), ver("b", 10*time.Minute, true)},
			pointer:  "id-b",
			now:      5 * time.Hour,
			active:   "b", serving: "b",
			older: "a",
		},
		{
			name:     "a scaled-down previous is kept and stays unrouted",
			versions: []Version{scaledDown(ver("a", 0, true), time.Hour), ver("b", 10*time.Minute, true)},
			pointer:  "id-b",
			now:      30 * time.Hour,
			active:   "b", serving: "b",
			older: "a", unrouted: "a",
		},
		{
			name: "max 2: beyond the previous one, versions are deleted",
			versions: []Version{
				scaledDown(ver("a", 0, true), time.Hour), ver("b", 2*time.Hour, true), ver("c", 3*time.Hour, true),
			},
			pointer: "id-c",
			now:     3*time.Hour + time.Minute,
			active:  "c", serving: "c",
			older: "a,b", unrouted: "a", del: "a",
		},
		{
			name: "rollout in progress: newer unready version is left alone",
			versions: []Version{
				ver("a", 0, true), ver("b", 10*time.Minute, true), ver("c", 20*time.Minute, false),
			},
			pointer: "id-b",
			now:     30 * time.Minute,
			active:  "b", serving: "b",
			older: "a",
		},
		{
			name: "active went unready: pointer stays, the newest ready serves and is protected",
			versions: []Version{
				superseded(ver("a", 0, true), time.Minute), ver("b", time.Minute, false),
			},
			pointer: "id-b",
			now:     5 * time.Hour,
			active:  "b", serving: "a",
			older: "a",
		},
		{
			name: "instant rollback to a running previous: promote moves the pointer",
			versions: []Version{
				ver("a", 30*time.Minute, true), // re-promoted after b
				superseded(ver("b", 10*time.Minute, true), 10*time.Minute),
			},
			pointer: "id-b",
			now:     30 * time.Minute,
			active:  "a", serving: "a", promote: "a",
			older: "b",
		},
		{
			name: "rollback target still scaling up: pointer waits, target untouched",
			versions: []Version{
				func() Version {
					v := scaledDown(ver("a", 0, false), time.Hour)
					v.PromotedAt = t0.Add(5 * time.Hour)
					return v
				}(),
				ver("b", 2*time.Hour, true),
			},
			pointer: "id-b",
			now:     5*time.Hour + time.Second,
			active:  "b", serving: "b",
		},
		{
			name: "revoking active: every older version unrouted and deleted at once",
			versions: []Version{
				scaledDown(ver("a", 0, true), time.Hour),
				superseded(ver("b", 2*time.Hour, true), 3*time.Hour),
				revokes(ver("c", 3*time.Hour, true)),
			},
			pointer: "id-c",
			now:     3*time.Hour + time.Minute, // b well within its TTL
			active:  "c", serving: "c",
			older: "a,b", unrouted: "a,b", del: "a,b", revoked: "a,b",
		},
		{
			name: "revoking version becomes ready: revoked in the same plan that promotes it",
			versions: []Version{
				ver("a", 0, true), revokes(ver("b", 10*time.Minute, true)),
			},
			pointer: "id-a",
			now:     11 * time.Minute,
			active:  "b", serving: "b", promote: "b",
			older: "a", unrouted: "a", del: "a", revoked: "a",
		},
		{
			name: "revoking version still rolling out: nothing revoked yet",
			versions: []Version{
				ver("a", 0, true), ver("b", 10*time.Minute, true), revokes(ver("c", 20*time.Minute, false)),
			},
			pointer: "id-b",
			now:     30 * time.Minute,
			active:  "b", serving: "b",
			older: "a",
		},
		{
			name: "revoking active not ready: the older version keeps the site up, not revoked",
			versions: []Version{
				superseded(ver("a", 0, true), time.Minute), revokes(ver("b", time.Minute, false)),
			},
			pointer: "id-b",
			now:     5 * time.Hour,
			active:  "b", serving: "a",
			older: "a",
		},
		{
			name: "revoked stays revoked after the revoking version is superseded",
			versions: []Version{
				revoked(ver("a", 0, true), 2*time.Hour),
				superseded(revokes(ver("b", time.Hour, true)), 3*time.Hour),
				ver("c", 3*time.Hour, true),
			},
			pointer: "id-c",
			now:     3*time.Hour + time.Minute,
			active:  "c", serving: "c",
			older: "a,b", unrouted: "a", del: "a", revoked: "a",
		},
		{
			name: "a revoked version never serves as the fallback",
			versions: []Version{
				revoked(ver("a", 0, true), time.Hour), revokes(ver("b", time.Minute, false)),
			},
			pointer: "id-b",
			now:     2 * time.Hour,
			active:  "b",
			older:   "a", unrouted: "a", del: "a", revoked: "a",
		},
		{
			name:     "pointer to a deleted version: newest ready takes over",
			versions: []Version{ver("a", 0, true)},
			pointer:  "id-gone",
			now:      time.Minute,
			active:   "a", serving: "a", promote: "a",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Plan(tt.versions, tt.pointer, t0.Add(tt.now), p)
			for _, c := range []struct{ what, got, want string }{
				{"active", name(d.Active), tt.active},
				{"serving", name(d.Serving), tt.serving},
				{"promote", name(d.Promote), tt.promote},
				{"older", names(d.Older), tt.older},
				{"unrouted", names(d.Unrouted), tt.unrouted},
				{"scaleDown", names(d.ScaleDown), tt.scaleDown},
				{"delete", names(d.Delete), tt.del},
				{"revoked", names(d.Revoked), tt.revoked},
			} {
				if c.got != c.want {
					t.Errorf("%s = %q, want %q", c.what, c.got, c.want)
				}
			}
		})
	}
}

func TestPlanLegacyFallsBackToCreationTime(t *testing.T) {
	legacy := Version{ID: "legacy", Name: "web", CreatedAt: t0.Add(-24 * time.Hour), Ready: true}
	next := ver("web-abc", 0, true)
	d := Plan([]Version{legacy, next}, "legacy", t0.Add(time.Minute), Policy{TTL: time.Hour, MaxVersions: 2})
	if name(d.Promote) != "web-abc" || name(d.Active) != "web-abc" {
		t.Fatalf("promote/active = %s/%s, want web-abc", name(d.Promote), name(d.Active))
	}
	if len(d.Unrouted) != 0 {
		t.Fatalf("legacy unrouted too early: %v", d.Unrouted)
	}
}

func TestRoutesPick(t *testing.T) {
	a, b := ver("a", 0, true), ver("b", time.Minute, true)
	r := &Routes{Serving: &b, Active: &b, byID: map[string]*Version{a.ID: &a, b.ID: &b}}
	cases := []struct {
		id      string
		want    string
		outcome Outcome
	}{
		{"", "b", OutcomeUnpinned},
		{b.ID, "b", OutcomeLatest},
		{a.ID, "a", OutcomePinned},
		{"gone", "b", OutcomeFallback},
	}
	for _, c := range cases {
		v, o := r.Pick(c.id)
		if v.Name != c.want || o != c.outcome {
			t.Errorf("Pick(%q) = %s/%s, want %s/%s", c.id, v.Name, o, c.want, c.outcome)
		}
	}
	// A revoked version is not in the table: its id is routed to the
	// active version, never to its old upstream.
	d := Plan([]Version{a, revokes(b)}, b.ID, t0.Add(2*time.Minute), Policy{TTL: time.Hour, MaxVersions: 2})
	c := &Controller{Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: &Metrics{}}
	c.publish([]Version{a, revokes(b)}, d)
	if v, o := c.Routes().Pick(a.ID); v.Name != "b" || o != OutcomeFallback {
		t.Errorf("Pick(revoked id) = %s/%s, want b/fallback", v.Name, o)
	}
	if v, o := (&Routes{}).Pick("x"); v != nil || o != OutcomeUnavailable {
		t.Errorf("empty table: got %v/%s", v, o)
	}
}

func TestValidID(t *testing.T) {
	for id, want := range map[string]bool{
		"0123456789abcdef0123456789abcdef01234567": true,
		"legacy":                true,
		"a.b-c_d":               true,
		"":                      false,
		"-x":                    false,
		"x-":                    false,
		"has space":             false,
		"../etc":                false,
		strings.Repeat("a", 64): false,
	} {
		if got := validID(id); got != want {
			t.Errorf("validID(%q) = %v, want %v", id, got, want)
		}
	}
}
