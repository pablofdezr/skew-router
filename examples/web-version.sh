#!/usr/bin/env bash
# web-version.sh — create, promote and retire web versions for skew-router.
#
#   web-version.sh rollout <id> <image> [timeout_s]  new version web-<id> from <image>; waits until the
#                                                    router makes it active, deletes it if it never does
#   web-version.sh promote <id|name|previous>        make an existing version active again (instant
#                                                    rollback; scales it back up if the router scaled it to 0)
#   web-version.sh revoke-older                      the active version becomes a security fix: every
#                                                    older version is unrouted and deleted
#   web-version.sh retire <id|name> [--force]        delete a version
#   web-version.sh active | previous | list
#
# <id> must be the build's deployment id: the value baked in as Next.js `deploymentId`
# (NEXT_DEPLOYMENT_ID at build time), e.g. the commit SHA. It must be a valid label value.
#
# Environment: NS (namespace, default web), PREFIX (annotation/label prefix, default
# skew-router.dev, must match the router's -annotation-prefix), WEB_MIN_REPLICAS (default 2),
# WEB_REVOKE_OLDER=1 (this promotion is a security fix), POINTER_TIMEOUT, REVOKE_TIMEOUT.
# Needs kubectl and jq.
set -euo pipefail

NS=${NS:-web}
PREFIX=${PREFIX:-skew-router.dev}
VERSION_LABEL=$PREFIX/version
PROMOTED=$PREFIX/promoted-at
REVOKES=$PREFIX/revokes-older
# Timers the router writes; a promotion clears them (they described the version as an old one).
ROUTER_TIMERS=("$PREFIX/superseded-at" "$PREFIX/expired-at" "$PREFIX/scaled-down-at" "$PREFIX/revoked-at")
POINTER=${POINTER:-web-routing}
POINTER_TIMEOUT=${POINTER_TIMEOUT:-60}
REVOKE_TIMEOUT=${REVOKE_TIMEOUT:-120}
TEMPLATE=${WEB_VERSION_TEMPLATE:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/web-version.yaml}
ESC=${PREFIX//./\\.} # jsonpath needs the dots in a key escaped

log() { echo "[web-version] $*" >&2; }
die() { log "$*"; exit 2; }
k() { kubectl -n "$NS" "$@"; }
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
name_of() { case "$1" in web-*) echo "$1" ;; *) echo "web-$1" ;; esac; }
label_of() { k get deployment "$1" -o jsonpath="{.metadata.labels.$ESC/version}" 2>/dev/null || true; }
revokes_of() { k get service "$1" -o jsonpath="{.metadata.annotations.$ESC/revokes-older}" 2>/dev/null || true; }

pointer() { k get configmap "$POINTER" -o jsonpath='{.data.active}' 2>/dev/null || true; }
router_running() { [ "$(k get deployment skew-router -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)" -ge 1 ] 2>/dev/null; }

# One object per version Deployment, newest promotion first.
versions_json() {
  k get deployments -l "$VERSION_LABEL" -o json | jq --arg l "$VERSION_LABEL" --arg a "$PROMOTED" --arg p "$(pointer)" '
    [.items[] | {
      name: .metadata.name,
      id: .metadata.labels[$l],
      at: (.metadata.annotations[$a] // .metadata.creationTimestamp),
      ready: (.status.readyReplicas // 0),
      replicas: (.spec.replicas // 0),
      active: (.metadata.labels[$l] == $p),
      image: .spec.template.spec.containers[0].image
    }] | sort_by(.at) | reverse'
}

# The pointer's version; before the router ever set it: the newest ready version.
active() { versions_json | jq -r '(map(select(.active)) + map(select(.ready > 0)) + .)[0].name // empty'; }

# The newest version promoted before the active one.
previous() {
  local cur; cur=$(active)
  versions_json | jq -r --arg c "$cur" '(map(.name) | index($c)) as $i | if $i == null then empty else (.[$i + 1].name // empty) end'
}

older_than() {
  versions_json | jq -r --arg n "$1" '(map(.name) | index($n)) as $i | if $i == null then empty else .[$i + 1:][].name end'
}

wait_ready() { k rollout status deployment/"$1" --timeout="${2}s" >&2; }
revoke_flag() { if [ "${WEB_REVOKE_OLDER:-0}" = 1 ]; then echo true; else echo false; fi; }

wait_pointer() {
  local id=$1 deadline=$(( $(date +%s) + POINTER_TIMEOUT ))
  if ! router_running; then log "no skew-router running — not waiting for the pointer"; return 0; fi
  until [ "$(pointer)" = "$id" ]; do
    [ "$(date +%s)" -lt "$deadline" ] || { log "the router did not make $id active within ${POINTER_TIMEOUT}s (pointer: $(pointer))"; return 1; }
    sleep 1
  done
  log "pointer → $id"
}

# Until no version older than <name> exists (the router unroutes them the moment <name> is
# active and ready, and deletes them after its drain).
wait_revoked() {
  local name=$1 deadline=$(( $(date +%s) + REVOKE_TIMEOUT )) left
  router_running || { log "no skew-router running — nothing revokes older versions"; return 1; }
  while left=$(older_than "$name") && [ -n "$left" ]; do
    [ "$(date +%s)" -lt "$deadline" ] || { log "older versions still present after ${REVOKE_TIMEOUT}s: $(tr '\n' ' ' <<<"$left")"; return 1; }
    sleep 1
  done
  log "older versions revoked: nothing older than $name is routed or running"
}

# The request the router acts on: promoted-at=now and revokes-older in the same write (so the
# router never sees one without the other), router timers cleared.
stamp_promoted() {
  local timers=() t
  for t in "${ROUTER_TIMERS[@]}"; do timers+=("$t-"); done
  k annotate deployment/"$1" service/"$1" "$PROMOTED=$(now)" "$REVOKES=${2:-$(revoke_flag)}" "${timers[@]}" --overwrite >/dev/null
}

promote() {
  local target=$1 timeout=${2:-180} name id current replicas revokes=""
  if [ "$target" = previous ]; then
    current=$(active)
    if [ -n "$current" ] && [ "$(revokes_of "$current")" = true ]; then
      log "$current is a security fix: every older version was revoked — nothing to roll back to"
      exit 3
    fi
    target=$(previous)
    [ -n "$target" ] || die "no previous version to roll back to"
  fi
  name=$(name_of "$target")
  id=$(label_of "$name")
  [ -n "$id" ] || die "$name is not a web version"
  current=$(active)
  [ -z "$current" ] || revokes=$(revokes_of "$current")

  # Promote first, scale second: once promoted it is newer than the active version, which the
  # router never scales down, so it can't race the scale-up. It becomes active only when ready.
  stamp_promoted "$name"
  replicas=$(k get deployment "$name" -o jsonpath='{.spec.replicas}')
  if [ "${replicas:-0}" = 0 ]; then
    replicas=$(k get deployment "$name" -o jsonpath="{.metadata.annotations.$ESC/scaled-from}")
    log "$name was scaled to 0 by the router — scaling back to ${replicas:-${WEB_MIN_REPLICAS:-2}}"
    k scale deployment/"$name" --replicas="${replicas:-${WEB_MIN_REPLICAS:-2}}" >/dev/null
  fi
  if wait_ready "$name" "$timeout" && wait_pointer "$id"; then
    [ "$(revoke_flag)" != true ] || wait_revoked "$name" || log "WARN: older versions unrouted but not all deleted yet"
    echo "$name"; return 0
  fi
  # Leave things as they were: re-stamp the version that was active so the failed target sorts older.
  if [ -n "$current" ] && [ "$current" != "$name" ]; then
    log "promotion of $name failed — keeping $current active"
    stamp_promoted "$current" "${revokes:-false}"
  fi
  return 1
}

rollout() {
  local id=$1 image=$2 timeout=${3:-300} name current replicas cur
  name=$(name_of "$id")
  if k get deployment "$name" >/dev/null 2>&1; then
    log "$name exists — promoting it instead"
    promote "$name" "$timeout"; return
  fi
  [ -f "$TEMPLATE" ] || die "template not found: $TEMPLATE"
  current=$(active)
  # At least WEB_MIN_REPLICAS: after its hour the previous version is scaled to 0, so the active
  # one must survive a pod dying on its own.
  replicas=${WEB_MIN_REPLICAS:-2}
  if [ -n "$current" ]; then
    cur=$(k get deployment "$current" -o jsonpath='{.spec.replicas}')
    [ "${cur:-0}" -gt "$replicas" ] && replicas=$cur
  fi
  log "creating $name from $image (replicas=$replicas, current active: ${current:-none})"
  sed -e "s|__NAME__|$name|g" -e "s|__VERSION__|$id|g" -e "s|__IMAGE__|$image|g" \
      -e "s|__NAMESPACE__|$NS|g" -e "s|__PREFIX__|$PREFIX|g" -e "s|__PROMOTED_AT__|$(now)|g" \
      -e "s|__REPLICAS__|$replicas|g" -e "s|__REVOKES_OLDER__|$(revoke_flag)|g" \
      "$TEMPLATE" | k apply -f - >/dev/null
  if wait_ready "$name" "$timeout" && wait_pointer "$id"; then
    [ "$(revoke_flag)" != true ] || wait_revoked "$name" || log "WARN: older versions unrouted but not all deleted yet"
    log "$name active — tabs on ${current:-the old version} stay there until it retires"
    echo "$name"; return 0
  fi
  if [ "$(pointer)" = "$id" ]; then log "$name is active although the wait failed — leaving it"; return 1; fi
  log "$name did not become active — deleting it; ${current:-nothing} keeps serving"
  k delete deployment/"$name" service/"$name" --ignore-not-found --wait=false >&2 || true
  return 1
}

revoke_older() {
  local name id
  name=$(active)
  [ -n "$name" ] || die "no web version deployed"
  id=$(label_of "$name")
  if router_running && [ "$(pointer)" != "$id" ]; then die "$name is not the router's active version yet (pointer: $(pointer))"; fi
  k annotate deployment/"$name" service/"$name" "$REVOKES=true" --overwrite >/dev/null
  log "$name marked $REVOKES=true; revoking: $(older_than "$name" | tr '\n' ' ')"
  wait_revoked "$name"
}

retire() {
  local name others
  name=$(name_of "$1")
  [ -n "$(label_of "$name")" ] || die "$name is not a web version — refusing"
  others=$(versions_json | jq --arg n "$name" '[.[] | select(.name != $n and .ready > 0)] | length')
  if [ "$others" = 0 ] && [ "${2:-}" != --force ]; then die "$name is the only ready version — pass --force to take the site down"; fi
  k delete deployment/"$name" service/"$name" --ignore-not-found --wait=false >&2
  log "$name retired; if it was active, the router moves the pointer to the newest ready version"
}

cmd=${1:-}
[ $# -gt 0 ] && shift
case "$cmd" in
  rollout)  [ $# -ge 2 ] || die "usage: rollout <id> <image> [timeout_s]"; rollout "$@" ;;
  promote)  [ $# -ge 1 ] || die "usage: promote <id|name|previous> [timeout_s]"; promote "$@" ;;
  revoke-older) revoke_older ;;
  retire)   [ $# -ge 1 ] || die "usage: retire <id|name> [--force]"; retire "$@" ;;
  active)   active ;;
  previous) previous ;;
  list)     versions_json | jq -r '(["", "NAME", "ID", "PROMOTED", "READY", "IMAGE"] | @tsv),
                                   (.[] | [(if .active then "*" else "" end), .name, .id, .at, "\(.ready)/\(.replicas)", .image] | @tsv)' ;;
  *) sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//; /^set -euo/d' >&2; exit 2 ;;
esac
