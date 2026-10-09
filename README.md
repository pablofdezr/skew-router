# skew-router

**Skew protection for a self-hosted Next.js app on Kubernetes and Traefik.** A browser tab keeps
talking to the build that served it, even after you deploy a new one, so its JavaScript chunks,
RSC payloads and prefetches never land on a server that doesn't have them.

```
browser ──▶ Traefik ──────────────────────▶ Service web-<id>   (one Deployment + Service per build)
               ▲
               │ routes, every few seconds (Traefik HTTP provider)
               │
         skew-router   pointer, retention, revocation
```

[Vercel does this](https://vercel.com/docs/skew-protection) for its deployments. skew-router does
it inside your cluster without sitting in the request path: it watches one Service per web build
and tells Traefik where each request goes. Traefik then sends every request straight to its
build, so skew protection costs nothing per request.

The story behind it: [Two Builds, One Address](https://www.pablofr.com/blog/two-builds-one-address).

## How it routes

Next.js already tells you which build a request belongs to. With `deploymentId` set at build time,
it adds `?dpl=<id>` to every `/_next/static` URL and sends `x-deployment-id: <id>` on client
navigations and prefetches. skew-router turns that into Traefik routes:

| The request carries | It goes to |
| --- | --- |
| no id (a document navigation, a crawler) | the active version |
| the id of a version that is still routable | that version |
| an unknown, retired or revoked id | the active version (Next then reloads the page) |
| anything, while no version is ready | `503` |

The **active version** is an explicit pointer in a ConfigMap (`web-routing`), not "the newest
pod". A deploy only stamps the new version as promoted; the router moves the pointer once that
version has ready endpoints, and never to one without.

## Route templates

A real site rarely has one route. It has a catch-all with its rate limit and WAF, a static-assets
route with a different limit, a route for internal traffic with no limits. You describe those
routes once, in a JSON file (`-routes`), exactly as you'd write them in Traefik:

```json
{
  "routes": [
    { "name": "trusted", "priority": 1000,
      "rule": "Host(`www.example.com`) && ClientIP(`10.42.0.0/16`)",
      "entryPoints": ["websecure"], "middlewares": ["web-strip-token@kubernetescrd"], "tls": {} },
    { "name": "static", "priority": 500,
      "rule": "Host(`www.example.com`) && PathPrefix(`/_next/static/`)",
      "entryPoints": ["websecure"], "middlewares": ["web-edge-static@kubernetescrd"], "tls": {} },
    { "name": "web", "priority": 100,
      "rule": "Host(`www.example.com`)",
      "entryPoints": ["websecure"], "middlewares": ["web-edge-web@kubernetescrd"], "tls": {} }
  ]
}
```

For each template skew-router generates:

- **a default router:** the template's rule, priority and middlewares, to the active version;
- **a pinned router per routable version:** the template's rule AND the version's id (header or
  `?dpl=`), at priority + 1, with the **same middlewares**, straight to that version.

So a pinned request keeps exactly the protection its template gives it: a pinned `/_next/static`
chunk still gets the static route's rate limit. Keep template priorities at least 2 apart, so a
pinned router never ties or overtakes the next template (the router refuses a file that doesn't).

It also serves a service `skew-active` (referenced from other providers as `skew-active@http`)
pointing at the active version, for routes that need a backend but no pinning, like an apex
redirect.

A single route can be given with flags instead of a file: `-traefik-rule`,
`-traefik-entrypoints`, `-traefik-middlewares`, `-traefik-certresolver` or `-traefik-tls`.

## Retention

- **The previous version stays routable** for `-version-ttl` (default 1 h) after it stopped being
  active, so open tabs keep working.
- **Then it is scaled to 0, not deleted:** the objects stay, with the replica count recorded, so an
  instant rollback is a scale-up plus a pointer move.
- **Beyond `-max-versions`** (default 2, the active one included), versions are deleted.
- **Security fixes skip the window:** a version annotated `revokes-older=true` unroutes every older
  version the moment it is active and ready, and they are deleted after the drain. A revoked
  version is never routed again: a 503 beats serving a build with a known hole.

The timers are annotations on the version Services, so they survive router restarts and leader
changes.

## High availability

- Run several replicas: each builds the routing table from its informer cache with the same pure
  function, so all of them serve the same routes.
- Only the Lease holder (leader election) writes: the pointer, the timers, scale-downs and deletes.
  Writes are pinned with UID and `resourceVersion` preconditions plus a version-label check,
  because RBAC can't scope a write to objects with a label.
- If every replica is down, Traefik keeps the last routes it fetched and goes on routing (frozen)
  until one is back. Tested: with the endpoint gone for 8 s, Traefik kept answering 200.

## Set it up

1. **Bake the id into the build** ([`examples/next.config.ts`](examples/next.config.ts)):
   `deploymentId: process.env.NEXT_DEPLOYMENT_ID`, with `NEXT_DEPLOYMENT_ID` set to the commit SHA
   in CI. It must be a valid Kubernetes label value.
2. **Install the router** ([`examples/skew-router.yaml`](examples/skew-router.yaml)): ServiceAccount,
   a namespace-scoped Role, 2 replicas, a PDB, the routes ConfigMap. Build the image with
   `docker build -t <your-registry>/skew-router:<tag> .`
3. **Point Traefik at it** (static configuration):

   ```yaml
   providers:
     http:
       endpoint: http://skew-router.web.svc.cluster.local:9090/traefik
       pollInterval: 2s
   ```

   On k3s that's `additionalArguments` in the Traefik `HelmChartConfig`. Remove your own routes for
   the site: the generated ones replace them.
4. **Deploy each build as its own version** with
   [`examples/web-version.sh`](examples/web-version.sh) and the
   [`examples/web-version.yaml`](examples/web-version.yaml) template:

```sh
NS=web ./examples/web-version.sh rollout "$GIT_SHA" registry.example.com/web:"$GIT_SHA"
NS=web ./examples/web-version.sh promote previous        # instant rollback
NS=web WEB_REVOKE_OLDER=1 ./examples/web-version.sh rollout "$SHA" "$IMAGE"   # a security fix
NS=web ./examples/web-version.sh list
```

`rollout` waits until the router has made the new version active and deletes it if that never
happens, so a broken build leaves the previous one serving.

### The contract this creates

Two builds now serve users at the same time for up to an hour. Anything they share (your API, the
database) must stay compatible with both for at least `-version-ttl`. Only the web build is
versioned; a backend fix reaches every version at once.

If you use Server Actions, set a stable `NEXT_SERVER_ACTIONS_ENCRYPTION_KEY`, or an action posted
from an old tab fails against a new build.

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `-routes` | none | JSON file with the route templates |
| `-traefik-rule` (and `-traefik-entrypoints`, `-traefik-middlewares`, `-traefik-certresolver`, `-traefik-tls`) | none | a single route, instead of `-routes` |
| `-admin-listen` | `:9090` | `/traefik`, `/healthz`, `/readyz`, `/metrics`, `/versions` (keep it private) |
| `-namespace` | `$POD_NAMESPACE` or `default` | namespace of the web versions |
| `-selector` | `app=web` | label selector of the version Services |
| `-version-label` | `skew-router.dev/version` | label holding the deployment id |
| `-annotation-prefix` | `skew-router.dev` | prefix of the promotion and retention annotations |
| `-pointer-configmap` | `web-routing` | ConfigMap holding the active version |
| `-port-name` | `http` | Service port the routes point at |
| `-version-ttl` | `1h` | how long a version stays routable after it stops being active |
| `-max-versions` | `2` | versions kept, the active one included |
| `-drain` | `10s` | wait between unrouting a version and scaling it down or deleting it |
| `-reconcile-interval` | `5s` | periodic reconcile, on top of watch events |
| `-shutdown-delay` | `3s` | keep answering (unready) this long after SIGTERM |
| `-leader-elect` | `true` | only the Lease holder writes |
| `-lease-name` | `skew-router` | Lease used for leader election |
| `-dry-run` | `false` | log deletions instead of doing them |

`web-version.sh` reads the same prefix from `PREFIX` (default `skew-router.dev`); keep the two in
sync.

## Overhead

None per request: requests never pass through the router. Measured behind a real Traefik on a 4
vCPU AMD EPYC box (Traefik, backend and load generator sharing the cores; 32 concurrent clients;
`TestMeasureTraefik`, which also checks every generated route against that Traefik):

| Path | req/s | p50 | p99 |
| --- | --- | --- | --- |
| Traefik → version, no skew protection | 18,759 | 1.3 ms | 6.5 ms |
| Traefik → version, skew routes (pinned) | 18,322 | 1.4 ms | 6.1 ms |

Up to v0.2 skew-router could also proxy the traffic itself; behind Traefik that halved throughput
(10,668 vs 20,975 req/s in the same setup), so v0.3 removed it.

## Observability

- `GET /versions` on the admin port: the routing table every replica is using.
- `GET /traefik`: the routes Traefik is being given.
- `GET /metrics`: `skew_router_versions_routable`, `skew_router_versions_retired_total`,
  `skew_router_versions_revoked_total`, `skew_router_active_info`. Per-request numbers are
  Traefik's: its router metrics carry the version id in the router name.
- JSON logs on stdout.

## Limits

- **Traefik only:** the routes are Traefik's dynamic configuration.
- **It costs a second set of pods** for up to `-version-ttl` after every deploy.
- **A pointer move takes up to one poll interval** to reach Traefik.
- **Revocation loses client state:** tabs on a revoked build reload into the fix.
- **An instant rollback to a version scaled to 0** may need to pull its image again if your nodes
  pruned it.

## License

[MIT](LICENSE)
