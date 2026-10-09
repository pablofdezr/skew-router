# skew-router

**Skew protection for a self-hosted Next.js app on Kubernetes.** A browser tab keeps talking to
the build that served it, even after you deploy a new one, so its JavaScript chunks, RSC payloads
and prefetches never land on a server that doesn't have them.

```
ingress ──▶ skew-router :8080 ──▶ Service web-<id> :3000   (one Deployment + Service per build)
```

[Vercel does this](https://vercel.com/docs/skew-protection) for its deployments. skew-router does
it inside your cluster: a small Go proxy (client-go informers, no other dependencies) that sits
between the ingress and one Service per web build.

The story behind it: [Two Builds, One Address](https://www.pablofr.com/blog/two-builds-one-address).

## How it routes

Next.js already tells you which build a request belongs to. With `deploymentId` set at build time,
it adds `?dpl=<id>` to every `/_next/static` URL and sends `x-deployment-id: <id>` on client
navigations and prefetches. skew-router reads the header first, then `?dpl=`, and picks a version:

| The request carries | It goes to | Metric outcome |
| --- | --- | --- |
| no id (a document navigation, a crawler) | the active version | `unpinned` |
| the active version's id | the active version | `latest` |
| the id of an older version that is still routable | that version | `pinned` |
| an unknown, retired or revoked id | the active version (Next then reloads the page) | `fallback` |
| anything, while no version is ready | `503 no web version available` | `unavailable` |

The **active version** is an explicit pointer in a ConfigMap (`web-routing`), not "the newest
pod". A deploy only stamps the new version as promoted; the router moves the pointer once that
version has ready endpoints, and never to one without.

## Retention

- **The previous version stays routable** for `-version-ttl` (default 1 h) after it stopped being
  active, so open tabs keep working.
- **Then it is scaled to 0, not deleted:** the objects stay, with the replica count recorded, so an
  instant rollback is a scale-up plus a pointer move.
- **Beyond `-max-versions`** (default 2, the active one included), versions are deleted.
- **Security fixes skip the window:** a version annotated `revokes-older=true` unroutes every older
  version the moment it is active and ready, on every replica at once, and they are deleted after
  the drain. A revoked version is never served again, not even as the fallback: a 503 beats
  serving a build with a known hole.

The timers are annotations on the version Services, so they survive router restarts and leader
changes.

## High availability

- Run several replicas: every replica routes from its informer cache, and the planning step is a
  pure function, so all of them compute the same routing table.
- Only the Lease holder (leader election) writes: the pointer, the timers, scale-downs and deletes.
- Writes are pinned with UID and `resourceVersion` preconditions plus a version-label check,
  because RBAC can't scope a write to objects with a label.
- Refused connections to a just-created Service (kube-proxy not programmed yet) are retried 5
  times, 200 ms apart, instead of becoming a 502.
- If every router replica is down, a Traefik failover service sends traffic straight to all web
  pods ([`examples/traefik-failover.yml`](examples/traefik-failover.yml)). Pinning is lost
  meanwhile, the site stays up.

## Set it up

1. **Bake the id into the build** ([`examples/next.config.ts`](examples/next.config.ts)):
   `deploymentId: process.env.NEXT_DEPLOYMENT_ID`, with `NEXT_DEPLOYMENT_ID` set to the commit SHA
   in CI. It must be a valid Kubernetes label value.
2. **Install the router** ([`examples/skew-router.yaml`](examples/skew-router.yaml)): ServiceAccount,
   a namespace-scoped Role, 2 replicas, a PDB, and the `web-fallback` Service. Build the image with
   `docker build -t <your-registry>/skew-router:<tag> .`
3. **Deploy each build as its own version** with
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
| `-listen` | `:8080` | proxy address |
| `-admin-listen` | `:9090` | `/healthz`, `/readyz`, `/metrics`, `/versions` (keep it private) |
| `-namespace` | `$POD_NAMESPACE` or `default` | namespace of the web versions |
| `-selector` | `app=web` | label selector of the version Services |
| `-version-label` | `skew-router.dev/version` | label holding the deployment id |
| `-annotation-prefix` | `skew-router.dev` | prefix of the promotion and retention annotations |
| `-pointer-configmap` | `web-routing` | ConfigMap holding the active version |
| `-port-name` | `http` | Service port to proxy to |
| `-version-ttl` | `1h` | how long a version stays routable after it stops being active |
| `-max-versions` | `2` | versions kept, the active one included |
| `-drain` | `10s` | wait between unrouting a version and scaling it down or deleting it |
| `-reconcile-interval` | `5s` | periodic reconcile, on top of watch events |
| `-shutdown-delay` | `3s` | keep serving (unready) this long after SIGTERM |
| `-leader-elect` | `true` | only the Lease holder writes |
| `-lease-name` | `skew-router` | Lease used for leader election |
| `-dry-run` | `false` | log deletions instead of doing them |

`web-version.sh` reads the same prefix from `PREFIX` (default `skew-router.dev`); keep the two in
sync.

## Observability

- `GET /versions` on the admin port: the routing table every replica is using.
- `GET /metrics`: `skew_router_requests_total{outcome}`, `skew_router_versions_routable`,
  `skew_router_versions_retired_total`, `skew_router_versions_revoked_total`.
- JSON logs on stdout.

## Limits

- **It costs a second set of pods** for up to `-version-ttl` after every deploy.
- **Revocation loses client state:** tabs on a revoked build reload into the fix.
- **An instant rollback to a version scaled to 0** may need to pull its image again if your nodes
  pruned it.
- **Kubernetes only:** it discovers versions through the API server. On plain Docker you'd need a
  different discovery layer.

## License

[MIT](LICENSE)
