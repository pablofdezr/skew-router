// skew-router sits between the ingress (Traefik) and the web versions and
// implements Vercel-style skew protection for a self-hosted Next.js app.
//
// Every web build carries a deployment id (next.config `deploymentId`).
// Each build runs as its own Deployment + Service `web-<id>`, labelled
// `skew-router.dev/version=<id>`. A browser tab keeps sending its
// build's id (`x-deployment-id` header on client navigations, `?dpl=` on
// static assets), and this router sends those requests to the version
// that served the tab, as long as it is alive. Requests without an id
// (document navigations) and ids of versions that are gone go to the
// latest version. Older versions are retired after a TTL and beyond a
// maximum count — or at once when the active version is a security fix
// (annotation `skew-router.dev/revokes-older`).
//
// Runs as several replicas on any node: every replica routes from cluster
// state alone; one of them (Lease leader election) keeps the retention
// timers — stored as annotations on the version Services, so restarts and
// failovers keep them — and deletes expired versions.
//
// See README.md for the operational side and examples/ for the manifests.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type config struct {
	listen, adminListen    string
	kubeconfig, namespace  string
	selector, versionLabel string
	port                   string
	ttl, drain, resync     time.Duration
	shutdownDelay          time.Duration
	maxVersions            int
	dryRun                 bool
	leaderElect            bool
	leaseName, identity    string
	annotationPrefix       string
	pointerConfigMap       string
	mode                   string // proxy | traefik
	traefik                TraefikOptions
}

func parseFlags(args []string) (config, error) {
	var c config
	fs := flag.NewFlagSet("skew-router", flag.ContinueOnError)
	fs.StringVar(&c.listen, "listen", ":8080", "proxy listen address")
	fs.StringVar(&c.adminListen, "admin-listen", ":9090", "health/metrics listen address (not exposed publicly)")
	fs.StringVar(&c.kubeconfig, "kubeconfig", "", "kubeconfig path (default: in-cluster config)")
	fs.StringVar(&c.namespace, "namespace", envOr("POD_NAMESPACE", "default"), "namespace of the web versions")
	fs.StringVar(&c.selector, "selector", "app=web", "label selector of web version Services")
	fs.StringVar(&c.versionLabel, "version-label", DefaultAnnotationPrefix+"/version", "label holding the deployment id")
	fs.StringVar(&c.annotationPrefix, "annotation-prefix", DefaultAnnotationPrefix, "prefix of the annotations holding promotion and retention state")
	fs.StringVar(&c.pointerConfigMap, "pointer-configmap", PointerConfigMap, "ConfigMap holding the active version")
	fs.StringVar(&c.port, "port-name", "http", "Service port name to proxy to")
	fs.DurationVar(&c.ttl, "version-ttl", time.Hour, "how long a version stays routable after it stops being latest")
	fs.IntVar(&c.maxVersions, "max-versions", 2, "versions kept alive, latest included")
	fs.DurationVar(&c.drain, "drain", 10*time.Second, "wait between unrouting an expired version and deleting it")
	fs.DurationVar(&c.resync, "reconcile-interval", 5*time.Second, "periodic reconcile (TTL expiry), on top of watch events")
	fs.DurationVar(&c.shutdownDelay, "shutdown-delay", 3*time.Second, "on SIGTERM, keep serving this long (unready) before closing listeners")
	fs.BoolVar(&c.dryRun, "dry-run", false, "log deletions instead of performing them")
	fs.StringVar(&c.mode, "mode", "proxy", "proxy: requests go through the router; traefik: the router serves Traefik routes on the admin port (/traefik) and carries no traffic")
	fs.StringVar(&c.traefik.Rule, "traefik-rule", "", "traefik mode: the site's Traefik rule, e.g. Host(`www.example.com`)")
	var eps, mws string
	fs.StringVar(&eps, "traefik-entrypoints", "websecure", "traefik mode: entrypoints, comma-separated")
	fs.StringVar(&mws, "traefik-middlewares", "", "traefik mode: middlewares for every router, comma-separated")
	fs.StringVar(&c.traefik.CertResolver, "traefik-certresolver", "", "traefik mode: TLS certificate resolver")
	fs.BoolVar(&c.traefik.TLS, "traefik-tls", false, "traefik mode: TLS with the default certificate (when no resolver)")
	fs.BoolVar(&c.leaderElect, "leader-elect", true, "only the Lease holder writes retention annotations and deletes versions (all replicas route)")
	fs.StringVar(&c.leaseName, "lease-name", "skew-router", "coordination.k8s.io Lease used for leader election")
	fs.StringVar(&c.identity, "identity", envOr("POD_NAME", hostname()), "leader election identity")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	c.traefik.EntryPoints, c.traefik.Middlewares = splitCSV(eps), splitCSV(mws)
	switch c.mode {
	case "proxy":
	case "traefik":
		if c.traefik.Rule == "" {
			return c, errors.New("-mode=traefik needs -traefik-rule")
		}
	default:
		return c, fmt.Errorf("-mode %q: proxy or traefik", c.mode)
	}
	if c.annotationPrefix == "" || c.pointerConfigMap == "" {
		return c, errors.New("-annotation-prefix and -pointer-configmap must not be empty")
	}
	SetAnnotationPrefix(c.annotationPrefix)
	PointerConfigMap = c.pointerConfigMap
	if c.maxVersions < 1 {
		return c, errors.New("-max-versions must be >= 1")
	}
	if c.ttl <= 0 || c.drain < 0 || c.resync <= 0 {
		return c, errors.New("-version-ttl and -reconcile-interval must be > 0, -drain >= 0")
	}
	return c, nil
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		return err
	}
	sel, err := labels.Parse(cfg.selector)
	if err != nil {
		return fmt.Errorf("-selector: %w", err)
	}
	client, err := kubeClient(cfg.kubeconfig)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	trigger := make(chan struct{}, 1)
	notify := func() {
		select {
		case trigger <- struct{}{}:
		default:
		}
	}
	disc, err := NewDiscovery(client, DiscoveryConfig{
		Namespace:    cfg.namespace,
		Selector:     sel,
		VersionLabel: cfg.versionLabel,
		PortName:     cfg.port,
	}, notify)
	if err != nil {
		return err
	}
	metrics := &Metrics{}
	var leading atomic.Bool
	ctrl := &Controller{
		Source:  disc,
		Writer:  &KubeVersions{Client: client, Namespace: cfg.namespace, VersionLabel: cfg.versionLabel},
		Policy:  Policy{TTL: cfg.ttl, MaxVersions: cfg.maxVersions},
		Drain:   cfg.drain,
		DryRun:  cfg.dryRun,
		Leader:  leading.Load,
		Now:     time.Now,
		Log:     log,
		Metrics: metrics,
	}
	disc.Start(ctx.Done())
	go ctrl.Run(ctx, trigger, cfg.resync)
	if cfg.leaderElect {
		go runLeaderElection(ctx, client, cfg.namespace, cfg.leaseName, cfg.identity, &leading, notify, log)
	} else {
		leading.Store(true)
	}

	var shuttingDown atomic.Bool
	proxySrv := &http.Server{
		Addr:              cfg.listen,
		Handler:           NewProxy(ctrl.Routes, metrics, log),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
		// No Read/WriteTimeout: uploads and streamed responses (RSC, SSE)
		// may legitimately run long. Upstream hangs are bounded by the
		// transport's ResponseHeaderTimeout.
	}
	adminSrv := &http.Server{
		Addr:              cfg.adminListen,
		Handler:           adminMux(ctrl, metrics, &shuttingDown, cfg.traefik),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errc := make(chan error, 2)
	for _, s := range []*http.Server{proxySrv, adminSrv} {
		go func(s *http.Server) {
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errc <- err
			}
		}(s)
	}
	log.Info("skew-router started", "mode", cfg.mode, "listen", cfg.listen, "namespace", cfg.namespace,
		"ttl", cfg.ttl.String(), "max_versions", cfg.maxVersions, "dry_run", cfg.dryRun)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	// Graceful stop: report unready so the Service drops this pod, keep
	// serving through the delay (endpoint removal propagates), then stop
	// accepting and let in-flight requests finish. Hijacked connections
	// (WebSockets) are not waited for; their clients reconnect.
	shuttingDown.Store(true)
	log.Info("shutting down", "delay", cfg.shutdownDelay.String())
	time.Sleep(cfg.shutdownDelay)
	sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = proxySrv.Shutdown(sctx)
	_ = adminSrv.Shutdown(sctx)
	return err
}

func kubeClient(kubeconfig string) (kubernetes.Interface, error) {
	var (
		rc  *rest.Config
		err error
	)
	if kubeconfig != "" {
		rc, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		rc, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("kube config: %w", err)
	}
	rc.UserAgent = "skew-router"
	return kubernetes.NewForConfig(rc)
}

// adminMux serves the probes, metrics and a JSON view of the routing
// table. It listens on its own port, never reachable through Traefik
// (paths like /healthz on the public port belong to the web app).
func adminMux(ctrl *Controller, m *Metrics, shuttingDown *atomic.Bool, tr TraefikOptions) *http.ServeMux {
	mux := http.NewServeMux()
	// Traefik's HTTP provider polls this (-mode=traefik). Served before the first table is built
	// only as a 503, so Traefik keeps what it had instead of loading an empty config.
	if tr.Rule != "" {
		mux.HandleFunc("GET /traefik", func(w http.ResponseWriter, _ *http.Request) {
			r := ctrl.Routes()
			if r == nil {
				http.Error(w, "routing table not built yet", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(TraefikConfig(r, tr))
		})
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	// Ready once the informer caches synced and a table was built. A
	// router with no ready web version still reports ready: it answers
	// 503 itself, and pulling it out would not make web come back.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if shuttingDown.Load() || ctrl.Routes() == nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("GET /metrics", m)
	mux.HandleFunc("GET /versions", func(w http.ResponseWriter, _ *http.Request) {
		type row struct {
			ID       string `json:"id"`
			Name     string `json:"name"`
			Upstream string `json:"upstream"`
		}
		var out struct {
			Leader   bool  `json:"leader"`
			Active   *row  `json:"active"`  // the pointer
			Serving  *row  `json:"serving"` // unpinned traffic (== active unless it is unready)
			Routable []row `json:"routable"`
		}
		out.Leader = ctrl.Leader == nil || ctrl.Leader()
		toRow := func(v *Version) *row {
			if v == nil {
				return nil
			}
			return &row{v.ID, v.Name, v.Upstream.String()}
		}
		if r := ctrl.Routes(); r != nil {
			out.Active, out.Serving = toRow(r.Active), toRow(r.Serving)
			for _, v := range r.byID {
				out.Routable = append(out.Routable, row{v.ID, v.Name, v.Upstream.String()})
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	return mux
}
