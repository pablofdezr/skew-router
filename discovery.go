package main

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	discoverylisters "k8s.io/client-go/listers/discovery/v1"
	"k8s.io/client-go/tools/cache"
)

// DiscoveryConfig says which Services are web versions and how to read them.
type DiscoveryConfig struct {
	Namespace    string
	Selector     labels.Selector // e.g. app=web
	VersionLabel string          // e.g. skew-router.dev/version
	PortName     string          // Service port to proxy to
}

// Discovery watches the version Services, their EndpointSlices and the
// pointer ConfigMap in one namespace through shared informers. Reads come
// from the informer cache, so when the API server is unreachable the last
// observed state keeps being served.
type Discovery struct {
	cfg        DiscoveryConfig
	factories  []informers.SharedInformerFactory
	services   corelisters.ServiceLister
	slices     discoverylisters.EndpointSliceLister
	configMaps corelisters.ConfigMapLister
	synced     []cache.InformerSynced
}

// NewDiscovery wires the informers; notify is called (from informer
// goroutines) on every change so the controller can react immediately.
func NewDiscovery(client kubernetes.Interface, cfg DiscoveryConfig, notify func()) (*Discovery, error) {
	req, err := labels.NewRequirement(cfg.VersionLabel, selection.Exists, nil)
	if err != nil {
		return nil, fmt.Errorf("version label: %w", err)
	}
	cfg.Selector = cfg.Selector.Add(*req)

	factory := informers.NewSharedInformerFactoryWithOptions(client, 10*time.Minute,
		informers.WithNamespace(cfg.Namespace))
	svc := factory.Core().V1().Services()
	sl := factory.Discovery().V1().EndpointSlices()
	// Only the pointer ConfigMap: RBAC grants nothing else.
	cmFactory := informers.NewSharedInformerFactoryWithOptions(client, 10*time.Minute,
		informers.WithNamespace(cfg.Namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = fields.OneTermEqualSelector("metadata.name", PointerConfigMap).String()
		}))
	cm := cmFactory.Core().V1().ConfigMaps()

	h := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { notify() },
		UpdateFunc: func(any, any) { notify() },
		DeleteFunc: func(any) { notify() },
	}
	if _, err := svc.Informer().AddEventHandler(h); err != nil {
		return nil, err
	}
	if _, err := sl.Informer().AddEventHandler(h); err != nil {
		return nil, err
	}
	if _, err := cm.Informer().AddEventHandler(h); err != nil {
		return nil, err
	}
	return &Discovery{
		cfg:        cfg,
		factories:  []informers.SharedInformerFactory{factory, cmFactory},
		services:   svc.Lister(),
		slices:     sl.Lister(),
		configMaps: cm.Lister(),
		synced: []cache.InformerSynced{
			svc.Informer().HasSynced, sl.Informer().HasSynced, cm.Informer().HasSynced,
		},
	}, nil
}

// Start runs the informers until stop is closed.
func (d *Discovery) Start(stop <-chan struct{}) {
	for _, f := range d.factories {
		f.Start(stop)
	}
}

// Pointer returns the active deployment id ("" when not set yet).
func (d *Discovery) Pointer() string {
	cm, err := d.configMaps.ConfigMaps(d.cfg.Namespace).Get(PointerConfigMap)
	if err != nil {
		return ""
	}
	return cm.Data[PointerKeyID]
}

// Synced reports whether every informer completed its initial list. The
// controller neither routes nor deletes anything before that.
func (d *Discovery) Synced() bool {
	for _, s := range d.synced {
		if !s() {
			return false
		}
	}
	return true
}

// Versions returns every web version currently known, ready or not.
func (d *Discovery) Versions() []Version {
	svcs, err := d.services.List(d.cfg.Selector)
	if err != nil {
		return nil
	}
	ready := d.readyServices()
	out := make([]Version, 0, len(svcs))
	for _, s := range svcs {
		v, ok := d.version(s)
		if !ok {
			continue
		}
		v.Ready = ready[s.Name]
		out = append(out, v)
	}
	return out
}

func (d *Discovery) version(s *corev1.Service) (Version, bool) {
	port, ok := servicePort(s, d.cfg.PortName)
	if !ok {
		return Version{}, false
	}
	host := s.Spec.ClusterIP
	if host == "" || host == corev1.ClusterIPNone {
		host = s.Name + "." + s.Namespace + ".svc"
	}
	v := Version{
		ID:        s.Labels[d.cfg.VersionLabel],
		Name:      s.Name,
		UID:       s.UID,
		RV:        s.ResourceVersion,
		Upstream:  &url.URL{Scheme: "http", Host: net.JoinHostPort(host, strconv.Itoa(int(port)))},
		CreatedAt: s.CreationTimestamp.Time,
	}
	v.PromotedAt = annotationTime(s, AnnPromotedAt)
	v.SupersededAt = annotationTime(s, AnnSupersededAt)
	v.ExpiredAt = annotationTime(s, AnnExpiredAt)
	v.ScaledDownAt = annotationTime(s, AnnScaledDownAt)
	v.RevokedAt = annotationTime(s, AnnRevokedAt)
	v.RevokesOlder = s.Annotations[AnnRevokesOlder] == "true"
	return v, v.ID != ""
}

// annotationTime parses an RFC3339 annotation; zero when absent or invalid.
func annotationTime(s *corev1.Service, key string) time.Time {
	t, err := time.Parse(time.RFC3339, s.Annotations[key])
	if err != nil {
		return time.Time{}
	}
	return t
}

func servicePort(s *corev1.Service, name string) (int32, bool) {
	for _, p := range s.Spec.Ports {
		if p.Name == name {
			return p.Port, true
		}
	}
	if len(s.Spec.Ports) == 1 {
		return s.Spec.Ports[0].Port, true
	}
	return 0, false
}

// readyServices maps Service name → has at least one ready endpoint.
func (d *Discovery) readyServices() map[string]bool {
	slices, err := d.slices.List(labels.Everything())
	if err != nil {
		return nil
	}
	ready := map[string]bool{}
	for _, sl := range slices {
		svc := sl.Labels[discoveryv1.LabelServiceName]
		if svc == "" || ready[svc] {
			continue
		}
		for _, ep := range sl.Endpoints {
			// A nil Ready condition means "unknown", which consumers must
			// treat as ready (EndpointSlice API contract).
			if len(ep.Addresses) > 0 && (ep.Conditions.Ready == nil || *ep.Conditions.Ready) {
				ready[svc] = true
				break
			}
		}
	}
	return ready
}
