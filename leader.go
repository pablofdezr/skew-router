package main

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// runLeaderElection campaigns for the Lease until ctx is done, keeping
// `leading` in sync. Only the leader writes retention annotations and
// deletes versions; every replica routes. Losing the lease (API outage,
// pause) just stops the bookkeeping until it is won again — routing is
// unaffected. onElected is called after winning, to reconcile at once.
func runLeaderElection(ctx context.Context, client kubernetes.Interface, namespace, lease, identity string,
	leading *atomic.Bool, onElected func(), log *slog.Logger) {
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: lease, Namespace: namespace},
		Client:     client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: identity},
	}
	for ctx.Err() == nil {
		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock:            lock,
			LeaseDuration:   15 * time.Second,
			RenewDeadline:   10 * time.Second,
			RetryPeriod:     2 * time.Second,
			ReleaseOnCancel: true,
			Name:            lease,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(context.Context) {
					leading.Store(true)
					log.Info("leader: this replica now runs version retention", "identity", identity)
					onElected()
				},
				OnStoppedLeading: func() {
					if leading.Swap(false) {
						log.Info("leader: lost the lease", "identity", identity)
					}
				},
				OnNewLeader: func(id string) {
					if id != identity {
						log.Info("leader: another replica leads", "leader", id)
					}
				},
			},
		})
		// RunOrDie returns when the lease is lost; campaign again.
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}
}
