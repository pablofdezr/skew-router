package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// KubeVersions writes to web versions in the cluster. RBAC can't scope a
// write to a label, so the scoping is enforced here: a Deployment or
// Service is only touched when it carries the version label with the
// expected id, and every write is pinned to the UID that was checked.
type KubeVersions struct {
	Client       kubernetes.Interface
	Namespace    string
	VersionLabel string
}

// SetPointer makes v the active version (creates the ConfigMap the first
// time). The ConfigMap is the record of truth for unpinned traffic.
func (k *KubeVersions) SetPointer(ctx context.Context, v Version, at time.Time) error {
	data := map[string]string{
		PointerKeyID:   v.ID,
		PointerKeyName: v.Name,
		PointerKeyAt:   at.UTC().Format(time.RFC3339),
	}
	cms := k.Client.CoreV1().ConfigMaps(k.Namespace)
	cm, err := cms.Get(ctx, PointerConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = cms.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:   PointerConfigMap,
				Labels: map[string]string{"app.kubernetes.io/managed-by": "skew-router"},
			},
			Data: data,
		}, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	cm.Data = data
	_, err = cms.Update(ctx, cm, metav1.UpdateOptions{}) // resourceVersion-checked
	return err
}

// Annotate merge-patches annotations on the version's Service; an empty
// value removes the key. The patch carries the observed UID and
// resourceVersion: it fails rather than overwrite a newer state (the next
// pass retries from fresh state) or touch a Service recreated by name.
func (k *KubeVersions) Annotate(ctx context.Context, v Version, ann map[string]string) error {
	if v.ID == "" || v.UID == "" {
		return fmt.Errorf("refusing to annotate %s: no version id/UID observed", v.Name)
	}
	vals := map[string]any{}
	for key, val := range ann {
		if val == "" {
			vals[key] = nil
		} else {
			vals[key] = val
		}
	}
	meta := map[string]any{"uid": v.UID, "annotations": vals}
	if v.RV != "" {
		meta["resourceVersion"] = v.RV
	}
	patch, err := json.Marshal(map[string]any{"metadata": meta})
	if err != nil {
		return err
	}
	_, err = k.Client.CoreV1().Services(k.Namespace).Patch(ctx, v.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		// Gone, or changed since observed (often by our own previous
		// write the informer hasn't delivered yet): the next pass decides
		// again from fresh state.
		return nil
	}
	return err
}

// ScaleDown sets the version's Deployment to 0 replicas and records the
// previous count (AnnScaledFrom) so a rollback can restore it. The
// Deployment and Service stay: scaling back up is seconds (the image is
// already on the node), not a rebuild.
func (k *KubeVersions) ScaleDown(ctx context.Context, v Version) error {
	dep, err := k.Client.AppsV1().Deployments(k.Namespace).Get(ctx, v.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get deployment %s: %w", v.Name, err)
	}
	if err := k.checkLabel("deployment", v, dep.Labels); err != nil {
		return err
	}
	replicas := int32(1)
	if dep.Spec.Replicas != nil {
		replicas = *dep.Spec.Replicas
	}
	if replicas == 0 {
		return nil
	}
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"uid":         dep.UID,
			"annotations": map[string]string{AnnScaledFrom: strconv.Itoa(int(replicas))},
		},
		"spec": map[string]any{"replicas": 0},
	})
	if err != nil {
		return err
	}
	_, err = k.Client.AppsV1().Deployments(k.Namespace).Patch(ctx, v.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

// Retire deletes the version's Deployment, then its Service. Missing
// objects are fine: another replica or an operator got there first.
func (k *KubeVersions) Retire(ctx context.Context, v Version) error {
	// Deployment first: its pods get SIGTERM after their preStop sleep and
	// finish in-flight requests. The Service is no longer routed anyway.
	dep, err := k.Client.AppsV1().Deployments(k.Namespace).Get(ctx, v.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		return fmt.Errorf("get deployment %s: %w", v.Name, err)
	default:
		if err := k.checkLabel("deployment", v, dep.Labels); err != nil {
			return err
		}
		if err := deletePinned(dep.UID, func(o metav1.DeleteOptions) error {
			return k.Client.AppsV1().Deployments(k.Namespace).Delete(ctx, v.Name, o)
		}); err != nil {
			return fmt.Errorf("delete deployment %s: %w", v.Name, err)
		}
	}

	svc, err := k.Client.CoreV1().Services(k.Namespace).Get(ctx, v.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return nil
	case err != nil:
		return fmt.Errorf("get service %s: %w", v.Name, err)
	}
	if err := k.checkLabel("service", v, svc.Labels); err != nil {
		return err
	}
	if v.UID != "" && svc.UID != v.UID {
		return fmt.Errorf("refusing to delete service %s: UID changed (recreated since observed)", v.Name)
	}
	if err := deletePinned(svc.UID, func(o metav1.DeleteOptions) error {
		return k.Client.CoreV1().Services(k.Namespace).Delete(ctx, v.Name, o)
	}); err != nil {
		return fmt.Errorf("delete service %s: %w", v.Name, err)
	}
	return nil
}

func (k *KubeVersions) checkLabel(kind string, v Version, labels map[string]string) error {
	if got, ok := labels[k.VersionLabel]; !ok || got != v.ID {
		return fmt.Errorf("refusing to touch %s %s: label %s=%q, want %q", kind, v.Name, k.VersionLabel, got, v.ID)
	}
	return nil
}

func deletePinned(uid types.UID, del func(metav1.DeleteOptions) error) error {
	bg := metav1.DeletePropagationBackground
	err := del(metav1.DeleteOptions{
		Preconditions:     &metav1.Preconditions{UID: &uid},
		PropagationPolicy: &bg,
	})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
