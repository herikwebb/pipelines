// Copyright 2018 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package reconciler describes a Reconciler for working with Viewer CRDs. The
// reconciler takes care of the main logic for ensuring every Viewer CRD
// corresponds to a unique deployment and backing service. The service is
// annotated such that it is compatible with Ambassador managed routing.
// Currently, only supports Tensorboard viewers. Adding a new viewer CRD for
// tensorboard with the name 'abc123' will result in the tensorboard instance
// serving under the path '/tensorboard/abc123'.
package reconciler

import (
	"context"
	"fmt"
	"strings"

	"github.com/golang/glog"
	viewerV1beta1 "github.com/kubeflow/pipelines/backend/src/crd/pkg/apis/viewer/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const viewerTargetPort = 6006

const defaultTensorflowImage = "tensorflow/tensorflow:1.13.2"

// tensorboardCommand is the entrypoint the controller forces on the viewer
// container. Pinning the command keeps a standard image from running an
// unexpected entrypoint and overrides any caller-supplied command; it is
// defense-in-depth, not a trust boundary, because the image itself is
// caller-selectable (see hardenViewerPodSpec for the threat model).
const tensorboardCommand = "tensorboard"

// Reconciler implements reconcile.Reconciler for the Viewer CRD.
type Reconciler struct {
	client.Client
	scheme *runtime.Scheme
	opts   *Options
}

// Options are the set of options to configure the behaviour of Reconciler.
type Options struct {
	// MaxNumViewers sets an upper bound on the number of viewer instances allowed.
	// When a user attempts to create one more viewer than this number, the oldest
	// existing viewer will be deleted.
	MaxNumViewers int
}

// New returns a new Reconciler.
func New(cli client.Client, scheme *runtime.Scheme, opts *Options) (*Reconciler, error) {
	if opts.MaxNumViewers < 1 {
		return nil, fmt.Errorf("MaxNumViewers should at least be 1. Got %d", opts.MaxNumViewers)
	}
	return &Reconciler{Client: cli, scheme: scheme, opts: opts}, nil
}

// Reconcile runs the main logic for reconciling the state of a viewer with a
// corresponding deployment and service allowing users to access the view under
// a specific path.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	glog.Infof("Reconcile request: %+v", req)

	view := &viewerV1beta1.Viewer{}
	if err := r.Get(context.Background(), req.NamespacedName, view); err != nil {
		if errors.IsNotFound(err) {
			// No viewer instance, so this may be the result of a delete.
			// Nothing to do.
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}
	glog.Infof("Got instance: %+v", view)

	// Ignore other viewer types for now.
	if view.Spec.Type != viewerV1beta1.ViewerTypeTensorboard {
		glog.Infof("Unsupported spec type: %q", view.Spec.Type)
		// Return nil to indicate nothing more to do here.
		return reconcile.Result{}, nil
	}

	if len(view.Spec.TensorboardSpec.TensorflowImage) == 0 {
		view.Spec.TensorboardSpec.TensorflowImage = defaultTensorflowImage
	}

	// Check and maybe delete the oldest viewer before creating the next one.
	if err := r.maybeDeleteOldestViewer(view.Spec.Type, view.Namespace); err != nil {
		// Couldn't delete. Requeue.
		return reconcile.Result{Requeue: true}, err
	}

	// Set up potential deployment.
	dpl, err := deploymentFrom(view)
	if err != nil {
		utilruntime.HandleError(err)
		// User error, don't requeue key.
		return reconcile.Result{}, nil
	}

	// Set the deployment to be owned by the view instance. This ensures that
	// deleting the viewer instance will delete the deployment as well.
	if err := controllerutil.SetControllerReference(view, dpl, r.scheme); err != nil {
		// Error means that the deployment is already owned by some other instance.
		utilruntime.HandleError(err)
		return reconcile.Result{}, err
	}

	foundDpl := &appsv1.Deployment{}
	nsn := types.NamespacedName{Name: dpl.Name, Namespace: dpl.Namespace}
	if err := r.Client.Get(context.Background(), nsn, foundDpl); err != nil {
		if errors.IsNotFound(err) {
			// Create a new instance.
			if createErr := r.Client.Create(context.Background(), dpl); createErr != nil {
				utilruntime.HandleError(fmt.Errorf("error creating deployment: %v", createErr))
				return reconcile.Result{}, createErr
			}
			glog.Infof("Created new deployment with spec: %+v", dpl)
		} else {
			// Some other error.
			utilruntime.HandleError(err)
			return reconcile.Result{}, err
		}
	} else if metav1.IsControlledBy(foundDpl, view) {
		// Re-enforce the hardened pod spec on a Deployment this Viewer already
		// owns, so one created before this control loop cannot retain unsafe
		// fields. Only owned Deployments are touched, so a Viewer whose derived
		// name collides with an unrelated workload cannot mutate it.
		if err := r.hardenExistingDeployment(foundDpl); err != nil {
			utilruntime.HandleError(fmt.Errorf("error hardening existing deployment: %v", err))
			return reconcile.Result{}, err
		}
	} else {
		// A Deployment with the derived name exists but is not owned by this
		// Viewer. Do not touch the unrelated workload and, critically, do not
		// publish a Service/route that could point at it. Stop here instead.
		err := fmt.Errorf(
			"deployment %s/%s already exists and is not owned by viewer %q; refusing to publish a route to it",
			foundDpl.Namespace, foundDpl.Name, view.Name)
		utilruntime.HandleError(err)
		return reconcile.Result{}, err
	}

	// Set up a service for the deployment above.
	svc := serviceFrom(view, dpl.Name)
	// Set the service to be owned by the view instance as well.
	if err := controllerutil.SetControllerReference(view, svc, r.scheme); err != nil {
		// Error means that the service is already owned by some other instance.
		utilruntime.HandleError(err)
		return reconcile.Result{}, err
	}

	foundSvc := &corev1.Service{}
	nsn = types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}
	if err := r.Client.Get(context.Background(), nsn, foundSvc); err != nil {
		if errors.IsNotFound(err) {
			// Create a new instance.
			if createErr := r.Client.Create(context.Background(), svc); createErr != nil {
				utilruntime.HandleError(fmt.Errorf("error creating service: %v", createErr))
				return reconcile.Result{}, createErr
			}
			glog.Infof("Created new service with spec: %+v", svc)
		} else {
			// Some other error.
			utilruntime.HandleError(err)
			return reconcile.Result{}, err
		}
	} else if metav1.IsControlledBy(foundSvc, view) {
		// Reconcile the route-defining fields of a Service this Viewer owns so a
		// drifted selector or mapping is corrected.
		if err := r.reconcileExistingService(foundSvc, svc); err != nil {
			utilruntime.HandleError(fmt.Errorf("error reconciling existing service: %v", err))
			return reconcile.Result{}, err
		}
	} else {
		// A Service with the derived name exists but is not owned by this Viewer.
		// Do not treat the Viewer as reconciled against a route we do not control.
		err := fmt.Errorf(
			"service %s/%s already exists and is not owned by viewer %q; refusing to publish a route to it",
			foundSvc.Namespace, foundSvc.Name, view.Name)
		utilruntime.HandleError(err)
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

// reconcileExistingService brings the route-defining fields of a Viewer-owned
// Service (selector, ports, and the Ambassador mapping annotation) back to the
// desired state, updating only when one of them has drifted so already-correct
// Services are not needlessly rewritten.
func (r *Reconciler) reconcileExistingService(found, desired *corev1.Service) error {
	changed := false
	if !equality.Semantic.DeepEqual(found.Spec.Selector, desired.Spec.Selector) {
		found.Spec.Selector = desired.Spec.Selector
		changed = true
	}
	if !equality.Semantic.DeepEqual(found.Spec.Ports, desired.Spec.Ports) {
		found.Spec.Ports = desired.Spec.Ports
		changed = true
	}
	const ambassadorAnnotation = "getambassador.io/config"
	if found.Annotations[ambassadorAnnotation] != desired.Annotations[ambassadorAnnotation] {
		if found.Annotations == nil {
			found.Annotations = map[string]string{}
		}
		found.Annotations[ambassadorAnnotation] = desired.Annotations[ambassadorAnnotation]
		changed = true
	}
	if !changed {
		return nil
	}
	glog.Infof("Reconciling existing viewer service %s/%s", found.Namespace, found.Name)
	return r.Update(context.Background(), found)
}

func setPodSpecForTensorboard(view *viewerV1beta1.Viewer, s *corev1.PodSpec) {
	if len(s.Containers) == 0 {
		s.Containers = append(s.Containers, corev1.Container{})
	}

	c := &s.Containers[0]
	c.Name = view.Name + "-pod"
	c.Image = view.Spec.TensorboardSpec.TensorflowImage
	// Pin the entrypoint to tensorboard so a caller-chosen image cannot run its
	// own entrypoint; hardenViewerContainers re-affirms this on every path.
	c.Command = []string{tensorboardCommand}
	c.Args = []string{
		fmt.Sprintf("--logdir=%s", view.Spec.TensorboardSpec.LogDir),
		fmt.Sprintf("--path_prefix=/tensorboard/%s/", view.Name),
	}
	isTensorflowV1 := false
	parts := strings.Split(view.Spec.TensorboardSpec.TensorflowImage, ":")
	// an image might not contain a tag
	if len(parts) == 2 {
		tfImageVersion := parts[1]
		if strings.HasPrefix(tfImageVersion, `1.`) {
			isTensorflowV1 = true
		}
	}
	if !isTensorflowV1 {
		// This is needed for tf 2.0.
		// https://github.com/kubeflow/pipelines/issues/2440
		c.Args = append(c.Args, "--bind_all")
	}

	c.Ports = []corev1.ContainerPort{
		{ContainerPort: viewerTargetPort},
	}

}

func deploymentFrom(view *viewerV1beta1.Viewer) (*appsv1.Deployment, error) {
	name := view.Name + "-deployment"
	dpl := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: view.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"deployment": name,
					"app":        "viewer",
					"viewer":     view.Name,
				},
			},
			Template: view.Spec.PodTemplateSpec,
		},
	}

	// Add label so we can select this deployment with a service.
	if dpl.Spec.Template.ObjectMeta.Labels == nil {
		dpl.Spec.Template.ObjectMeta.Labels = make(map[string]string)
	}
	dpl.Spec.Template.ObjectMeta.Labels["deployment"] = name
	dpl.Spec.Template.ObjectMeta.Labels["app"] = "viewer"
	dpl.Spec.Template.ObjectMeta.Labels["viewer"] = view.Name

	switch view.Spec.Type {
	case viewerV1beta1.ViewerTypeTensorboard:
		setPodSpecForTensorboard(view, &dpl.Spec.Template.Spec)
	default:
		return nil, fmt.Errorf("unknown viewer type: %q", view.Spec.Type)
	}

	hardenViewerPodSpec(&dpl.Spec.Template.Spec)

	return dpl, nil
}

func boolPtr(v bool) *bool { return &v }

// hardenExistingDeployment re-applies the pod-spec hardening to a Deployment
// that already exists for a Viewer and updates it only when the hardening
// actually changed a field. This repairs Deployments created before the
// hardening was in place (or by an older controller) without triggering
// pointless rollouts on already-safe Deployments.
func (r *Reconciler) hardenExistingDeployment(dpl *appsv1.Deployment) error {
	original := dpl.Spec.Template.Spec.DeepCopy()
	hardenViewerPodSpec(&dpl.Spec.Template.Spec)
	if equality.Semantic.DeepEqual(original, &dpl.Spec.Template.Spec) {
		return nil
	}
	glog.Infof("Re-hardening existing viewer deployment %s/%s", dpl.Namespace, dpl.Name)
	return r.Update(context.Background(), dpl)
}

// hardenViewerPodSpec neutralizes the security-sensitive fields of a
// caller-supplied pod template. Anyone able to create a Viewer controls
// spec.podTemplateSpec verbatim, and the controller renders it into a
// Deployment in the caller's namespace, so an unsanitized template would let a
// namespace-scoped user escalate onto the node (host-path volumes, host ports,
// privileged or capability-granting containers, host namespaces, Windows
// HostProcess containers), assume another identity (serviceAccountName), or run
// arbitrary code via extra/init containers, lifecycle hooks, or exec probes —
// privileges that the pipeline pod hardening otherwise denies. A viewer only
// ever runs a single TensorBoard container, so caller-supplied init and
// additional containers are dropped; ordinary fields on the remaining container
// (persistent-volume mounts, resources, env) are left untouched.
//
// Threat model: the container image is intentionally caller-selectable (users
// choose a TensorBoard/TensorFlow version, and the UI exposes it). Running a
// chosen image in this hardened, non-privileged pod under the namespace-default
// service account is equivalent to what the tenant can already do through
// pipeline runs and Argo workflows, so it is not an escalation and this function
// does not attempt to restrict the image. The purpose here is to prevent
// escalation BEYOND that baseline: host access, identity assumption, and
// arbitrary sidecar/lifecycle/probe execution.
func hardenViewerPodSpec(spec *corev1.PodSpec) {
	spec.HostNetwork = false
	spec.HostPID = false
	spec.HostIPC = false
	// Force the namespace default service account so a viewer cannot run under a
	// more privileged identity, and do not mount its token: TensorBoard does not
	// call the Kubernetes API, and the caller-selectable image could otherwise
	// read or exfiltrate the default service account token.
	spec.ServiceAccountName = ""
	spec.DeprecatedServiceAccount = ""
	spec.AutomountServiceAccountToken = boolPtr(false)
	// Drop pod-level Windows options (e.g. HostProcess) that grant host access.
	if spec.SecurityContext != nil {
		spec.SecurityContext.WindowsOptions = nil
	}
	// A viewer serves a single TensorBoard container. Drop caller-supplied init
	// and extra containers so they cannot run arbitrary images or commands.
	spec.InitContainers = nil
	if len(spec.Containers) > 1 {
		spec.Containers = spec.Containers[:1]
	}

	droppedVolumes := make(map[string]bool)
	keptVolumes := spec.Volumes[:0]
	for i := range spec.Volumes {
		v := spec.Volumes[i]
		// Host-path volumes expose the node filesystem.
		if v.HostPath != nil {
			droppedVolumes[v.Name] = true
			continue
		}
		// Strip service-account-token projections so the caller cannot mount the
		// default SA token even though automount is disabled. If that empties a
		// projected volume, drop it entirely.
		if v.Projected != nil {
			v.Projected.Sources = dropServiceAccountTokenSources(v.Projected.Sources)
			if len(v.Projected.Sources) == 0 {
				droppedVolumes[v.Name] = true
				continue
			}
		}
		keptVolumes = append(keptVolumes, v)
	}
	spec.Volumes = keptVolumes
	if len(spec.Volumes) == 0 {
		spec.Volumes = nil
	}

	hardenViewerContainers(spec.InitContainers, droppedVolumes)
	hardenViewerContainers(spec.Containers, droppedVolumes)
}

// dropServiceAccountTokenSources removes ServiceAccountToken projections from a
// projected volume's source list, leaving any other (configMap, secret,
// downwardAPI) sources intact.
func dropServiceAccountTokenSources(sources []corev1.VolumeProjection) []corev1.VolumeProjection {
	kept := sources[:0]
	for _, s := range sources {
		if s.ServiceAccountToken != nil {
			continue
		}
		kept = append(kept, s)
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}

func hardenViewerContainers(containers []corev1.Container, droppedVolumes map[string]bool) {
	for i := range containers {
		c := &containers[i]
		// Enforce the escalation-preventing settings on every container, even one
		// that supplied no securityContext, so Kubernetes' permissive defaults
		// cannot leave privilege escalation enabled.
		if c.SecurityContext == nil {
			c.SecurityContext = &corev1.SecurityContext{}
		}
		sc := c.SecurityContext
		sc.Privileged = boolPtr(false)
		sc.AllowPrivilegeEscalation = boolPtr(false)
		sc.ProcMount = nil
		// Windows HostProcess containers share the host; deny them.
		sc.WindowsOptions = nil
		// Strip any added Linux capabilities while preserving defensive drops the
		// template may have declared (e.g. drop: ["ALL"]).
		if sc.Capabilities != nil {
			sc.Capabilities.Add = nil
			if len(sc.Capabilities.Drop) == 0 {
				sc.Capabilities = nil
			}
		}
		// Force the tensorboard entrypoint, overriding any caller-supplied command
		// (defense-in-depth; the image itself is caller-selectable, see the
		// threat model on hardenViewerPodSpec), and drop lifecycle hooks and
		// probes, any of which can carry an exec handler that would run beyond the
		// pinned command. This also repairs Deployments created before this
		// hardening was in place. A leading "tensorboard" arg left by older
		// controllers (which passed it as args[0]) is trimmed to avoid duplication.
		c.Command = []string{tensorboardCommand}
		if len(c.Args) > 0 && c.Args[0] == tensorboardCommand {
			c.Args = c.Args[1:]
		}
		c.Lifecycle = nil
		c.LivenessProbe = nil
		c.ReadinessProbe = nil
		c.StartupProbe = nil
		// Host ports bind on the node even when host networking is disabled.
		for j := range c.Ports {
			c.Ports[j].HostPort = 0
		}
		if len(droppedVolumes) == 0 {
			continue
		}
		c.VolumeMounts = filterVolumeMounts(c.VolumeMounts, droppedVolumes)
		c.VolumeDevices = filterVolumeDevices(c.VolumeDevices, droppedVolumes)
	}
}

func filterVolumeMounts(mounts []corev1.VolumeMount, dropped map[string]bool) []corev1.VolumeMount {
	kept := mounts[:0]
	for _, m := range mounts {
		if dropped[m.Name] {
			continue
		}
		kept = append(kept, m)
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}

func filterVolumeDevices(devices []corev1.VolumeDevice, dropped map[string]bool) []corev1.VolumeDevice {
	kept := devices[:0]
	for _, d := range devices {
		if dropped[d.Name] {
			continue
		}
		kept = append(kept, d)
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}

const mappingTpl = `
---
apiVersion: ambassador/v0
kind: Mapping
name: viewer-mapping-%s
prefix: %s
rewrite: %s
service: %s`

func serviceFrom(v *viewerV1beta1.Viewer, deploymentName string) *corev1.Service {
	name := v.Name + "-service"
	path := fmt.Sprintf("/%s/%s/", v.Spec.Type, v.Name)
	mapping := fmt.Sprintf(mappingTpl, v.Name, path, path, name)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   v.Namespace,
			Annotations: map[string]string{"getambassador.io/config": mapping},
			Labels: map[string]string{
				"app":    "viewer",
				"viewer": v.Name,
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"deployment": deploymentName,
				"app":        "viewer",
				"viewer":     v.Name,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Protocol:   corev1.ProtocolTCP,
					Port:       80,
					TargetPort: intstr.IntOrString{IntVal: viewerTargetPort}},
			},
		},
	}
}

func (r *Reconciler) maybeDeleteOldestViewer(t viewerV1beta1.ViewerType, namespace string) error {
	list := &viewerV1beta1.ViewerList{}

	if err := r.Client.List(context.Background(), list, &client.ListOptions{Namespace: namespace}); err != nil {
		return fmt.Errorf("failed to list viewers: %v", err)
	}

	if len(list.Items) <= r.opts.MaxNumViewers {
		return nil
	}

	// Delete the oldest viewer by creation timestamp.
	oldest := &list.Items[0] // MaxNumViewers must be at least one.
	for i := range list.Items {
		if list.Items[i].CreationTimestamp.Time.Before(oldest.CreationTimestamp.Time) {
			oldest = &list.Items[i]
		}
	}

	return r.Client.Delete(context.Background(), oldest)
}
