// Copyright 2020 The Kubeflow Authors
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

package server

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/kubeflow/pipelines/backend/src/cache/model"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/api/admission/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	fakePod = &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				ArgoWorkflowNodeName: "test_node",
			},
			Labels: map[string]string{
				ArgoCompleteLabelKey:    "true",
				KFPCacheEnabledLabelKey: KFPCacheEnabledLabelValue,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    "main",
					Image:   "test_image",
					Command: []string{"python"},
					Env: []corev1.EnvVar{{
						Name:  ArgoWorkflowTemplateEnvKey,
						Value: `{"name": "Does not matter","container":{"command":["echo", "Hello"],"image":"python:3.11"}}`,
					}},
				},
			},
			NodeSelector: map[string]string{"disktype": "ssd"},
		},
	}
	fakeAdmissionRequest = v1beta1.AdmissionRequest{
		UID: "test-12345",
		Kind: metav1.GroupVersionKind{
			Group:   "group",
			Version: "v1",
			Kind:    "k8s",
		},
		Resource: metav1.GroupVersionResource{
			Version:  "v1",
			Resource: "pods",
		},
		SubResource: "subresource",
		Name:        "test",
		Namespace:   "default",
		Operation:   "test",
		Object: runtime.RawExtension{
			Raw: EncodePod(fakePod),
		},
	}
)

func EncodePod(pod *corev1.Pod) []byte {
	reqBodyBytes := new(bytes.Buffer)
	json.NewEncoder(reqBodyBytes).Encode(*pod)

	return reqBodyBytes.Bytes()
}

func GetFakeRequestFromPod(pod *corev1.Pod) *v1beta1.AdmissionRequest {
	fakeRequest := fakeAdmissionRequest
	fakeRequest.Object.Raw = EncodePod(pod)
	return &fakeRequest
}

func newFakeClientManagerForTest(t *testing.T) *FakeClientManager {
	t.Helper()
	clientManager := NewFakeClientManagerOrFatal(util.NewFakeTimeForEpoch())
	t.Cleanup(func() { require.NoError(t, clientManager.Close()) })
	return clientManager
}

func cacheKeyForPod(t *testing.T, pod *corev1.Pod, namespace string) string {
	t.Helper()
	template, found := getArgoTemplate(pod)
	require.True(t, found)
	key, err := generateCacheKeyFromTemplate(template, namespace)
	require.NoError(t, err)
	return key
}

func TestMain(m *testing.M) {
	os.Setenv("CACHE_NODE_RESTRICTIONS", "true")
	defer os.Unsetenv("CACHE_NODE_RESTRICTIONS")
	code := m.Run()
	os.Exit(code)
}

func TestMutatePodIfCachedWithErrorPodResource(t *testing.T) {
	mockAdmissionRequest := &v1beta1.AdmissionRequest{
		Resource: metav1.GroupVersionResource{
			Version: "wrong", Resource: "wrong",
		},
	}
	patchOperations, err := MutatePodIfCached(mockAdmissionRequest, fakeClientManager)
	assert.Nil(t, patchOperations)
	assert.Nil(t, err)
}

func TestMutatePodIfCachedWithDecodeError(t *testing.T) {
	invalidAdmissionRequest := fakeAdmissionRequest
	invalidAdmissionRequest.Object.Raw = []byte{5, 5}
	patchOperation, err := MutatePodIfCached(&invalidAdmissionRequest, fakeClientManager)
	assert.Nil(t, patchOperation)
	assert.Contains(t, err.Error(), "could not deserialize pod object")
}

func TestMutatePodIfCachedWithCacheDisabledPod(t *testing.T) {
	cacheDisabledPod := *fakePod.DeepCopy()
	cacheDisabledPod.ObjectMeta.Labels[KFPCacheEnabledLabelKey] = "false"
	patchOperation, err := MutatePodIfCached(GetFakeRequestFromPod(&cacheDisabledPod), fakeClientManager)
	assert.Nil(t, patchOperation)
	assert.Nil(t, err)
}

func TestMutatePodIfCachedWithTFXPod(t *testing.T) {
	tfxPod := *fakePod.DeepCopy()
	mainContainerCommand := append(tfxPod.Spec.Containers[0].Command, "/tfx-src/"+TFXPodSuffix)
	tfxPod.Spec.Containers[0].Command = mainContainerCommand
	patchOperation, err := MutatePodIfCached(GetFakeRequestFromPod(&tfxPod), fakeClientManager)
	assert.Nil(t, patchOperation)
	assert.Nil(t, err)
}

func TestMutatePodIfCachedWithTFXPod2(t *testing.T) {
	tfxPod := *fakePod.DeepCopy()
	tfxPod.Labels["pipelines.kubeflow.org/pipeline-sdk-type"] = "tfx"
	patchOperation, err := MutatePodIfCached(GetFakeRequestFromPod(&tfxPod), fakeClientManager)
	assert.Nil(t, patchOperation)
	assert.Nil(t, err)
	// test variation 2
	tfxPod = *fakePod.DeepCopy()
	tfxPod.Labels["pipelines.kubeflow.org/pipeline-sdk-type"] = "tfx-template"
	patchOperation, err = MutatePodIfCached(GetFakeRequestFromPod(&tfxPod), fakeClientManager)
	assert.Nil(t, patchOperation)
	assert.Nil(t, err)
}

func TestMutatePodIfCachedWithKfpV2Pod(t *testing.T) {
	v2Pod := *fakePod.DeepCopy()
	v2Pod.Annotations["pipelines.kubeflow.org/v2_component"] = "true"
	patchOperation, err := MutatePodIfCached(GetFakeRequestFromPod(&v2Pod), fakeClientManager)
	assert.Nil(t, patchOperation)
	assert.Nil(t, err)
}

func TestMutatePodIfCached(t *testing.T) {
	patchOperation, err := MutatePodIfCached(&fakeAdmissionRequest, fakeClientManager)
	assert.Nil(t, err)
	require.NotNil(t, patchOperation)
	require.Equal(t, 2, len(patchOperation))
	require.Equal(t, patchOperation[0].Op, OperationTypeAdd)
	require.Equal(t, patchOperation[1].Op, OperationTypeAdd)
}

func TestMutatePodIfCachedUsesPodNamespaceWhenRequestNamespaceEmpty(t *testing.T) {
	pod := fakePod.DeepCopy()
	pod.Namespace = "pod-namespace"
	request := GetFakeRequestFromPod(pod)
	request.Namespace = ""

	patchOperations, err := MutatePodIfCached(request, newFakeClientManagerForTest(t))
	require.NoError(t, err)
	require.Len(t, patchOperations, 2)
	annotations, ok := patchOperations[0].Value.(map[string]string)
	require.True(t, ok)
	require.Equal(t, cacheKeyForPod(t, pod, pod.Namespace), annotations[ExecutionKey])
}

func TestMutatePodIfCachedSkipsPodWithoutNamespace(t *testing.T) {
	pod := fakePod.DeepCopy()
	pod.Namespace = ""
	request := GetFakeRequestFromPod(pod)
	request.Namespace = ""

	patchOperations, err := MutatePodIfCached(request, newFakeClientManagerForTest(t))
	require.NoError(t, err)
	require.Empty(t, patchOperations)
}

func TestMutatePodIfCachedWithCacheEntryExist(t *testing.T) {
	clientManager := newFakeClientManagerForTest(t)
	executionCache := &model.ExecutionCache{
		ExecutionCacheKey: cacheKeyForPod(t, fakePod, fakeAdmissionRequest.Namespace),
		ExecutionOutput:   "testOutput",
		ExecutionTemplate: `{"container":{"command":["echo", "Hello"],"image":"python:3.11"}}`,
		MaxCacheStaleness: -1,
	}
	_, err := clientManager.CacheStore().CreateExecutionCache(executionCache)
	require.NoError(t, err)

	patchOperation, err := MutatePodIfCached(&fakeAdmissionRequest, clientManager)
	assert.Nil(t, err)

	require.NotNil(t, patchOperation)
	require.Equal(t, 3, len(patchOperation))
	require.Equal(t, patchOperation[0].Op, OperationTypeReplace)
	require.Equal(t, patchOperation[1].Op, OperationTypeAdd)
	require.Equal(t, patchOperation[2].Op, OperationTypeAdd)
}

func TestDefaultImage(t *testing.T) {
	clientManager := newFakeClientManagerForTest(t)
	executionCache := &model.ExecutionCache{
		ExecutionCacheKey: cacheKeyForPod(t, fakePod, fakeAdmissionRequest.Namespace),
		ExecutionOutput:   "testOutput",
		ExecutionTemplate: `{"container":{"command":["echo", "Hello"],"image":"python:3.11"}}`,
		MaxCacheStaleness: -1,
	}
	_, err := clientManager.CacheStore().CreateExecutionCache(executionCache)
	require.NoError(t, err)

	patchOperation, err := MutatePodIfCached(&fakeAdmissionRequest, clientManager)
	assert.Nil(t, err)
	container := patchOperation[0].Value.([]corev1.Container)[0]
	require.Equal(t, "ghcr.io/containerd/busybox", container.Image)
}

func TestSetImage(t *testing.T) {
	testImage := "testimage"
	os.Setenv("CACHE_IMAGE", testImage)
	defer os.Unsetenv("CACHE_IMAGE")

	clientManager := newFakeClientManagerForTest(t)
	executionCache := &model.ExecutionCache{
		ExecutionCacheKey: cacheKeyForPod(t, fakePod, fakeAdmissionRequest.Namespace),
		ExecutionOutput:   "testOutput",
		ExecutionTemplate: `{"container":{"command":["echo", "Hello"],"image":"python:3.11"}}`,
		MaxCacheStaleness: -1,
	}
	_, err := clientManager.CacheStore().CreateExecutionCache(executionCache)
	require.NoError(t, err)

	patchOperation, err := MutatePodIfCached(&fakeAdmissionRequest, clientManager)
	assert.Nil(t, err)
	container := patchOperation[0].Value.([]corev1.Container)[0]
	assert.Equal(t, testImage, container.Image)
}

func TestCacheNodeRestriction(t *testing.T) {
	os.Setenv("CACHE_NODE_RESTRICTIONS", "false")

	clientManager := newFakeClientManagerForTest(t)
	executionCache := &model.ExecutionCache{
		ExecutionCacheKey: cacheKeyForPod(t, fakePod, fakeAdmissionRequest.Namespace),
		ExecutionOutput:   "testOutput",
		ExecutionTemplate: `{"container":{"command":["echo", "Hello"],"image":"python:3.11"},"nodeSelector":{"disktype":"ssd"}}`,
		MaxCacheStaleness: -1,
	}
	_, err := clientManager.CacheStore().CreateExecutionCache(executionCache)
	require.NoError(t, err)
	patchOperation, err := MutatePodIfCached(&fakeAdmissionRequest, clientManager)
	assert.Nil(t, err)
	assert.Equal(t, OperationTypeRemove, patchOperation[1].Op)
	assert.Nil(t, patchOperation[1].Value)
	os.Setenv("CACHE_NODE_RESTRICTIONS", "true")
}

func TestMutatePodIfCachedWithTeamplateCleanup(t *testing.T) {
	clientManager := newFakeClientManagerForTest(t)
	pod := *fakePod.DeepCopy()
	pod.Spec.Containers[0].Env = []corev1.EnvVar{{
		Name: ArgoWorkflowTemplateEnvKey,
		Value: `{
			"name": "Does not matter",
			"metadata": "anything",
			"container": {
				"image": "python:3.11",
				"command": ["echo", "Hello"]
			},
			"outputs": "anything",
			"foo": "bar"
		}`,
	}}
	request := GetFakeRequestFromPod(&pod)
	executionCache := &model.ExecutionCache{
		ExecutionCacheKey: cacheKeyForPod(t, &pod, request.Namespace),
		ExecutionOutput:   "testOutput",
		ExecutionTemplate: "test template",
		MaxCacheStaleness: -1,
	}
	_, err := clientManager.CacheStore().CreateExecutionCache(executionCache)
	require.NoError(t, err)

	patchOperation, err := MutatePodIfCached(request, clientManager)
	assert.Nil(t, err)
	require.NotNil(t, patchOperation)
	require.Equal(t, 3, len(patchOperation))
	require.Equal(t, patchOperation[0].Op, OperationTypeReplace)
	require.Equal(t, patchOperation[1].Op, OperationTypeAdd)
	require.Equal(t, patchOperation[2].Op, OperationTypeAdd)
}

func TestMutatePodIfCachedIsNamespaceScoped(t *testing.T) {
	clientManager := newFakeClientManagerForTest(t)
	template := `{
		"name": "Does not matter",
		"metadata": "anything",
		"container": {
			"image": "python:3.11",
			"command": ["echo", "Hello"]
		},
		"outputs": "anything",
		"foo": "bar"
	}`
	defaultKey, err := generateCacheKeyFromTemplate(template, "default")
	require.NoError(t, err)
	tenantKey, err := generateCacheKeyFromTemplate(template, "tenant-b")
	require.NoError(t, err)
	require.NotEqual(t, defaultKey, tenantKey)

	executionCache := &model.ExecutionCache{
		ExecutionCacheKey: defaultKey,
		ExecutionOutput:   "victimOutput",
		ExecutionTemplate: `namespace "default"`,
		MaxCacheStaleness: -1,
	}
	_, err = clientManager.CacheStore().CreateExecutionCache(executionCache)
	require.NoError(t, err)

	pod := *fakePod.DeepCopy()
	pod.Spec.Containers[0].Env = []corev1.EnvVar{{
		Name:  ArgoWorkflowTemplateEnvKey,
		Value: template,
	}}

	defaultRequest := GetFakeRequestFromPod(&pod)
	defaultRequest.Namespace = "default"
	defaultPatches, err := MutatePodIfCached(defaultRequest, clientManager)
	require.NoError(t, err)
	require.Len(t, defaultPatches, 3)
	require.Equal(t, OperationTypeReplace, defaultPatches[0].Op)

	tenantRequest := GetFakeRequestFromPod(&pod)
	tenantRequest.Namespace = "tenant-b"
	patchOperations, err := MutatePodIfCached(tenantRequest, clientManager)
	require.NoError(t, err)
	require.Len(t, patchOperations, 2)
	for _, operation := range patchOperations {
		require.NotEqual(t, OperationTypeReplace, operation.Op,
			"cross-namespace cache hit: attacker in tenant-b received default's cached execution")
	}
	annotations, ok := patchOperations[0].Value.(map[string]string)
	require.True(t, ok)
	require.Equal(t, tenantKey, annotations[ExecutionKey])
}
