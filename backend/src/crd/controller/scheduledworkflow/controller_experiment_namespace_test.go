// Copyright 2026 The Kubeflow Authors
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

package main

import (
	"context"
	"testing"

	api "github.com/kubeflow/pipelines/backend/api/v2beta1/go_client"
	"github.com/kubeflow/pipelines/backend/src/crd/controller/scheduledworkflow/client"
	util "github.com/kubeflow/pipelines/backend/src/crd/controller/scheduledworkflow/util"
	swfapi "github.com/kubeflow/pipelines/backend/src/crd/pkg/apis/scheduledworkflow/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// fakeExperimentServiceClient returns a fixed experiment namespace for
// GetExperiment and records whether it was called.
type fakeExperimentServiceClient struct {
	namespace string
	called    bool
}

func (f *fakeExperimentServiceClient) GetExperiment(ctx context.Context, in *api.GetExperimentRequest, opts ...grpc.CallOption) (*api.Experiment, error) {
	f.called = true
	return &api.Experiment{ExperimentId: in.GetExperimentId(), Namespace: f.namespace}, nil
}

func (f *fakeExperimentServiceClient) CreateExperiment(ctx context.Context, in *api.CreateExperimentRequest, opts ...grpc.CallOption) (*api.Experiment, error) {
	return nil, nil
}

func (f *fakeExperimentServiceClient) ListExperiments(ctx context.Context, in *api.ListExperimentsRequest, opts ...grpc.CallOption) (*api.ListExperimentsResponse, error) {
	return nil, nil
}

func (f *fakeExperimentServiceClient) ArchiveExperiment(ctx context.Context, in *api.ArchiveExperimentRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, nil
}

func (f *fakeExperimentServiceClient) UnarchiveExperiment(ctx context.Context, in *api.UnarchiveExperimentRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, nil
}

func (f *fakeExperimentServiceClient) DeleteExperiment(ctx context.Context, in *api.DeleteExperimentRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, nil
}

// newTestSWFWithExperiment builds a ScheduledWorkflow in the given namespace
// that references the given experiment via the API-server code path.
func newTestSWFWithExperiment(namespace, experimentID string) *util.ScheduledWorkflow {
	return util.NewScheduledWorkflow(&swfapi.ScheduledWorkflow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "kubeflow.org/v1beta1",
			Kind:       "ScheduledWorkflow",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-swf",
			Namespace: namespace,
			UID:       "test-uid",
		},
		Spec: swfapi.ScheduledWorkflowSpec{
			ExperimentId: experimentID,
			PipelineId:   "pipeline-123",
			Workflow:     &swfapi.WorkflowResource{},
		},
	})
}

// TestExperimentNamespace_MatchingNamespaceAllowsRun verifies that when the
// referenced experiment resolves to the ScheduledWorkflow's own namespace, the
// run is created.
func TestExperimentNamespace_MatchingNamespaceAllowsRun(test *testing.T) {
	fakeRun := &fakeRunServiceClient{}
	fakeExp := &fakeExperimentServiceClient{namespace: "tenant-a"}
	controller := &Controller{
		workflowClient:     client.NewWorkflowClient(&fakeExecutionClient{}, &fakeExecutionInformer{}),
		runClient:          fakeRun,
		experimentClient:   fakeExp,
		userIdentityHeader: "kubeflow-userid",
		userIdentityValue:  "system:serviceaccount:kubeflow:ml-pipeline-scheduledworkflow",
	}

	swf := newTestSWFWithExperiment("tenant-a", "experiment-in-tenant-a")
	submitted, _, err := controller.submitNewWorkflowIfNotAlreadySubmitted(
		context.Background(), swf, 100, 200)

	require.NoError(test, err)
	assert.True(test, submitted)
	assert.True(test, fakeExp.called, "expected the experiment namespace to be validated")
	assert.NotNil(test, fakeRun.capturedCtx, "expected a run to be created")
}

// TestExperimentNamespace_MismatchedNamespaceRejectsRun verifies that when the
// referenced experiment belongs to a different namespace than the
// ScheduledWorkflow, no run is created and an error is returned. This is the
// cross-tenant privilege-escalation guard: a tenant that hand-crafts a
// ScheduledWorkflow in their own namespace pointing at another tenant's
// experiment must not cause a run to execute in that other namespace.
func TestExperimentNamespace_MismatchedNamespaceRejectsRun(test *testing.T) {
	fakeRun := &fakeRunServiceClient{}
	fakeExp := &fakeExperimentServiceClient{namespace: "tenant-victim"}
	controller := &Controller{
		workflowClient:     client.NewWorkflowClient(&fakeExecutionClient{}, &fakeExecutionInformer{}),
		runClient:          fakeRun,
		experimentClient:   fakeExp,
		userIdentityHeader: "kubeflow-userid",
		userIdentityValue:  "system:serviceaccount:kubeflow:ml-pipeline-scheduledworkflow",
	}

	swf := newTestSWFWithExperiment("tenant-attacker", "experiment-in-victim")
	submitted, _, err := controller.submitNewWorkflowIfNotAlreadySubmitted(
		context.Background(), swf, 100, 200)

	require.Error(test, err)
	assert.False(test, submitted)
	assert.Contains(test, err.Error(), "does not match")
	assert.Nil(test, fakeRun.capturedCtx, "expected no run to be created for a cross-namespace experiment")
}

// TestExperimentNamespace_SingleUserModeSkipsValidation verifies that when the
// identity header is not configured (single-user mode), the experiment
// namespace validation is skipped and the run is created without consulting the
// experiment client.
func TestExperimentNamespace_SingleUserModeSkipsValidation(test *testing.T) {
	fakeRun := &fakeRunServiceClient{}
	fakeExp := &fakeExperimentServiceClient{namespace: "unused"}
	controller := &Controller{
		workflowClient:   client.NewWorkflowClient(&fakeExecutionClient{}, &fakeExecutionInformer{}),
		runClient:        fakeRun,
		experimentClient: fakeExp,
	}

	swf := newTestSWFWithExperiment("tenant-a", "experiment-in-tenant-a")
	submitted, _, err := controller.submitNewWorkflowIfNotAlreadySubmitted(
		context.Background(), swf, 100, 200)

	require.NoError(test, err)
	assert.True(test, submitted)
	assert.False(test, fakeExp.called, "experiment validation should be skipped in single-user mode")
}
