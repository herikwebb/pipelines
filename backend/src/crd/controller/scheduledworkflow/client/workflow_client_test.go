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

package client

import (
	"testing"
	"time"

	workflowapi "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	workflowcommon "github.com/argoproj/argo-workflows/v4/workflow/common"
	"github.com/kubeflow/pipelines/backend/src/common"
	commonutil "github.com/kubeflow/pipelines/backend/src/common/util"
	swfapi "github.com/kubeflow/pipelines/backend/src/crd/pkg/apis/scheduledworkflow/v1beta1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/tools/cache"
)

func TestToWorkflowStatuses(t *testing.T) {
	workflow := commonutil.NewWorkflow(&workflowapi.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "WORKFLOW_NAME",
			Namespace:         "NAMESPACE",
			SelfLink:          "SELF_LINK",
			UID:               "UID",
			CreationTimestamp: metav1.NewTime(time.Unix(50, 0).UTC()),
			Labels: map[string]string{
				commonutil.LabelKeyWorkflowEpoch: "54",
				commonutil.LabelKeyWorkflowIndex: "55",
			},
		},
		Status: workflowapi.WorkflowStatus{
			Phase:      workflowapi.WorkflowRunning,
			Message:    "WORKFLOW_MESSAGE",
			StartedAt:  metav1.NewTime(time.Unix(51, 0).UTC()),
			FinishedAt: metav1.NewTime(time.Unix(52, 0).UTC()),
		},
	})

	result := toWorkflowStatuses(commonutil.ExecutionSpecList{workflow})

	expected := &swfapi.WorkflowStatus{
		Name:        "WORKFLOW_NAME",
		Namespace:   "NAMESPACE",
		SelfLink:    "SELF_LINK",
		UID:         "UID",
		Phase:       common.ExecutionPhase(workflowapi.WorkflowRunning),
		Message:     "WORKFLOW_MESSAGE",
		CreatedAt:   metav1.NewTime(time.Unix(50, 0).UTC()),
		StartedAt:   metav1.NewTime(time.Unix(51, 0).UTC()),
		FinishedAt:  metav1.NewTime(time.Unix(52, 0).UTC()),
		ScheduledAt: metav1.NewTime(time.Unix(54, 0).UTC()),
		Index:       55,
	}

	assert.Equal(t, []swfapi.WorkflowStatus{*expected}, result)
}

func TestToWorkflowStatuses_NullOrEmpty(t *testing.T) {
	workflow := commonutil.NewWorkflow(&workflowapi.Workflow{})

	result := toWorkflowStatuses(commonutil.ExecutionSpecList{workflow})

	expected := &swfapi.WorkflowStatus{
		Name:        "",
		Namespace:   "",
		SelfLink:    "",
		UID:         "",
		Phase:       "",
		Message:     "",
		CreatedAt:   metav1.NewTime(time.Time{}.UTC()),
		StartedAt:   metav1.NewTime(time.Time{}.UTC()),
		FinishedAt:  metav1.NewTime(time.Time{}.UTC()),
		ScheduledAt: metav1.NewTime(time.Time{}.UTC()),
		Index:       0,
	}

	assert.Equal(t, []swfapi.WorkflowStatus{*expected}, result)
}

func TestRetrieveScheduledTime(t *testing.T) {

	// Base case.
	workflow := commonutil.NewWorkflow(&workflowapi.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.NewTime(time.Unix(50, 0).UTC()),
			Labels: map[string]string{
				commonutil.LabelKeyWorkflowEpoch: "54",
			},
		},
	})
	result := retrieveScheduledTime(workflow)
	assert.Equal(t, metav1.NewTime(time.Unix(54, 0).UTC()), result)

	// No label
	workflow = commonutil.NewWorkflow(&workflowapi.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.NewTime(time.Unix(50, 0).UTC()),
			Labels: map[string]string{
				"WRONG_LABEL": "54",
			},
		},
	})
	result = retrieveScheduledTime(workflow)
	assert.Equal(t, metav1.NewTime(time.Unix(50, 0).UTC()), result)

	// Parsing problem
	workflow = commonutil.NewWorkflow(&workflowapi.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.NewTime(time.Unix(50, 0).UTC()),
			Labels: map[string]string{
				commonutil.LabelKeyWorkflowEpoch: "UNPARSABLE_@%^%@^#%",
			},
		},
	})
	result = retrieveScheduledTime(workflow)
	assert.Equal(t, metav1.NewTime(time.Unix(50, 0).UTC()), result)
}

func TestRetrieveIndex(t *testing.T) {

	// Base case.
	workflow := commonutil.NewWorkflow(&workflowapi.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				commonutil.LabelKeyWorkflowIndex: "100",
			},
		},
	})
	result := retrieveIndex(workflow)
	assert.Equal(t, int64(100), result)

	// No label
	workflow = commonutil.NewWorkflow(&workflowapi.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"WRONG_LABEL": "100",
			},
		},
	})
	result = retrieveIndex(workflow)
	assert.Equal(t, int64(0), result)

	// Parsing problem
	workflow = commonutil.NewWorkflow(&workflowapi.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				commonutil.LabelKeyWorkflowIndex: "UNPARSABLE_LABEL_!@^^!%@#%",
			},
		},
	})
	result = retrieveIndex(workflow)
	assert.Equal(t, int64(0), result)
}

func TestLabelSelectorToGetWorkflows(t *testing.T) {

	// Completed
	result := getLabelSelectorToGetWorkflows(
		"PIPELINE_NAME",
		true, /* completed */
		50 /* min index */)

	expected := labels.NewSelector()

	req, err := labels.NewRequirement(workflowcommon.LabelKeyCompleted, selection.Equals,
		[]string{"true"})
	assert.Nil(t, err)
	expected = expected.Add(*req)

	req, err = labels.NewRequirement(commonutil.LabelKeyWorkflowScheduledWorkflowName, selection.Equals,
		[]string{"PIPELINE_NAME"})
	assert.Nil(t, err)
	expected = expected.Add(*req)

	req, err = labels.NewRequirement(commonutil.LabelKeyWorkflowIndex, selection.GreaterThan,
		[]string{"50"})
	assert.Nil(t, err)
	expected = expected.Add(*req)

	assert.Equal(t, expected, *result)

	// Not completed
	result = getLabelSelectorToGetWorkflows(
		"PIPELINE_NAME",
		false, /* completed */
		50 /* min index */)

	expected = labels.NewSelector()

	req, err = labels.NewRequirement(workflowcommon.LabelKeyCompleted, selection.NotEquals,
		[]string{"true"})
	assert.Nil(t, err)
	expected = expected.Add(*req)

	req, err = labels.NewRequirement(commonutil.LabelKeyWorkflowScheduledWorkflowName, selection.Equals,
		[]string{"PIPELINE_NAME"})
	assert.Nil(t, err)
	expected = expected.Add(*req)

	req, err = labels.NewRequirement(commonutil.LabelKeyWorkflowIndex, selection.GreaterThan,
		[]string{"50"})
	assert.Nil(t, err)
	expected = expected.Add(*req)

	assert.Equal(t, expected, *result)
}

// namespaceFilteringInformer is a fake ExecutionInformer that mimics a
// namespaced lister: List only returns seeded workflows whose namespace matches
// the requested namespace and whose labels match the selector.
type namespaceFilteringInformer struct {
	workflows commonutil.ExecutionSpecList
}

func (f *namespaceFilteringInformer) AddEventHandler(funcs cache.ResourceEventHandler) (
	cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (f *namespaceFilteringInformer) HasSynced() func() bool {
	return func() bool { return true }
}

func (f *namespaceFilteringInformer) Get(namespace string, name string) (commonutil.ExecutionSpec, bool, error) {
	return nil, true, nil
}

func (f *namespaceFilteringInformer) List(namespace string, selector *labels.Selector) (
	commonutil.ExecutionSpecList, error) {
	result := commonutil.ExecutionSpecList{}
	for _, wf := range f.workflows {
		if wf.ExecutionNamespace() != namespace {
			continue
		}
		if !(*selector).Matches(labels.Set(wf.ExecutionObjectMeta().Labels)) {
			continue
		}
		result = append(result, wf)
	}
	return result, nil
}

func (f *namespaceFilteringInformer) InformerFactoryStart(stopCh <-chan struct{}) {}

var _ commonutil.ExecutionInformer = &namespaceFilteringInformer{}

// TestList_ScopedToNamespace asserts that two ScheduledWorkflows sharing the
// same name in different namespaces do not count each other's workflows: List
// must only return workflows from the requested namespace, even when workflows
// in another namespace carry identical scheduledWorkflowName/workflowIndex
// labels.
func TestList_ScopedToNamespace(t *testing.T) {
	sharedLabels := map[string]string{
		commonutil.LabelKeyWorkflowScheduledWorkflowName: "SHARED_NAME",
		commonutil.LabelKeyWorkflowIndex:                 "1",
	}
	workflowInNamespace := func(name, namespace string) commonutil.ExecutionSpec {
		return commonutil.NewWorkflow(&workflowapi.Workflow{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Labels:    sharedLabels,
			},
		})
	}
	informer := &namespaceFilteringInformer{
		workflows: commonutil.ExecutionSpecList{
			workflowInNamespace("victim-run", "ns-a"),
			workflowInNamespace("attacker-decoy", "ns-b"),
		},
	}
	workflowClient := NewWorkflowClient(nil, informer)

	// ns-a only sees its own workflow, not the identically labeled decoy in ns-b.
	nsAResult, err := workflowClient.List("ns-a", "SHARED_NAME", false /* completed */, 0 /* minIndex */)
	assert.Nil(t, err)
	assert.Len(t, nsAResult, 1)
	assert.Equal(t, "victim-run", nsAResult[0].Name)

	// ns-b likewise only sees its own workflow.
	nsBResult, err := workflowClient.List("ns-b", "SHARED_NAME", false, 0)
	assert.Nil(t, err)
	assert.Len(t, nsBResult, 1)
	assert.Equal(t, "attacker-decoy", nsBResult[0].Name)
}
