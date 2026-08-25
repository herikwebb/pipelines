// Copyright 2023 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package component

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	api "github.com/kubeflow/pipelines/backend/api/v1beta1/go_client"
	"github.com/kubeflow/pipelines/backend/src/v2/cacheutils"
	"github.com/kubeflow/pipelines/backend/src/v2/client_manager"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/kubeflow/pipelines/api/v2alpha1/go/pipelinespec"
	"github.com/kubeflow/pipelines/backend/src/v2/metadata"
	"github.com/kubeflow/pipelines/backend/src/v2/objectstore"
	pb "github.com/kubeflow/pipelines/third_party/ml-metadata/go/ml_metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	k8score "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

var addNumbersComponent = &pipelinespec.ComponentSpec{
	Implementation: &pipelinespec.ComponentSpec_ExecutorLabel{ExecutorLabel: "add"},
	InputDefinitions: &pipelinespec.ComponentInputsSpec{
		Parameters: map[string]*pipelinespec.ComponentInputsSpec_ParameterSpec{
			"a": {ParameterType: pipelinespec.ParameterType_NUMBER_INTEGER, DefaultValue: structpb.NewNumberValue(5)},
			"b": {ParameterType: pipelinespec.ParameterType_NUMBER_INTEGER},
		},
	},
	OutputDefinitions: &pipelinespec.ComponentOutputsSpec{
		Parameters: map[string]*pipelinespec.ComponentOutputsSpec_ParameterSpec{
			"Output": {ParameterType: pipelinespec.ParameterType_NUMBER_INTEGER},
		},
	},
}

type launcherTestSignalCause struct {
	signal     syscall.Signal
	receivedAt time.Time
}

func (cause launcherTestSignalCause) Error() string {
	return cause.signal.String()
}

func (cause launcherTestSignalCause) Signal() syscall.Signal {
	return cause.signal
}

func (cause launcherTestSignalCause) ReceivedAt() time.Time {
	return cause.receivedAt
}

type launcherFinalizationMetadataClient struct {
	*metadata.FakeClient
	execution               *metadata.Execution
	cancelExecution         context.CancelCauseFunc
	cancellationCause       error
	finalizationContexts    []launcherFinalizationContextObservation
	publishedExecutionState pb.Execution_State
	publishedParameters     map[string]*structpb.Value
	publishedArtifacts      []*metadata.OutputArtifact
	getDAGErr               error
	updateDAGCalls          int
	cancelOnPrePublish      bool
	cancelOnPublish         bool
	blockPublishUntilCancel bool
	publishError            error
}

type launcherFinalizationContextObservation struct {
	err         error
	hasDeadline bool
}

type launcherCancellationCacheClient struct {
	cacheutils.Client
	cancel context.CancelCauseFunc
	cause  error
	calls  int
}

func (client *launcherCancellationCacheClient) CreateExecutionCache(ctx context.Context, _ *api.Task) error {
	client.calls++
	client.cancel(client.cause)
	return ctx.Err()
}

func (client *launcherFinalizationMetadataClient) GetExecution(context.Context, int64) (*metadata.Execution, error) {
	return client.execution, nil
}

func (client *launcherFinalizationMetadataClient) PrePublishExecution(
	context.Context,
	*metadata.Execution,
	*metadata.ExecutionConfig,
) (*metadata.Execution, error) {
	if client.cancelOnPrePublish {
		client.cancelExecution(client.cancellationCause)
	}
	return client.execution, nil
}

func (client *launcherFinalizationMetadataClient) observeFinalizationContext(ctx context.Context) {
	_, hasDeadline := ctx.Deadline()
	client.finalizationContexts = append(client.finalizationContexts, launcherFinalizationContextObservation{
		err:         ctx.Err(),
		hasDeadline: hasDeadline,
	})
}

func (client *launcherFinalizationMetadataClient) PublishExecution(
	ctx context.Context,
	_ *metadata.Execution,
	parameters map[string]*structpb.Value,
	artifacts []*metadata.OutputArtifact,
	state pb.Execution_State,
) error {
	client.observeFinalizationContext(ctx)
	client.publishedExecutionState = state
	client.publishedParameters = parameters
	client.publishedArtifacts = artifacts
	if client.cancelOnPublish {
		client.cancelExecution(client.cancellationCause)
	}
	if client.blockPublishUntilCancel {
		<-ctx.Done()
		if client.publishError != nil {
			return client.publishError
		}
		return ctx.Err()
	}
	return nil
}

func (client *launcherFinalizationMetadataClient) GetDAG(ctx context.Context, _ int64) (*metadata.DAG, error) {
	client.observeFinalizationContext(ctx)
	if client.getDAGErr != nil {
		return nil, client.getDAGErr
	}
	return &metadata.DAG{}, nil
}

func (client *launcherFinalizationMetadataClient) GetPipelineFromExecution(ctx context.Context, _ int64) (*metadata.Pipeline, error) {
	client.observeFinalizationContext(ctx)
	return &metadata.Pipeline{}, nil
}

func (client *launcherFinalizationMetadataClient) UpdateDAGExecutionsState(
	ctx context.Context,
	_ *metadata.DAG,
	_ *metadata.Pipeline,
) error {
	client.observeFinalizationContext(ctx)
	client.updateDAGCalls++
	return nil
}

func writeLauncherTestHelperFile(path string, contents []byte) {
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "launcher test helper failed to write %s: %v\n", path, err)
		os.Exit(1)
	}
}

func TestLauncherCommandHelperProcess(t *testing.T) {
	mode := os.Getenv("KFP_LAUNCHER_TEST_HELPER_MODE")
	if mode == "" {
		return
	}
	if os.Getenv("KFP_LAUNCHER_TEST_ESCAPE_PROCESS_GROUP") == "true" {
		if _, err := syscall.Setsid(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "launcher test helper failed to create a session: %v\n", err)
			os.Exit(1)
		}
	}

	terminationSignals := make(chan os.Signal, 1)
	signal.Notify(terminationSignals, syscall.SIGINT, syscall.SIGTERM)

	pidFile := os.Getenv("KFP_LAUNCHER_TEST_PID_FILE")
	if pidFile != "" {
		writeLauncherTestHelperFile(pidFile, []byte(strconv.Itoa(os.Getpid())))
	}
	readyFile := os.Getenv("KFP_LAUNCHER_TEST_READY_FILE")
	writeLauncherTestHelperFile(readyFile, []byte("ready"))
	receivedSignal := <-terminationSignals
	signal.Stop(terminationSignals)

	terminatedFile := os.Getenv("KFP_LAUNCHER_TEST_TERMINATED_FILE")
	writeLauncherTestHelperFile(terminatedFile, []byte(receivedSignal.String()))
	if mode == "exit" {
		os.Exit(42)
	}
	if mode == "exit-zero" {
		return
	}

	for {
		time.Sleep(time.Hour)
	}
}

func launcherTestHelperCommand(
	ctx context.Context,
	terminationGracePeriod time.Duration,
	mode string,
	readyFile string,
	terminatedFile string,
) *commandWithGracefulCancellation {
	command := newCommandWithGracefulCancellation(
		ctx,
		terminationGracePeriod,
		os.Args[0],
		"-test.run=^TestLauncherCommandHelperProcess$",
	)
	command.Env = append(
		os.Environ(),
		"KFP_LAUNCHER_TEST_HELPER_MODE="+mode,
		"KFP_LAUNCHER_TEST_READY_FILE="+readyFile,
		"KFP_LAUNCHER_TEST_TERMINATED_FILE="+terminatedFile,
	)
	return command
}

func launcherTestShellChildCommand(
	ctx context.Context,
	terminationGracePeriod time.Duration,
	readyFile string,
	terminatedFile string,
	pidFile string,
) *commandWithGracefulCancellation {
	command := newCommandWithGracefulCancellation(
		ctx,
		terminationGracePeriod,
		"sh",
		"-c",
		`"$1" -test.run=^TestLauncherCommandHelperProcess$ & wait`,
		"launcher-test-shell",
		os.Args[0],
	)
	command.Env = append(
		os.Environ(),
		"KFP_LAUNCHER_TEST_HELPER_MODE=wait",
		"KFP_LAUNCHER_TEST_READY_FILE="+readyFile,
		"KFP_LAUNCHER_TEST_TERMINATED_FILE="+terminatedFile,
		"KFP_LAUNCHER_TEST_PID_FILE="+pidFile,
	)
	return command
}

func launcherTestEscapedChildCommand(
	ctx context.Context,
	terminationGracePeriod time.Duration,
	readyFile string,
	terminatedFile string,
	pidFile string,
) *commandWithGracefulCancellation {
	command := newCommandWithGracefulCancellation(
		ctx,
		terminationGracePeriod,
		"sh",
		"-c",
		`"$1" -test.run=^TestLauncherCommandHelperProcess$ & wait`,
		"launcher-test-shell",
		os.Args[0],
	)
	command.Env = append(
		os.Environ(),
		"KFP_LAUNCHER_TEST_HELPER_MODE=wait",
		"KFP_LAUNCHER_TEST_READY_FILE="+readyFile,
		"KFP_LAUNCHER_TEST_TERMINATED_FILE="+terminatedFile,
		"KFP_LAUNCHER_TEST_PID_FILE="+pidFile,
		"KFP_LAUNCHER_TEST_ESCAPE_PROCESS_GROUP=true",
	)
	output := &bytes.Buffer{}
	command.Stdout = output
	command.Stderr = output
	return command
}

func cleanupLauncherTestCommand(command *commandWithGracefulCancellation) func() {
	return func() {
		if command.Process != nil {
			_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		}
	}
}

func requireLinuxProcessGroups(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("launcher process-group behavior is Linux-specific")
	}
}

func TestNewCommandWithGracefulCancellation_ForwardsSIGTERMAndPreservesExitStatus(t *testing.T) {
	requireLinuxProcessGroups(t)
	tempDir := t.TempDir()
	readyFile := filepath.Join(tempDir, "ready")
	terminatedFile := filepath.Join(tempDir, "terminated")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	command := launcherTestHelperCommand(ctx, 5*time.Second, "exit", readyFile, terminatedFile)
	t.Cleanup(cleanupLauncherTestCommand(command))
	result := make(chan error, 1)
	go func() {
		result <- command.Run()
	}()

	require.Eventually(t, func() bool {
		_, err := os.Stat(readyFile)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	cancel()

	var err error
	select {
	case err = <-result:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the user command to exit after SIGTERM")
	}
	var exitError *exec.ExitError
	require.ErrorAs(t, err, &exitError)
	assert.Equal(t, 42, exitError.ExitCode())
	_, err = os.Stat(terminatedFile)
	assert.NoError(t, err, "the user command did not observe SIGTERM")
}

func TestNewCommandWithGracefulCancellation_ReturnsCancellationAfterGracefulZeroExit(t *testing.T) {
	requireLinuxProcessGroups(t)
	tempDir := t.TempDir()
	readyFile := filepath.Join(tempDir, "ready")
	terminatedFile := filepath.Join(tempDir, "terminated")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	command := launcherTestHelperCommand(ctx, 5*time.Second, "exit-zero", readyFile, terminatedFile)
	t.Cleanup(cleanupLauncherTestCommand(command))
	result := make(chan error, 1)
	go func() {
		result <- command.Run()
	}()

	require.Eventually(t, func() bool {
		_, err := os.Stat(readyFile)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	cancel()

	select {
	case err := <-result:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the user command to exit successfully after SIGTERM")
	}
	_, err := os.Stat(terminatedFile)
	assert.NoError(t, err, "the user command did not observe SIGTERM")
}

func TestNewCommandWithGracefulCancellation_ForwardsSIGINT(t *testing.T) {
	requireLinuxProcessGroups(t)
	tempDir := t.TempDir()
	readyFile := filepath.Join(tempDir, "ready")
	terminatedFile := filepath.Join(tempDir, "terminated")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	command := launcherTestHelperCommand(ctx, 5*time.Second, "exit-zero", readyFile, terminatedFile)
	t.Cleanup(cleanupLauncherTestCommand(command))
	result := make(chan error, 1)
	go func() {
		result <- command.Run()
	}()

	require.Eventually(t, func() bool {
		_, err := os.Stat(readyFile)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	cancel(launcherTestSignalCause{signal: syscall.SIGINT, receivedAt: time.Now()})

	select {
	case err := <-result:
		var signalCause launcherTestSignalCause
		require.ErrorAs(t, err, &signalCause)
		assert.Equal(t, syscall.SIGINT, signalCause.Signal())
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the user command to exit after SIGINT")
	}
	receivedSignal, err := os.ReadFile(terminatedFile)
	require.NoError(t, err)
	assert.Equal(t, syscall.SIGINT.String(), string(receivedSignal))
}

func TestNewCommandWithGracefulCancellation_PreservesSignalBeforeStart(t *testing.T) {
	requireLinuxProcessGroups(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	signalCause := launcherTestSignalCause{signal: syscall.SIGTERM, receivedAt: time.Now()}
	cancel(signalCause)
	command := newCommandWithGracefulCancellation(ctx, time.Second, "sh", "-c", "exit 0")

	err := command.Run()

	var actualCause launcherTestSignalCause
	require.ErrorAs(t, err, &actualCause)
	assert.Equal(t, signalCause, actualCause)
	assert.Nil(t, command.Process)
}

func TestWaitForCommandExit_TimesOut(t *testing.T) {
	waitResult := make(chan error)

	timedOut, err := waitForCommandExit(waitResult, 20*time.Millisecond)

	assert.NoError(t, err)
	assert.True(t, timedOut)
}

func TestCancellationSignalDefaultsToSIGTERM(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("non-signal cancellation"))

	assert.Equal(t, syscall.SIGTERM, cancellationSignal(ctx))
}

func TestShouldSkipOutputUploadsOnlyWhileCommandWaitIsIncomplete(t *testing.T) {
	assert.True(t, shouldSkipOutputUploads(fmt.Errorf("wrapped: %w", errUserCommandWaitIncomplete)))
	assert.False(t, shouldSkipOutputUploads(errUserCommandCleanupIncomplete))
	assert.False(t, shouldSkipOutputUploads(errors.New("component failed")))
}

func TestProcessGroupRunningWithProcDistinguishesZombies(t *testing.T) {
	procRoot := t.TempDir()
	processDir := filepath.Join(procRoot, "123")
	require.NoError(t, os.Mkdir(processDir, 0o700))
	statPath := filepath.Join(processDir, "stat")
	require.NoError(t, os.WriteFile(statPath, []byte("123 (worker) Z 1 42 0"), 0o600))
	groupExists := func(int) bool { return true }

	assert.False(t, processGroupRunningWithProc(42, procRoot, groupExists))
	require.NoError(t, os.WriteFile(statPath, []byte("123 (worker) S 1 42 0"), 0o600))
	assert.True(t, processGroupRunningWithProc(42, procRoot, groupExists))
}

func TestProcessGroupRunningWithProcFallsBackToKernelProbe(t *testing.T) {
	missingProcRoot := filepath.Join(t.TempDir(), "missing")

	assert.True(t, processGroupRunningWithProc(42, missingProcRoot, func(int) bool { return true }))
	assert.False(t, processGroupRunningWithProc(42, missingProcRoot, func(int) bool { return false }))
}

func TestNewCommandWithGracefulCancellation_ForceKillsAfterGracePeriod(t *testing.T) {
	requireLinuxProcessGroups(t)
	tempDir := t.TempDir()
	readyFile := filepath.Join(tempDir, "ready")
	terminatedFile := filepath.Join(tempDir, "terminated")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	terminationGracePeriod := 500 * time.Millisecond
	command := launcherTestHelperCommand(ctx, terminationGracePeriod, "wait", readyFile, terminatedFile)
	t.Cleanup(cleanupLauncherTestCommand(command))
	result := make(chan error, 1)
	go func() {
		result <- command.Run()
	}()

	require.Eventually(t, func() bool {
		_, err := os.Stat(readyFile)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	startedCancellation := time.Now()
	cancel()

	var err error
	select {
	case err = <-result:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the user command to be force-killed")
	}
	assert.Error(t, err)
	assert.Less(t, time.Since(startedCancellation), 5*time.Second)
	_, err = os.Stat(terminatedFile)
	assert.NoError(t, err, "the user command did not observe SIGTERM before it was killed")
}

func TestNewCommandWithGracefulCancellation_TerminatesShellSpawnedProcessGroup(t *testing.T) {
	requireLinuxProcessGroups(t)
	tempDir := t.TempDir()
	readyFile := filepath.Join(tempDir, "ready")
	terminatedFile := filepath.Join(tempDir, "terminated")
	pidFile := filepath.Join(tempDir, "pid")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	terminationGracePeriod := 500 * time.Millisecond
	command := launcherTestShellChildCommand(ctx, terminationGracePeriod, readyFile, terminatedFile, pidFile)
	t.Cleanup(cleanupLauncherTestCommand(command))
	result := make(chan error, 1)
	go func() {
		result <- command.Run()
	}()

	require.Eventually(t, func() bool {
		_, err := os.Stat(readyFile)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	startedCancellation := time.Now()
	cancel()

	var err error
	select {
	case err = <-result:
		assert.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the user command process group to be force-killed")
	}
	assert.GreaterOrEqual(t, time.Since(startedCancellation), terminationGracePeriod)
	assert.Less(t, time.Since(startedCancellation), 5*time.Second)
	_, err = os.Stat(terminatedFile)
	assert.NoError(t, err, "the child process did not observe SIGTERM before it was killed")
	require.Eventually(t, func() bool {
		return !processGroupRunning(command.Process.Pid)
	}, time.Second, 10*time.Millisecond, "Run returned while the process group still had running members")
}

func TestNewCommandWithGracefulCancellation_BoundsWaitForEscapedDescendant(t *testing.T) {
	requireLinuxProcessGroups(t)
	tempDir := t.TempDir()
	readyFile := filepath.Join(tempDir, "ready")
	terminatedFile := filepath.Join(tempDir, "terminated")
	pidFile := filepath.Join(tempDir, "pid")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	command := launcherTestEscapedChildCommand(ctx, 100*time.Millisecond, readyFile, terminatedFile, pidFile)
	escapedChildPID := 0
	t.Cleanup(func() {
		if escapedChildPID != 0 {
			_ = syscall.Kill(escapedChildPID, syscall.SIGKILL)
		}
	})
	result := make(chan error, 1)
	go func() {
		result <- command.Run()
	}()

	require.Eventually(t, func() bool {
		_, err := os.Stat(readyFile)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	pidBytes, err := os.ReadFile(pidFile)
	require.NoError(t, err)
	escapedChildPID, err = strconv.Atoi(string(pidBytes))
	require.NoError(t, err)
	startedCancellation := time.Now()
	cancel()

	select {
	case err = <-result:
		require.Error(t, err)
		assert.ErrorContains(t, err, "process group exited")
		assert.ErrorIs(t, err, errUserCommandCleanupIncomplete)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the command with an escaped descendant")
	}
	assert.Less(t, time.Since(startedCancellation), 4*time.Second)
	_, err = os.Stat(terminatedFile)
	assert.ErrorIs(t, err, os.ErrNotExist, "the escaped child unexpectedly received the process-group signal")
}

func TestLauncherV2Execute_UsesBoundedFinalizationContextAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	signalCause := launcherTestSignalCause{signal: syscall.SIGTERM, receivedAt: time.Now()}
	execution := metadata.NewExecution(&pb.Execution{
		CustomProperties: map[string]*pb.Value{
			"parent_dag_id": {Value: &pb.Value_IntValue{IntValue: 7}},
		},
	})
	metadataClient := &launcherFinalizationMetadataClient{
		FakeClient:         metadata.NewFakeClient(),
		execution:          execution,
		cancelExecution:    cancel,
		cancellationCause:  signalCause,
		cancelOnPrePublish: true,
		getDAGErr:          errors.New("DAG lookup failed"),
	}
	launcher := &LauncherV2{
		executionID:   1,
		executorInput: &pipelinespec.ExecutorInput{},
		clientManager: client_manager.NewFakeClientManager(nil, metadataClient, nil),
	}

	err := launcher.Execute(ctx)

	require.Error(t, err)
	assert.ErrorIs(t, err, signalCause)
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.Equal(t, pb.Execution_FAILED, metadataClient.publishedExecutionState)
	assert.Zero(t, metadataClient.updateDAGCalls)
	require.NotEmpty(t, metadataClient.finalizationContexts)
	for _, finalizationCtx := range metadataClient.finalizationContexts {
		assert.NoError(t, finalizationCtx.err)
		assert.True(t, finalizationCtx.hasDeadline, "finalization context must be bounded")
	}
}

func TestLauncherV2Execute_CommitsCompletionAfterSuccessfulPublish(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	signalCause := launcherTestSignalCause{signal: syscall.SIGTERM, receivedAt: time.Now()}
	execution := metadata.NewExecution(&pb.Execution{
		CustomProperties: map[string]*pb.Value{
			"parent_dag_id": {Value: &pb.Value_IntValue{IntValue: 7}},
		},
	})
	metadataClient := &launcherFinalizationMetadataClient{
		FakeClient:        metadata.NewFakeClient(),
		execution:         execution,
		cancelExecution:   cancel,
		cancellationCause: signalCause,
		cancelOnPublish:   true,
		getDAGErr:         errors.New("DAG lookup failed"),
	}
	launcher := &LauncherV2{
		executionID:   1,
		executorInput: &pipelinespec.ExecutorInput{},
		clientManager: client_manager.NewFakeClientManager(nil, metadataClient, nil),
		componentExecutor: func(
			context.Context,
			*metadata.Execution,
		) (*pipelinespec.ExecutorOutput, []*metadata.OutputArtifact, error) {
			return nil, nil, nil
		},
	}

	err := launcher.Execute(ctx)

	require.NoError(t, err)
	assert.ErrorIs(t, context.Cause(ctx), signalCause)
	assert.Equal(t, pb.Execution_COMPLETE, metadataClient.publishedExecutionState)
}

func TestLauncherV2Execute_CommitsPersistedOutputsWhenSignalRacesComponentReturn(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	signalCause := launcherTestSignalCause{signal: syscall.SIGTERM, receivedAt: time.Now()}
	execution := metadata.NewExecution(&pb.Execution{
		CustomProperties: map[string]*pb.Value{
			"parent_dag_id": {Value: &pb.Value_IntValue{IntValue: 7}},
		},
	})
	metadataClient := &launcherFinalizationMetadataClient{
		FakeClient: metadata.NewFakeClient(),
		execution:  execution,
		getDAGErr:  errors.New("DAG lookup failed"),
	}
	launcher := &LauncherV2{
		executionID:   1,
		executorInput: &pipelinespec.ExecutorInput{},
		clientManager: client_manager.NewFakeClientManager(nil, metadataClient, nil),
		componentExecutor: func(
			context.Context,
			*metadata.Execution,
		) (*pipelinespec.ExecutorOutput, []*metadata.OutputArtifact, error) {
			cancel(signalCause)
			return nil, nil, nil
		},
	}

	err := launcher.Execute(ctx)

	require.NoError(t, err)
	assert.ErrorIs(t, context.Cause(ctx), signalCause)
	assert.Equal(t, pb.Execution_COMPLETE, metadataClient.publishedExecutionState)
}

func TestLauncherV2Execute_CommitsCompletionWhenSignalInterruptsCacheWrite(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	signalCause := launcherTestSignalCause{signal: syscall.SIGTERM, receivedAt: time.Now()}
	executionID := int64(1)
	execution := metadata.NewExecution(&pb.Execution{
		Id: &executionID,
		CustomProperties: map[string]*pb.Value{
			"parent_dag_id":     {Value: &pb.Value_IntValue{IntValue: 7}},
			"cache_fingerprint": {Value: &pb.Value_StringValue{StringValue: "fingerprint"}},
		},
	})
	metadataClient := &launcherFinalizationMetadataClient{
		FakeClient: metadata.NewFakeClient(),
		execution:  execution,
		getDAGErr:  errors.New("DAG lookup failed"),
	}
	cacheClient := &launcherCancellationCacheClient{
		cancel: cancel,
		cause:  signalCause,
	}
	launcher := &LauncherV2{
		executionID:   1,
		executorInput: &pipelinespec.ExecutorInput{},
		clientManager: client_manager.NewFakeClientManager(nil, metadataClient, cacheClient),
		componentExecutor: func(
			context.Context,
			*metadata.Execution,
		) (*pipelinespec.ExecutorOutput, []*metadata.OutputArtifact, error) {
			return nil, nil, nil
		},
	}

	err := launcher.Execute(ctx)

	require.NoError(t, err)
	assert.Equal(t, 1, cacheClient.calls)
	assert.ErrorIs(t, context.Cause(ctx), signalCause)
	assert.Equal(t, pb.Execution_COMPLETE, metadataClient.publishedExecutionState)
}

func TestLauncherV2Execute_FailsWhenPublishMissesShutdownDeadline(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	signalCause := launcherTestSignalCause{
		signal:     syscall.SIGTERM,
		receivedAt: time.Now().Add(-launcherShutdownTimeout),
	}
	execution := metadata.NewExecution(&pb.Execution{
		CustomProperties: map[string]*pb.Value{
			"parent_dag_id": {Value: &pb.Value_IntValue{IntValue: 7}},
		},
	})
	metadataClient := &launcherFinalizationMetadataClient{
		FakeClient:              metadata.NewFakeClient(),
		execution:               execution,
		cancelExecution:         cancel,
		cancellationCause:       signalCause,
		cancelOnPublish:         true,
		blockPublishUntilCancel: true,
		publishError:            grpcstatus.Error(codes.DeadlineExceeded, "metadata publication deadline expired"),
		getDAGErr:               context.DeadlineExceeded,
	}
	launcher := &LauncherV2{
		executionID:   1,
		executorInput: &pipelinespec.ExecutorInput{},
		clientManager: client_manager.NewFakeClientManager(nil, metadataClient, nil),
		componentExecutor: func(
			context.Context,
			*metadata.Execution,
		) (*pipelinespec.ExecutorOutput, []*metadata.OutputArtifact, error) {
			return nil, nil, nil
		},
	}

	started := time.Now()
	err := launcher.Execute(ctx)

	require.Error(t, err)
	assert.ErrorIs(t, err, signalCause)
	assert.ErrorContains(t, err, codes.DeadlineExceeded.String())
	assert.ErrorIs(t, context.Cause(ctx), signalCause)
	assert.Equal(t, pb.Execution_COMPLETE, metadataClient.publishedExecutionState)
	assert.Less(t, time.Since(started), 5*time.Second)
}

func TestLauncherV2Execute_PublishesPartialOutputsAfterComponentFailure(t *testing.T) {
	componentErr := errors.New("component failed")
	execution := metadata.NewExecution(&pb.Execution{
		CustomProperties: map[string]*pb.Value{
			"parent_dag_id": {Value: &pb.Value_IntValue{IntValue: 7}},
		},
	})
	metadataClient := &launcherFinalizationMetadataClient{
		FakeClient: metadata.NewFakeClient(),
		execution:  execution,
		getDAGErr:  errors.New("DAG lookup failed"),
	}
	executorOutput := &pipelinespec.ExecutorOutput{
		ParameterValues: map[string]*structpb.Value{
			"partial": structpb.NewStringValue("available"),
		},
	}
	outputArtifact := &metadata.OutputArtifact{Name: "executor-logs"}
	launcher := &LauncherV2{
		executionID:   1,
		executorInput: &pipelinespec.ExecutorInput{},
		clientManager: client_manager.NewFakeClientManager(nil, metadataClient, nil),
		componentExecutor: func(
			context.Context,
			*metadata.Execution,
		) (*pipelinespec.ExecutorOutput, []*metadata.OutputArtifact, error) {
			return executorOutput, []*metadata.OutputArtifact{outputArtifact}, componentErr
		},
	}

	err := launcher.Execute(context.Background())

	require.Error(t, err)
	assert.ErrorIs(t, err, componentErr)
	assert.Equal(t, pb.Execution_FAILED, metadataClient.publishedExecutionState)
	assert.Equal(t, "available", metadataClient.publishedParameters["partial"].GetStringValue())
	require.Len(t, metadataClient.publishedArtifacts, 1)
	assert.Same(t, outputArtifact, metadataClient.publishedArtifacts[0])
}

func TestNewFinalizationContext_RemainsLiveWhenParentIsCanceled(t *testing.T) {
	parentCtx, cancelParent := context.WithCancel(context.Background())
	finalizationCtx, cancelFinalization := newFinalizationContext(parentCtx, 0)
	defer cancelFinalization()
	_, hasDeadline := finalizationCtx.Deadline()
	assert.False(t, hasDeadline, "normal finalization must preserve unbounded artifact uploads")

	cancelParent()

	assert.NoError(t, finalizationCtx.Err())
}

func TestShutdownBudgetForTerminationGracePeriod(t *testing.T) {
	budget := shutdownBudgetForTerminationGracePeriod(5 * time.Second)

	assert.Equal(t, 1500*time.Millisecond, budget.userCommandGrace)
	assert.Equal(t, 4500*time.Millisecond, budget.finalization)
	assert.Equal(t, 4500*time.Millisecond, budget.shutdown)
	assert.Equal(t, 5*time.Second, budget.hardShutdown)
	assert.Equal(t, 1500*time.Millisecond, budget.metadataReserve)
	assert.Equal(t, time.Second, budget.minimumFinalization)
	assert.Equal(t, time.Second, budget.commandKillWait)

	zeroBudget := shutdownBudgetForTerminationGracePeriod(0)
	assert.Zero(t, zeroBudget.shutdown)
	assert.Zero(t, zeroBudget.hardShutdown)
	assert.Zero(t, zeroBudget.userCommandGrace)

	longBudget := shutdownBudgetForTerminationGracePeriod(60 * time.Second)
	assert.Equal(t, 59*time.Second/3, longBudget.userCommandGrace)
	assert.Greater(t, longBudget.userCommandGrace, userCommandTerminationGracePeriod)
}

func TestLauncherV2ReadsPodTerminationGracePeriod(t *testing.T) {
	terminationGracePeriodSeconds := int64(5)
	pod := &k8score.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "launcher-pod", Namespace: "test-namespace"},
		Spec: k8score.PodSpec{
			TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
		},
	}
	launcher := &LauncherV2{
		options: LauncherV2Options{Namespace: pod.Namespace, PodName: pod.Name},
		clientManager: client_manager.NewFakeClientManager(
			fake.NewSimpleClientset(pod),
			nil,
			nil,
		),
	}

	budget := shutdownBudgetFromContext(launcher.withPodShutdownBudget(context.Background()))

	assert.Equal(t, 4500*time.Millisecond, budget.shutdown)
	assert.Equal(t, 1500*time.Millisecond, budget.userCommandGrace)
}

func TestNewFinalizationContext_ArmsShutdownBudgetAfterStart(t *testing.T) {
	parentCtx, cancelParent := context.WithCancelCause(context.Background())
	finalizationCtx, cancelFinalization := newFinalizationContext(parentCtx, metadataFinalizationReserve)
	defer cancelFinalization()
	cancelParent(launcherTestSignalCause{
		signal:     syscall.SIGTERM,
		receivedAt: time.Now().Add(-launcherShutdownTimeout),
	})

	require.Eventually(t, func() bool {
		return finalizationCtx.Err() != nil
	}, 3*time.Second, 10*time.Millisecond)
}

func TestNewFinalizationContext_ReservesShutdownTimeForMetadata(t *testing.T) {
	receivedAt := time.Now().Add(-12 * time.Second)
	parentCtx, cancelParent := context.WithCancelCause(context.Background())
	cancelParent(launcherTestSignalCause{signal: syscall.SIGTERM, receivedAt: receivedAt})

	artifactCtx, cancelArtifact := newFinalizationContext(parentCtx, metadataFinalizationReserve)
	defer cancelArtifact()
	metadataCtx, cancelMetadata := newFinalizationContext(parentCtx, 0)
	defer cancelMetadata()

	artifactDeadline, hasDeadline := artifactCtx.Deadline()
	require.True(t, hasDeadline)
	metadataDeadline, hasDeadline := metadataCtx.Deadline()
	require.True(t, hasDeadline)
	assert.WithinDuration(
		t,
		receivedAt.Add(launcherShutdownTimeout-metadataFinalizationReserve),
		artifactDeadline,
		100*time.Millisecond,
	)
	assert.WithinDuration(t, receivedAt.Add(launcherShutdownTimeout), metadataDeadline, 100*time.Millisecond)
	assert.NoError(t, artifactCtx.Err())
	assert.NoError(t, metadataCtx.Err())
}

func TestFinalizationDeadline_UsesDefaultForPlainCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	deadline := finalizationDeadline(ctx, 0)

	assert.WithinDuration(t, time.Now().Add(launcherFinalizationTimeout), deadline, 100*time.Millisecond)
}

func TestFinalizationDeadline_UsesHardGraceForFinalMetadataAttempt(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	receivedAt := time.Now().Add(-launcherShutdownTimeout)
	cancel(launcherTestSignalCause{
		signal:     syscall.SIGTERM,
		receivedAt: receivedAt,
	})

	deadline := finalizationDeadline(ctx, 0)

	assert.WithinDuration(t, time.Now().Add(minimumFinalizationAttempt), deadline, 100*time.Millisecond)
	assert.True(t, deadline.Before(receivedAt.Add(launcherHardShutdownTimeout)))
}

func TestFinalizationDeadline_NeverExceedsPodHardGrace(t *testing.T) {
	budget := shutdownBudgetForTerminationGracePeriod(5 * time.Second)
	receivedAt := time.Now().Add(-budget.shutdown)
	parentCtx := context.WithValue(context.Background(), launcherShutdownBudgetContextKey{}, budget)
	ctx, cancel := context.WithCancelCause(parentCtx)
	cancel(launcherTestSignalCause{signal: syscall.SIGTERM, receivedAt: receivedAt})

	deadline := finalizationDeadline(ctx, 0)

	assert.WithinDuration(t, receivedAt.Add(budget.hardShutdown), deadline, 100*time.Millisecond)
	assert.False(t, deadline.After(receivedAt.Add(budget.hardShutdown)))
}

func TestExecuteV2RejectsNilOpenBucketConfig(t *testing.T) {
	_, _, err := executeV2(
		context.Background(),
		nil,
		nil,
		"",
		nil,
		nil,
		nil,
		nil,
		"",
		nil,
		"",
		"",
		nil,
	)

	assert.EqualError(t, err, "open bucket config is nil")
}

// Tests that launcher correctly executes the user component and successfully writes output parameters to file.
func Test_executeV2_Parameters(t *testing.T) {
	tests := []struct {
		name          string
		executorInput *pipelinespec.ExecutorInput
		executorArgs  []string
		wantErr       bool
	}{
		{
			"happy pass",
			&pipelinespec.ExecutorInput{
				Inputs: &pipelinespec.ExecutorInput_Inputs{
					ParameterValues: map[string]*structpb.Value{"a": structpb.NewNumberValue(1), "b": structpb.NewNumberValue(2)},
				},
			},
			[]string{"-c", "test {{$.inputs.parameters['a']}} -eq 1 || exit 1\ntest {{$.inputs.parameters['b']}} -eq 2 || exit 1"},
			false,
		},
		{
			"use default value",
			&pipelinespec.ExecutorInput{
				Inputs: &pipelinespec.ExecutorInput_Inputs{
					ParameterValues: map[string]*structpb.Value{"b": structpb.NewNumberValue(2)},
				},
			},
			[]string{"-c", "test {{$.inputs.parameters['a']}} -eq 5 || exit 1\ntest {{$.inputs.parameters['b']}} -eq 2 || exit 1"},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeKubernetesClientset := &fake.Clientset{}
			fakeMetadataClient := metadata.NewFakeClient()
			bucket, err := blob.OpenBucket(context.Background(), "mem://test-bucket")
			assert.Nil(t, err)
			bucketConfig, err := objectstore.ParseBucketConfig("mem://test-bucket/pipeline-root/", nil)
			assert.Nil(t, err)
			_, _, err = executeV2(
				context.Background(),
				test.executorInput,
				addNumbersComponent,
				"sh",
				test.executorArgs,
				bucket,
				bucketConfig,
				fakeMetadataClient,
				"namespace",
				fakeKubernetesClientset,
				"false",
				"",
				&OpenBucketConfig{context.Background(), fakeKubernetesClientset, "namespace", bucketConfig},
			)

			if test.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)

			}
		})
	}
}

func Test_executeV2_publishLogs(t *testing.T) {
	tests := []struct {
		name          string
		executorInput *pipelinespec.ExecutorInput
		executorArgs  []string
		retryIndex    string
		wantErr       bool
		uploadFailure bool
	}{
		{
			"happy pass",
			&pipelinespec.ExecutorInput{
				Inputs: &pipelinespec.ExecutorInput_Inputs{
					ParameterValues: map[string]*structpb.Value{"a": structpb.NewNumberValue(1), "b": structpb.NewNumberValue(2)},
				},
			},
			[]string{"-c", "echo testoutput && test {{$.inputs.parameters['a']}} -eq 1 || exit 1\ntest {{$.inputs.parameters['b']}} -eq 2 || exit 1"},
			"",
			false,
			false,
		},
		{
			"use default value",
			&pipelinespec.ExecutorInput{
				Inputs: &pipelinespec.ExecutorInput_Inputs{
					ParameterValues: map[string]*structpb.Value{"b": structpb.NewNumberValue(2)},
				},
			},
			[]string{"-c", "echo testoutput && test {{$.inputs.parameters['a']}} -eq 5 || exit 1\ntest {{$.inputs.parameters['b']}} -eq 2 || exit 1"},
			"",
			false,
			false,
		},
		{
			"sad fail",
			&pipelinespec.ExecutorInput{
				Inputs: &pipelinespec.ExecutorInput_Inputs{
					ParameterValues: map[string]*structpb.Value{"a": structpb.NewNumberValue(1), "b": structpb.NewNumberValue(2)},
				},
			},
			[]string{"-c", "echo testoutput && exit 1"},
			"",
			true,
			false,
		},
		{
			"retry required - component success",
			&pipelinespec.ExecutorInput{
				Inputs: &pipelinespec.ExecutorInput_Inputs{
					ParameterValues: map[string]*structpb.Value{"a": structpb.NewNumberValue(1), "b": structpb.NewNumberValue(2)},
				},
			},
			[]string{"-c", "echo testoutput && test {{$.inputs.parameters['a']}} -eq 1 || exit 1\ntest {{$.inputs.parameters['b']}} -eq 2 || exit 1"},
			"",
			false,
			true,
		},
		{
			"retry required - component failure",
			&pipelinespec.ExecutorInput{
				Inputs: &pipelinespec.ExecutorInput_Inputs{
					ParameterValues: map[string]*structpb.Value{"a": structpb.NewNumberValue(1), "b": structpb.NewNumberValue(2)},
				},
			},
			[]string{"-c", "echo testoutput && exit 1"},
			"",
			true,
			true,
		},
		{
			// KFP_RETRY_INDEX is injected by the Argo compiler via "{{retries}}".
			// The executor-logs URI must be qualified with the retry index so each
			// attempt writes to a distinct, human-readable path (executor-logs-0,
			// executor-logs-1, …).
			"retry index qualifies executor-logs URI",
			&pipelinespec.ExecutorInput{
				Inputs: &pipelinespec.ExecutorInput_Inputs{
					ParameterValues: map[string]*structpb.Value{"a": structpb.NewNumberValue(1), "b": structpb.NewNumberValue(2)},
				},
			},
			[]string{"-c", "echo testoutput && test {{$.inputs.parameters['a']}} -eq 1 || exit 1"},
			"3",
			false,
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeKubernetesClientset := &fake.Clientset{}
			var fakeMetadataClient metadata.ClientInterface
			var countingFakeMetadataClient *metadata.RecordArtifactFailureFakeClient
			// Use a fake client that will fail the executor-logs RecordArtifact call the first time,
			// and succeed the second time, to test retry behavior without depending on map iteration order.
			if test.uploadFailure {
				countingFakeMetadataClient = metadata.NewRecordArtifactFailureFakeClientForOutput("executor-logs", 1)
				fakeMetadataClient = countingFakeMetadataClient
			} else {
				fakeMetadataClient = metadata.NewFakeClient()
			}
			bucket, err := blob.OpenBucket(context.Background(), "mem://test-bucket")
			assert.Nil(t, err)
			bucketConfig, err := objectstore.ParseBucketConfig("mem://test-bucket/pipeline-root/", nil)
			assert.Nil(t, err)
			// Add executor-logs and output artifact to outputs
			if test.executorInput.Outputs == nil {
				test.executorInput.Outputs = &pipelinespec.ExecutorInput_Outputs{}
			}
			if test.executorInput.Outputs.Artifacts == nil {
				test.executorInput.Outputs.Artifacts = make(map[string]*pipelinespec.ArtifactList)
			}
			// Use a temp directory for CustomPath to avoid writing to filesystem
			tempDir := t.TempDir()
			customPath := filepath.Join(tempDir, "executor-logs")
			test.executorInput.Outputs.Artifacts["executor-logs"] = &pipelinespec.ArtifactList{
				Artifacts: []*pipelinespec.RuntimeArtifact{
					{
						Uri:        "mem://test-bucket/pipeline-root/executor-logs",
						Type:       &pipelinespec.ArtifactTypeSchema{Kind: &pipelinespec.ArtifactTypeSchema_SchemaTitle{SchemaTitle: "system.Artifact"}},
						CustomPath: &customPath,
					},
				},
			}
			outputDataPath := filepath.Join(tempDir, "output-data")
			test.executorInput.Outputs.Artifacts["output-data"] = &pipelinespec.ArtifactList{
				Artifacts: []*pipelinespec.RuntimeArtifact{
					{
						Uri:        "mem://test-bucket/pipeline-root/output-data",
						Type:       &pipelinespec.ArtifactTypeSchema{Kind: &pipelinespec.ArtifactTypeSchema_SchemaTitle{SchemaTitle: "system.Dataset"}},
						CustomPath: &outputDataPath,
					},
				},
			}

			// Simulate Argo injecting KFP_RETRY_INDEX into the pod env.
			if test.retryIndex != "" {
				t.Setenv(EnvRetryIndex, test.retryIndex)
			}

			_, outputArtifacts, err := executeV2(
				context.Background(),
				test.executorInput,
				addNumbersComponent,
				"sh",
				test.executorArgs,
				bucket,
				bucketConfig,
				fakeMetadataClient,
				"namespace",
				fakeKubernetesClientset,
				"true",
				"",
				&OpenBucketConfig{context.Background(), fakeKubernetesClientset, "namespace", bucketConfig},
			)

			if test.wantErr {
				assert.NotNil(t, err)
				assert.Len(t, outputArtifacts, 1, "Expected 1 output artifact (executor-logs)")
				if test.uploadFailure {
					assert.Equal(t, 2, countingFakeMetadataClient.OutputNameCalls["executor-logs"])
				}
			} else {
				assert.Nil(t, err)
				assert.Len(t, outputArtifacts, 2, "Expected 2 output artifacts (executor-logs and output-data)")
				if test.uploadFailure {
					assert.Equal(t, 2, countingFakeMetadataClient.OutputNameCalls["executor-logs"])
				}
			}

			// When a retry index is set, the executor-logs URI (and therefore the
			// object-store key) must be suffixed with the index so retries don't
			// overwrite each other (e.g. executor-logs-3).
			effectiveIndex := test.retryIndex
			if effectiveIndex == "" {
				effectiveIndex = "0"
			}
			logKey := "executor-logs-" + effectiveIndex
			logArt := test.executorInput.Outputs.Artifacts["executor-logs"].Artifacts[0]
			assert.Contains(t, logArt.Uri, effectiveIndex,
				"executor-logs URI should contain the retry index for attempt isolation")
			if assert.NotNil(t, logArt.CustomPath) {
				assert.Contains(t, *logArt.CustomPath, effectiveIndex,
					"executor-logs CustomPath should contain the retry index for attempt isolation")
				_, err = os.Stat(*logArt.CustomPath)
				assert.NoError(t, err, "Expected executor-logs file to exist at the qualified custom path")
			}

			outputLog, err := bucket.ReadAll(context.TODO(), logKey)
			assert.Nil(t, err, "Expected executor-logs to be readable at key %q", logKey)
			assert.Equal(t, "testoutput\n", string(outputLog))
		})
	}
}

func Test_executeV2_publishLogs_skipsArtifactWhenSetupFailsBeforeLogsExist(t *testing.T) {
	fakeKubernetesClientset := &fake.Clientset{}
	fakeMetadataClient := metadata.NewFakeClient()
	bucket, err := blob.OpenBucket(context.Background(), "mem://test-bucket")
	assert.Nil(t, err)
	bucketConfig, err := objectstore.ParseBucketConfig("mem://test-bucket/pipeline-root/", nil)
	assert.Nil(t, err)

	tempDir := t.TempDir()
	customPath := filepath.Join(tempDir, "executor-logs")
	executorInput := &pipelinespec.ExecutorInput{
		Inputs: &pipelinespec.ExecutorInput_Inputs{
			ParameterValues: map[string]*structpb.Value{},
		},
		Outputs: &pipelinespec.ExecutorInput_Outputs{
			Artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {
					Artifacts: []*pipelinespec.RuntimeArtifact{
						{
							Uri:        "mem://test-bucket/pipeline-root/executor-logs",
							Type:       &pipelinespec.ArtifactTypeSchema{Kind: &pipelinespec.ArtifactTypeSchema_SchemaTitle{SchemaTitle: "system.Artifact"}},
							CustomPath: &customPath,
						},
					},
				},
			},
		},
	}

	_, outputArtifacts, err := executeV2(
		context.Background(),
		executorInput,
		addNumbersComponent,
		"sh",
		[]string{"-c", "echo testoutput"},
		bucket,
		bucketConfig,
		fakeMetadataClient,
		"namespace",
		fakeKubernetesClientset,
		"true",
		filepath.Join(tempDir, "missing-ca.pem"),
		&OpenBucketConfig{context.Background(), fakeKubernetesClientset, "namespace", bucketConfig},
	)

	assert.Error(t, err)
	assert.Empty(t, outputArtifacts, "Expected no output artifacts when logs were never created")

	_, err = bucket.ReadAll(context.TODO(), "executor-logs-0")
	assert.Error(t, err, "Expected no qualified executor-logs blob to be uploaded")
}

func Test_executeV2_publishLogs_qualifiesExecutorInputBeforeCommandCompilation(t *testing.T) {
	fakeKubernetesClientset := &fake.Clientset{}
	fakeMetadataClient := metadata.NewFakeClient()
	bucket, err := blob.OpenBucket(context.Background(), "mem://test-bucket")
	assert.Nil(t, err)
	bucketConfig, err := objectstore.ParseBucketConfig("mem://test-bucket/pipeline-root/", nil)
	assert.Nil(t, err)

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "executor-logs")
	outputMetadataFile := filepath.Join(tempDir, "output_metadata.json")
	executorInput := &pipelinespec.ExecutorInput{
		Inputs: &pipelinespec.ExecutorInput_Inputs{
			ParameterValues: map[string]*structpb.Value{
				"a": structpb.NewNumberValue(1),
				"b": structpb.NewNumberValue(2),
			},
		},
		Outputs: &pipelinespec.ExecutorInput_Outputs{
			OutputFile: outputMetadataFile,
			Artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {
					Artifacts: []*pipelinespec.RuntimeArtifact{
						{
							Name:       "executor-logs",
							Uri:        "mem://test-bucket/pipeline-root/executor-logs",
							Type:       &pipelinespec.ArtifactTypeSchema{Kind: &pipelinespec.ArtifactTypeSchema_SchemaTitle{SchemaTitle: "system.Artifact"}},
							CustomPath: &logPath,
						},
					},
				},
			},
		},
	}
	t.Setenv(EnvRetryIndex, "0")

	script := fmt.Sprintf(`echo testoutput && mkdir -p %q && cat <<'EOF' > %q
{"artifacts":{"executor-logs":{"artifacts":[{"name":"executor-logs","uri":"{{$.outputs.artifacts['executor-logs'].uri}}","customPath":"{{$.outputs.artifacts['executor-logs'].path}}","type":{"schemaTitle":"system.Artifact"}}]}}}
EOF`, filepath.Dir(outputMetadataFile), outputMetadataFile)

	_, outputArtifacts, err := executeV2(
		context.Background(),
		executorInput,
		addNumbersComponent,
		"sh",
		[]string{"-c", script},
		bucket,
		bucketConfig,
		fakeMetadataClient,
		"namespace",
		fakeKubernetesClientset,
		"true",
		"",
		&OpenBucketConfig{context.Background(), fakeKubernetesClientset, "namespace", bucketConfig},
	)

	assert.Nil(t, err)
	assert.Len(t, outputArtifacts, 1, "Expected executor-logs to be uploaded")

	logArtifact := executorInput.Outputs.Artifacts["executor-logs"].Artifacts[0]
	assert.Contains(t, logArtifact.Uri, "-0")
	if assert.NotNil(t, logArtifact.CustomPath) {
		assert.Contains(t, *logArtifact.CustomPath, "-0")
	}

	outputMetadata, err := os.ReadFile(outputMetadataFile)
	assert.Nil(t, err)
	assert.Contains(t, string(outputMetadata), "executor-logs-0",
		"Expected compiled executor input placeholders to use the retry-qualified log location")

	outputLog, err := bucket.ReadAll(context.TODO(), "executor-logs-0")
	assert.Nil(t, err)
	assert.Equal(t, "testoutput\n", string(outputLog))
}

func Test_getPlaceholders_WorkspaceArtifactPath(t *testing.T) {
	execIn := &pipelinespec.ExecutorInput{
		Inputs: &pipelinespec.ExecutorInput_Inputs{
			Artifacts: map[string]*pipelinespec.ArtifactList{
				"data": {
					Artifacts: []*pipelinespec.RuntimeArtifact{
						{Uri: "minio://mlpipeline/sample/sample.txt", Metadata: &structpb.Struct{Fields: map[string]*structpb.Value{"_kfp_workspace": structpb.NewBoolValue(true)}}},
					},
				},
			},
		},
	}
	ph, err := getPlaceholders(execIn)
	if err != nil {
		t.Fatalf("getPlaceholders error: %v", err)
	}
	actual := ph["{{$.inputs.artifacts['data'].path}}"]
	expected := filepath.Join(WorkspaceMountPath, ".artifacts", "minio", "mlpipeline", "sample", "sample.txt")
	if actual != expected {
		t.Fatalf("placeholder path mismatch: actual=%q expected=%q", actual, expected)
	}
}

func Test_executorInput_compileCmdAndArgs(t *testing.T) {
	executorInputJSON := `{
		"inputs": {
			"parameterValues": {
				"config": {
					"category_ids": "{{$.inputs.parameters['pipelinechannel--category_ids']}}",
					"dump_filename": "{{$.inputs.parameters['pipelinechannel--dump_filename']}}",
					"sphinx_host": "{{$.inputs.parameters['pipelinechannel--sphinx_host']}}",
					"sphinx_port": "{{$.inputs.parameters['pipelinechannel--sphinx_port']}}"
				},
				"pipelinechannel--category_ids": "116",
				"pipelinechannel--dump_filename": "dump_filename_test.txt",
				"pipelinechannel--sphinx_host": "sphinx-default-host.ru",
				"pipelinechannel--sphinx_port": 9312
			}
		},
		"outputs": {
			"artifacts": {
				"dataset": {
					"artifacts": [{
						"type": {
							"schemaTitle": "system.Dataset",
							"schemaVersion": "0.0.1"
						},
						"uri": "s3://aviflow-stage-kfp-artifacts/debug-component-pipeline/ae02034e-bd96-4b8a-a06b-55c99fe9eccb/sayhello/c98ac032-2448-4637-bf37-3ad1e13a112c/dataset"
					}]
				}
			},
			"outputFile": "/tmp/kfp_outputs/output_metadata.json"
		}
	}`

	executorInput := &pipelinespec.ExecutorInput{}
	err := protojson.Unmarshal([]byte(executorInputJSON), executorInput)

	assert.NoError(t, err)

	cmd := "sh"
	args := []string{
		"--executor_input", "{{$}}",
		"--function_to_execute", "sayHello",
	}
	cmd, args, err = compileCmdAndArgs(executorInput, cmd, args)

	assert.NoError(t, err)

	var actualExecutorInput string
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "--executor_input" {
			actualExecutorInput = args[i+1]
			break
		}
	}
	assert.NotEmpty(t, actualExecutorInput, "--executor_input not found")

	var parsed map[string]any
	err = json.Unmarshal([]byte(actualExecutorInput), &parsed)
	assert.NoError(t, err)

	inputs := parsed["inputs"].(map[string]any)
	paramValues := inputs["parameterValues"].(map[string]any)
	config := paramValues["config"].(map[string]any)

	assert.Equal(t, "116", config["category_ids"])
	assert.Equal(t, "dump_filename_test.txt", config["dump_filename"])
	assert.Equal(t, "sphinx-default-host.ru", config["sphinx_host"])
	assert.Equal(t, "9312", config["sphinx_port"])
}

func Test_get_log_Writer(t *testing.T) {
	old := osCreateFunc
	defer func() { osCreateFunc = old }()

	osCreateFunc = func(name string) (*os.File, error) {
		tmpdir := t.TempDir()
		file, _ := os.CreateTemp(tmpdir, "*")
		return file, nil
	}

	tests := []struct {
		name        string
		artifacts   map[string]*pipelinespec.ArtifactList
		multiWriter bool
	}{
		{
			"single writer - no key logs",
			map[string]*pipelinespec.ArtifactList{
				"notLog": {},
			},
			false,
		},
		{
			"single writer - key log has empty list",
			map[string]*pipelinespec.ArtifactList{
				"logs": {
					Artifacts: []*pipelinespec.RuntimeArtifact{},
				},
			},
			false,
		},
		{
			"single writer - malformed uri",
			map[string]*pipelinespec.ArtifactList{
				"logs": {
					Artifacts: []*pipelinespec.RuntimeArtifact{
						{
							Uri: "",
						},
					},
				},
			},
			false,
		},
		{
			"multiwriter",
			map[string]*pipelinespec.ArtifactList{
				"executor-logs": {
					Artifacts: []*pipelinespec.RuntimeArtifact{
						{
							Uri: "minio://testinguri",
						},
					},
				},
			},
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer := getLogWriter(test.artifacts)
			if test.multiWriter == false {
				assert.Equal(t, os.Stdout, writer)
			} else {
				assert.IsType(t, io.MultiWriter(), writer)
			}
		})
	}
}

func Test_qualifyExecutorLogsURI(t *testing.T) {
	baseURI := "minio://mlpipeline/v2/artifacts/my-pipeline/run-id/always-fail/salt123/executor-logs"
	baseCustomPath := "/minio/mlpipeline/v2/artifacts/my-pipeline/run-id/always-fail/salt123/executor-logs"
	stringPtr := func(s string) *string { return &s }

	tests := []struct {
		name           string
		artifacts      map[string]*pipelinespec.ArtifactList
		retryIndex     string
		wantURI        string
		wantCustomPath *string
	}{
		{
			name: "appends retry index to executor-logs URI",
			artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {Artifacts: []*pipelinespec.RuntimeArtifact{{Uri: baseURI}}},
			},
			retryIndex:     "2",
			wantURI:        baseURI + "-2",
			wantCustomPath: nil,
		},
		{
			name: "appends retry index to executor-logs CustomPath",
			artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {Artifacts: []*pipelinespec.RuntimeArtifact{{
					Uri:        baseURI,
					CustomPath: stringPtr(baseCustomPath),
				}}},
			},
			retryIndex:     "2",
			wantURI:        baseURI + "-2",
			wantCustomPath: stringPtr(baseCustomPath + "-2"),
		},
		{
			name: "no-op when retry index already applied",
			artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {Artifacts: []*pipelinespec.RuntimeArtifact{{
					Uri:        baseURI + "-2",
					CustomPath: stringPtr(baseCustomPath + "-2"),
				}}},
			},
			retryIndex:     "2",
			wantURI:        baseURI + "-2",
			wantCustomPath: stringPtr(baseCustomPath + "-2"),
		},
		{
			name: "no-op when retry index is empty",
			artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {Artifacts: []*pipelinespec.RuntimeArtifact{{Uri: baseURI}}},
			},
			retryIndex:     "",
			wantURI:        baseURI,
			wantCustomPath: nil,
		},
		{
			name:           "no-op when executor-logs key is absent",
			artifacts:      map[string]*pipelinespec.ArtifactList{},
			retryIndex:     "1",
			wantURI:        "", // no artifact to check
			wantCustomPath: nil,
		},
		{
			name: "no-op when executor-logs list is empty",
			artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {Artifacts: []*pipelinespec.RuntimeArtifact{}},
			},
			retryIndex:     "1",
			wantURI:        "", // no artifact to check
			wantCustomPath: nil,
		},
		{
			name: "no-op when executor-logs list has multiple artifacts",
			artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {Artifacts: []*pipelinespec.RuntimeArtifact{
					{Uri: baseURI},
					{Uri: baseURI + "-2"},
				}},
			},
			retryIndex: "1",
			// list len != 1: guard should skip, original URIs unchanged
			wantURI:        baseURI,
			wantCustomPath: nil,
		},
		{
			name:           "no-op when ArtifactList value is nil",
			artifacts:      map[string]*pipelinespec.ArtifactList{"executor-logs": nil},
			retryIndex:     "1",
			wantURI:        "", // nil list: no artifact to check
			wantCustomPath: nil,
		},
		{
			name: "no-op when first artifact is nil",
			artifacts: map[string]*pipelinespec.ArtifactList{
				"executor-logs": {Artifacts: []*pipelinespec.RuntimeArtifact{nil}},
			},
			retryIndex:     "1",
			wantURI:        "", // nil artifact: no URI to check
			wantCustomPath: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.NotPanics(t, func() {
				qualifyExecutorLogsURI(tc.artifacts, tc.retryIndex)
			})
			list, ok := tc.artifacts["executor-logs"]
			if !ok || list == nil || len(list.Artifacts) == 0 || list.Artifacts[0] == nil {
				// Cases where there is nothing to assert on
				return
			}
			assert.Equal(t, tc.wantURI, list.Artifacts[0].Uri)
			if tc.wantCustomPath == nil {
				assert.Nil(t, list.Artifacts[0].CustomPath)
			} else if assert.NotNil(t, list.Artifacts[0].CustomPath) {
				assert.Equal(t, *tc.wantCustomPath, *list.Artifacts[0].CustomPath)
			}
		})
	}
}

func Test_retryIndexFromPodAnnotation(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
		wantIndex  string
		wantErr    bool
	}{
		{
			name:       "parses first attempt (0)",
			annotation: "my-pipeline-abc.root.always-fail.executor(0)",
			wantIndex:  "0",
		},
		{
			name:       "parses fourth retry (4)",
			annotation: "retry-e2e-pzhkb.root.always-fail.executor(4)",
			wantIndex:  "4",
		},
		{
			name:       "no annotation",
			annotation: "",
			wantErr:    true,
		},
		{
			name:       "annotation without parenthesised suffix",
			annotation: "my-pipeline-abc.root.always-fail.executor",
			wantErr:    true,
		},
		{
			name:       "annotation with non-integer index",
			annotation: "my-pipeline-abc.root.always-fail.executor(abc)",
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clientset := fake.NewClientset()
			if tc.annotation != "" {
				pod := &k8score.Pod{}
				pod.Name = "test-pod"
				pod.Namespace = "test-ns"
				pod.Annotations = map[string]string{
					"workflows.argoproj.io/node-name": tc.annotation,
				}
				_, err := clientset.CoreV1().Pods("test-ns").Create(context.Background(), pod, metav1.CreateOptions{})
				assert.NoError(t, err)
			}

			idx, err := retryIndexFromPodAnnotation(context.Background(), clientset, "test-ns", "test-pod")
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantIndex, idx)
			}
		})
	}
}

// Tests happy and unhappy paths for constructing a new LauncherV2
func Test_NewLauncherV2(t *testing.T) {
	var testCmdArgs = []string{"sh", "-c", "echo \"hello world\""}

	disabledCacheClient, _ := cacheutils.NewClient("ml-pipeline.kubeflow", "8887", true, &tls.Config{})
	var testLauncherV2Deps = client_manager.NewFakeClientManager(
		fake.NewClientset(),
		metadata.NewFakeClient(),
		disabledCacheClient,
	)

	var testValidLauncherV2Opts = LauncherV2Options{
		Namespace:         "my-namespace",
		PodName:           "my-pod",
		PodUID:            "abcd",
		MLMDServerAddress: "example.com",
		MLMDServerPort:    "1234",
	}

	type args struct {
		executionID       int64
		executorInputJSON string
		componentSpecJSON string
		cmdArgs           []string
		opts              LauncherV2Options
		cm                client_manager.ClientManagerInterface
	}
	tests := []struct {
		name        string
		args        *args
		expectedErr error
	}{
		{
			name: "happy path",
			args: &args{
				executionID:       1,
				executorInputJSON: "{}",
				componentSpecJSON: "{}",
				cmdArgs:           testCmdArgs,
				opts:              testValidLauncherV2Opts,
				cm:                testLauncherV2Deps,
			},
			expectedErr: nil,
		},
		{
			name: "missing executionID",
			args: &args{
				executionID: 0,
			},
			expectedErr: errors.New("must specify execution ID"),
		},
		{
			name: "invalid executorInput",
			args: &args{
				executionID:       1,
				executorInputJSON: "{",
			},
			expectedErr: errors.New("unexpected EOF"),
		},
		{
			name: "invalid componentSpec",
			args: &args{
				executionID:       1,
				executorInputJSON: "{}",
				componentSpecJSON: "{",
			},
			expectedErr: errors.New("unexpected EOF\ncomponentSpec: {"),
		},
		{
			name: "missing cmdArgs",
			args: &args{
				executionID:       1,
				executorInputJSON: "{}",
				componentSpecJSON: "{}",
				cmdArgs:           []string{},
			},
			expectedErr: errors.New("command and arguments are empty"),
		},
		{
			name: "invalid opts",
			args: &args{
				executionID:       1,
				executorInputJSON: "{}",
				componentSpecJSON: "{}",
				cmdArgs:           testCmdArgs,
				opts:              LauncherV2Options{},
			},
			expectedErr: errors.New("invalid launcher options: must specify Namespace"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := test.args
			_, err := NewLauncherV2(context.Background(), args.executionID, args.executorInputJSON, args.componentSpecJSON, args.cmdArgs, &args.opts, args.cm)
			if test.expectedErr != nil {
				assert.ErrorContains(t, err, test.expectedErr.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func Test_retrieve_artifact_path(t *testing.T) {
	customPath := "/var/lib/kubelet/pods/pod-uid/volumes/kubernetes.io~csi/pvc-uuid/mount"
	tests := []struct {
		name         string
		artifact     *pipelinespec.RuntimeArtifact
		expectedPath string
	}{
		{
			"Artifact with no custom path",
			&pipelinespec.RuntimeArtifact{
				Uri: "gs://bucket/path/to/artifact",
			},
			"/gcs/bucket/path/to/artifact",
		},
		{
			"Artifact with custom path",
			&pipelinespec.RuntimeArtifact{
				Uri:        "gs://bucket/path/to/artifact",
				CustomPath: &customPath,
			},
			customPath,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path, err := retrieveArtifactPath(test.artifact)
			assert.Nil(t, err)
			assert.Equal(t, path, test.expectedPath)
		})
	}
}
