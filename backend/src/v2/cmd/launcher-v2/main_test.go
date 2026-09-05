package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type unsupportedTestSignal string

func (signal unsupportedTestSignal) Signal() {}

func (signal unsupportedTestSignal) String() string {
	return string(signal)
}

func TestLauncherExitCode(t *testing.T) {
	tests := []struct {
		name    string
		command string
		want    int
		linux   bool
	}{
		{
			name:    "preserves process exit code",
			command: "exit 42",
			want:    42,
		},
		{
			name:    "preserves terminating signal",
			command: "kill -TERM $$",
			want:    143,
			linux:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.linux && runtime.GOOS != "linux" {
				t.Skip("shell signal exit behavior is Linux-specific")
			}
			err := exec.Command("sh", "-c", tc.command).Run()
			assert.Equal(t, tc.want, launcherExitCode(fmt.Errorf("wrapped launcher error: %w", err)))
		})
	}

	assert.Equal(t, 1, launcherExitCode(errors.New("launcher setup failed")))
	assert.Equal(t, 130, launcherExitCode(launcherSignalCause{signal: syscall.SIGINT}))
	assert.Equal(t, 143, launcherExitCode(launcherSignalCause{signal: syscall.SIGTERM}))
}

func TestLauncherContextForSignalsPreservesSignalAndDrainsRepeats(t *testing.T) {
	signals := make(chan os.Signal, 2)
	notificationsStopped := make(chan struct{})
	ctx, cancel := launcherContextForSignals(context.Background(), signals, func() {
		close(notificationsStopped)
	})
	signals <- syscall.SIGINT

	require.Eventually(t, func() bool {
		return ctx.Err() != nil
	}, time.Second, 10*time.Millisecond)
	var signalCause launcherSignalCause
	require.ErrorAs(t, context.Cause(ctx), &signalCause)
	assert.Equal(t, syscall.SIGINT, signalCause.Signal())
	assert.False(t, signalCause.ReceivedAt().IsZero())

	signals <- syscall.SIGTERM
	require.Eventually(t, func() bool {
		return len(signals) == 0
	}, time.Second, 10*time.Millisecond)
	require.ErrorAs(t, context.Cause(ctx), &signalCause)
	assert.Equal(t, syscall.SIGINT, signalCause.Signal(), "the first signal must remain the cancellation cause")
	select {
	case <-notificationsStopped:
		t.Fatal("signal notifications stopped before launcher cleanup")
	default:
	}

	cancel()
	select {
	case <-notificationsStopped:
	case <-time.After(time.Second):
		t.Fatal("signal notifications were not stopped during launcher cleanup")
	}
}

func TestLauncherContextForSignalsRejectsUnsupportedSignal(t *testing.T) {
	signals := make(chan os.Signal, 1)
	ctx, cancel := launcherContextForSignals(context.Background(), signals, nil)
	defer cancel()
	signals <- unsupportedTestSignal("unexpected")

	require.Eventually(t, func() bool {
		return ctx.Err() != nil
	}, time.Second, 10*time.Millisecond)
	assert.EqualError(t, context.Cause(ctx), "launcher received unsupported signal unexpected")
}

func TestPreserveSignalCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	signalCause := launcherSignalCause{signal: syscall.SIGTERM, receivedAt: time.Now()}
	cancel(signalCause)
	operationErr := errors.New("metadata operation canceled")

	err := preserveSignalCause(ctx, operationErr)

	assert.ErrorIs(t, err, operationErr)
	assert.ErrorIs(t, err, signalCause)
	assert.Equal(t, 143, launcherExitCode(err))
}

func TestPreserveSignalCauseAfterSuccessfulOperation(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	signalCause := launcherSignalCause{signal: syscall.SIGINT, receivedAt: time.Now()}
	cancel(signalCause)

	err := preserveSignalCause(ctx, nil)

	assert.ErrorIs(t, err, signalCause)
	assert.Equal(t, 130, launcherExitCode(err))
}

func TestPreserveSignalCauseLeavesUnrelatedCancellationUnchanged(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("unrelated cancellation"))
	operationErr := errors.New("operation failed")

	assert.Same(t, operationErr, preserveSignalCause(ctx, operationErr))
}

func TestPreserveSignalCauseDoesNotDoubleWrap(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	signalCause := launcherSignalCause{signal: syscall.SIGTERM, receivedAt: time.Now()}
	cancel(signalCause)
	operationErr := fmt.Errorf("operation stopped: %w", signalCause)

	assert.Same(t, operationErr, preserveSignalCause(ctx, operationErr))
}

func TestPreserveUnhandledSignalCauseHonorsCompletionOwner(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(launcherSignalCause{signal: syscall.SIGTERM, receivedAt: time.Now()})

	assert.NoError(t, preserveUnhandledSignalCause(ctx, nil, true))
	assert.Error(t, preserveUnhandledSignalCause(ctx, nil, false))
}

func TestCompletedBeforeCancellation(t *testing.T) {
	liveCtx := context.Background()
	assert.True(t, completedBeforeCancellation(liveCtx, nil))
	assert.False(t, completedBeforeCancellation(liveCtx, errors.New("operation failed")))

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, completedBeforeCancellation(canceledCtx, nil))
}

func allProvided(flags []string) map[string]bool {
	provided := make(map[string]bool, len(flags))
	for _, name := range flags {
		provided[name] = true
	}
	return provided
}

func TestRequiredLauncherFlags(t *testing.T) {
	common := []string{
		"executor_type", "pipeline_name", "run_id", "component_spec",
		"pod_name", "pod_uid", "mlmd_server_address", "mlmd_server_port",
		"log_level", "publish_logs", "cache_disabled", "ml_pipeline_tls_enabled",
		"metadata_tls_enabled", "ca_cert_path",
	}
	withCommon := func(extra ...string) []string {
		return append(append([]string{}, common...), extra...)
	}
	tests := []struct {
		executorType string
		want         []string
	}{
		{executorType: "container", want: withCommon("execution_id", "executor_input", "ml_pipeline_server_address", "ml_pipeline_server_port")},
		{executorType: "importer", want: withCommon("task_spec", "importer_spec", "parent_dag_id")},
	}
	for _, tc := range tests {
		t.Run(tc.executorType, func(t *testing.T) {
			got, err := requiredLauncherFlags(tc.executorType)
			assert.NoError(t, err)
			assert.ElementsMatch(t, tc.want, got)
		})
	}

	_, err := requiredLauncherFlags("unknown")
	assert.Error(t, err)
}

func TestValidateLauncherFlags(t *testing.T) {
	tests := []struct {
		name         string
		executorType string
		omit         []string
		wantErr      bool
	}{
		{
			name:         "container with all required flags",
			executorType: "container",
		},
		{
			name:         "importer with all required flags",
			executorType: "importer",
		},
		{
			name:         "container missing execution_id",
			executorType: "container",
			omit:         []string{"execution_id"},
			wantErr:      true,
		},
		{
			name:         "container missing common flag pod_name",
			executorType: "container",
			omit:         []string{"pod_name"},
			wantErr:      true,
		},
		{
			name:         "importer missing importer_spec",
			executorType: "importer",
			omit:         []string{"importer_spec"},
			wantErr:      true,
		},
		{
			name:         "importer missing executor_type",
			executorType: "importer",
			omit:         []string{"executor_type"},
			wantErr:      true,
		},
		{
			name:         "container missing cache_disabled",
			executorType: "container",
			omit:         []string{"cache_disabled"},
			wantErr:      true,
		},
		{
			name:         "importer missing log_level",
			executorType: "importer",
			omit:         []string{"log_level"},
			wantErr:      true,
		},
		{
			name:         "unsupported executor type",
			executorType: "unknown",
			wantErr:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			required, err := requiredLauncherFlags(tc.executorType)
			if err != nil {
				assert.True(t, tc.wantErr)
				assert.Error(t, validateLauncherFlags(map[string]bool{}, tc.executorType))
				return
			}
			provided := allProvided(required)
			for _, name := range tc.omit {
				delete(provided, name)
			}
			err = validateLauncherFlags(provided, tc.executorType)
			assert.Equal(t, tc.wantErr, err != nil, "unexpected error state: %v", err)
		})
	}
}
