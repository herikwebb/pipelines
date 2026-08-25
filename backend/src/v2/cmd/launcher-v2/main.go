// Copyright 2021 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Launcher command for Kubeflow Pipelines v2.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/kubeflow/pipelines/backend/src/v2/client_manager"
	"github.com/kubeflow/pipelines/backend/src/v2/component"
	"github.com/kubeflow/pipelines/backend/src/v2/config"
	"github.com/spf13/viper"
)

// TODO: use https://github.com/spf13/cobra as a framework to create more complex CLI tools with subcommands.
var (
	copy                    = flag.String("copy", "", "copy this binary to specified destination path")
	pipelineName            = flag.String("pipeline_name", "", "pipeline context name")
	runID                   = flag.String("run_id", "", "pipeline run uid")
	parentDagID             = flag.Int64("parent_dag_id", 0, "parent DAG execution ID")
	executorType            = flag.String("executor_type", "container", "The type of the ExecutorSpec")
	executionID             = flag.Int64("execution_id", 0, "Execution ID of this task.")
	executorInputJSON       = flag.String("executor_input", "", "The JSON-encoded ExecutorInput.")
	componentSpecJSON       = flag.String("component_spec", "", "The JSON-encoded ComponentSpec.")
	importerSpecJSON        = flag.String("importer_spec", "", "The JSON-encoded ImporterSpec.")
	taskSpecJSON            = flag.String("task_spec", "", "The JSON-encoded TaskSpec.")
	podName                 = flag.String("pod_name", "", "Kubernetes Pod name.")
	podUID                  = flag.String("pod_uid", "", "Kubernetes Pod UID.")
	mlPipelineServerAddress = flag.String("ml_pipeline_server_address", "ml-pipeline.kubeflow", "The name of the ML pipeline API server address.")
	mlPipelineServerPort    = flag.String("ml_pipeline_server_port", "8887", "The port of the ML pipeline API server.")
	mlmdServerAddress       = flag.String("mlmd_server_address", "", "The MLMD gRPC server address.")
	mlmdServerPort          = flag.String("mlmd_server_port", "8080", "The MLMD gRPC server port.")
	logLevel                = flag.String("log_level", "1", "The verbosity level to log.")
	publishLogs             = flag.String("publish_logs", "true", "Whether to publish component logs to the object store")
	cacheDisabledFlag       = flag.Bool("cache_disabled", false, "Disable cache globally.")
	caCertPath              = flag.String("ca_cert_path", "", "The path to the CA certificate to trust on connections to the ML pipeline API server and metadata server.")
	mlPipelineTLSEnabled    = flag.Bool("ml_pipeline_tls_enabled", false, "Set to true if mlpipeline API server serves over TLS.")
	metadataTLSEnabled      = flag.Bool("metadata_tls_enabled", false, "Set to true if MLMD serves over TLS.")
)

// Required flags the driver/compiler must always pass to the launcher, grouped
// by executor type, making the implicit contract fail-fast instead of silently
// falling back to defaults. A flag is required for an executor type when the
// driver/compiler always emits it for that type; only copy (a special mode
// selector that short-circuits before validation) stays optional.
var (
	commonRequiredLauncherFlags = []string{
		"executor_type",
		"pipeline_name",
		"run_id",
		"component_spec",
		"pod_name",
		"pod_uid",
		"mlmd_server_address",
		"mlmd_server_port",
		"log_level",
		"publish_logs",
		"cache_disabled",
		"ml_pipeline_tls_enabled",
		"metadata_tls_enabled",
		"ca_cert_path",
	}
	containerRequiredLauncherFlags = []string{
		"execution_id",
		"executor_input",
		"ml_pipeline_server_address",
		"ml_pipeline_server_port",
	}
	importerRequiredLauncherFlags = []string{
		"task_spec",
		"importer_spec",
		"parent_dag_id",
	}
)

// collectProvidedFlags returns the flags explicitly set on the command line.
// flag.Visit reports only flags that were provided, not those left at default.
func collectProvidedFlags() map[string]bool {
	provided := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		provided[f.Name] = true
	})
	return provided
}

func requiredLauncherFlags(executorType string) ([]string, error) {
	required := append([]string{}, commonRequiredLauncherFlags...)
	switch executorType {
	case "container":
		required = append(required, containerRequiredLauncherFlags...)
	case "importer":
		required = append(required, importerRequiredLauncherFlags...)
	default:
		return nil, fmt.Errorf("unsupported executor type %q, must be one of container, importer", executorType)
	}
	return required, nil
}

func validateLauncherFlags(provided map[string]bool, executorType string) error {
	required, err := requiredLauncherFlags(executorType)
	if err != nil {
		return err
	}
	for _, name := range required {
		if !provided[name] {
			return fmt.Errorf("--%s is required for %s executor but was not provided", name, executorType)
		}
	}
	return nil
}

func main() {
	err := run()
	if err != nil {
		glog.Error(err)
		glog.Flush()
		os.Exit(launcherExitCode(err))
	}
}

func launcherExitCode(err error) int {
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		if exitCode := exitError.ExitCode(); exitCode > 0 {
			return exitCode
		}
		if waitStatus, ok := exitError.Sys().(syscall.WaitStatus); ok && waitStatus.Signaled() {
			return 128 + int(waitStatus.Signal())
		}
	}
	var signalCause interface {
		Signal() syscall.Signal
	}
	if errors.As(err, &signalCause) {
		return 128 + int(signalCause.Signal())
	}
	return 1
}

type launcherSignalCause struct {
	signal     syscall.Signal
	receivedAt time.Time
}

func (cause launcherSignalCause) Error() string {
	return fmt.Sprintf("launcher received %s", cause.signal)
}

func (cause launcherSignalCause) Signal() syscall.Signal {
	return cause.signal
}

func (cause launcherSignalCause) ReceivedAt() time.Time {
	return cause.receivedAt
}

func preserveSignalCause(ctx context.Context, err error) error {
	var signalCause interface {
		Signal() syscall.Signal
	}
	cause := context.Cause(ctx)
	if !errors.As(cause, &signalCause) {
		return err
	}
	if err == nil {
		return cause
	}
	if errors.Is(err, cause) {
		return err
	}
	return fmt.Errorf("%w: %w", err, cause)
}

func preserveUnhandledSignalCause(ctx context.Context, err error, completionHandlesSignalCause bool) error {
	if completionHandlesSignalCause {
		return err
	}
	return preserveSignalCause(ctx, err)
}

func completedBeforeCancellation(ctx context.Context, err error) bool {
	return err == nil && ctx.Err() == nil
}

func launcherContextForSignals(
	parent context.Context,
	signals <-chan os.Signal,
	stopNotifications func(),
) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	stopListening := make(chan struct{})
	var stopOnce sync.Once
	go func() {
		for {
			select {
			case receivedSignal, ok := <-signals:
				if !ok {
					return
				}
				// Keep draining notifications after the first signal so repeated
				// SIGTERM deliveries cannot interrupt metadata finalization.
				if ctx.Err() != nil {
					continue
				}
				signalValue, ok := receivedSignal.(syscall.Signal)
				if !ok {
					cancel(fmt.Errorf("launcher received unsupported signal %v", receivedSignal))
					continue
				}
				cancel(launcherSignalCause{signal: signalValue, receivedAt: time.Now()})
			case <-stopListening:
				return
			}
		}
	}()
	return ctx, func() {
		stopOnce.Do(func() {
			if stopNotifications != nil {
				stopNotifications()
			}
			close(stopListening)
			cancel(context.Canceled)
		})
	}
}

func run() (err error) {
	flag.Parse()
	providedFlags := collectProvidedFlags()
	terminationSignals := make(chan os.Signal, 1)
	signal.Notify(terminationSignals, os.Interrupt, syscall.SIGTERM)
	ctx, cancel := launcherContextForSignals(context.Background(), terminationSignals, func() {
		signal.Stop(terminationSignals)
	})
	defer cancel()
	completionHandlesSignalCause := false
	defer func() {
		err = preserveUnhandledSignalCause(ctx, err, completionHandlesSignalCause)
	}()

	glog.Infof("Setting log level to: '%s'", *logLevel)
	err = flag.Set("v", *logLevel)
	if err != nil {
		glog.Warningf("Failed to set log level: %s", err.Error())
	}

	if *copy != "" {
		// copy is used to copy this binary to a shared volume
		// this is a special command, ignore all other flags by returning
		// early
		err := component.CopyThisBinary(*copy)
		completionHandlesSignalCause = completedBeforeCancellation(ctx, err)
		return err
	}

	if err := validateLauncherFlags(providedFlags, *executorType); err != nil {
		return err
	}
	namespace, err := config.InPodNamespace()
	if err != nil {
		return err
	}

	launcherV2Opts := &component.LauncherV2Options{
		Namespace:               namespace,
		PodName:                 *podName,
		PodUID:                  *podUID,
		MLPipelineServerAddress: *mlPipelineServerAddress,
		MLPipelineServerPort:    *mlPipelineServerPort,
		MLMDServerAddress:       *mlmdServerAddress,
		MLMDServerPort:          *mlmdServerPort,
		PipelineName:            *pipelineName,
		RunID:                   *runID,
		PublishLogs:             *publishLogs,
		CacheDisabled:           *cacheDisabledFlag,
		MLPipelineTLSEnabled:    *mlPipelineTLSEnabled,
		MLMDTLSEnabled:          *metadataTLSEnabled,
		CaCertPath:              *caCertPath,
	}

	switch *executorType {
	case "importer":
		importerLauncherOpts := &component.ImporterLauncherOptions{
			PipelineName: *pipelineName,
			RunID:        *runID,
			ParentDagID:  *parentDagID,
		}
		importerLauncher, err := component.NewImporterLauncher(ctx, *componentSpecJSON, *importerSpecJSON, *taskSpecJSON, launcherV2Opts, importerLauncherOpts)
		if err != nil {
			return err
		}
		err = importerLauncher.Execute(ctx)
		// A nil importer result means its metadata publication completed. Keep
		// the process result aligned even if cancellation raced with its return.
		completionHandlesSignalCause = err == nil
		return err
	case "container":
		clientOptions := &client_manager.Options{
			MLPipelineServerAddress: launcherV2Opts.MLPipelineServerAddress,
			MLPipelineServerPort:    launcherV2Opts.MLPipelineServerPort,
			MLMDServerAddress:       launcherV2Opts.MLMDServerAddress,
			MLMDServerPort:          launcherV2Opts.MLMDServerPort,
			CacheDisabled:           launcherV2Opts.CacheDisabled,
			MLMDTLSEnabled:          launcherV2Opts.MLMDTLSEnabled,
			CaCertPath:              launcherV2Opts.CaCertPath,
		}
		clientManager, err := client_manager.NewClientManager(clientOptions)
		if err != nil {
			return err
		}
		launcher, err := component.NewLauncherV2(ctx, *executionID, *executorInputJSON, *componentSpecJSON, flag.Args(), launcherV2Opts, clientManager)
		if err != nil {
			return err
		}
		glog.V(5).Info(launcher.Info())
		// LauncherV2.Execute owns the completion cutoff for container tasks so a
		// signal arriving during detached metadata finalization cannot make the
		// process result disagree with the state already being published.
		completionHandlesSignalCause = true
		return launcher.Execute(ctx)

	}
	return fmt.Errorf("unsupported executor type %s", *executorType)

}

// Use WARNING default logging level to facilitate troubleshooting.
func init() {
	flag.Set("logtostderr", "true")
	// Change the WARNING to INFO level for debugging.
	flag.Set("stderrthreshold", "WARNING")
	viper.AutomaticEnv()
}
