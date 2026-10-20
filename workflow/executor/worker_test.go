package executor

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v4/util/logging"
	"github.com/argoproj/argo-workflows/v4/workflow/executor/maindriver"
	"github.com/argoproj/argo-workflows/v4/workflow/executor/mocks"
	"github.com/argoproj/argo-workflows/v4/workflow/executor/tracing"
)

func TestWorkerSupports(t *testing.T) {
	cmd := []string{"sh", "-c"}
	tests := map[string]struct {
		tmpl    wfv1.Template
		wantErr string
	}{
		"container":         {tmpl: wfv1.Template{Container: &apiv1.Container{Command: cmd}}},
		"script":            {tmpl: wfv1.Template{Script: &wfv1.ScriptTemplate{Container: apiv1.Container{Command: cmd}}}},
		"no command":        {tmpl: wfv1.Template{Container: &apiv1.Container{}}, wantErr: "command"},
		"valueFrom env":     {tmpl: wfv1.Template{Container: &apiv1.Container{Command: cmd, Env: []apiv1.EnvVar{{Name: "X", ValueFrom: &apiv1.EnvVarSource{}}}}}, wantErr: "valueFrom"},
		"volumeMounts":      {tmpl: wfv1.Template{Container: &apiv1.Container{Command: cmd, VolumeMounts: []apiv1.VolumeMount{{Name: "v"}}}}, wantErr: "volumeMounts"},
		"volumes":           {tmpl: wfv1.Template{Container: &apiv1.Container{Command: cmd}, Volumes: []apiv1.Volume{{Name: "v"}}}, wantErr: "volumes"},
		"sidecars":          {tmpl: wfv1.Template{Container: &apiv1.Container{Command: cmd}, Sidecars: []wfv1.UserContainer{{}}}, wantErr: "sidecars"},
		"containerSet":      {tmpl: wfv1.Template{ContainerSet: &wfv1.ContainerSetTemplate{}}, wantErr: "containerSet"},
		"resource":          {tmpl: wfv1.Template{Resource: &wfv1.ResourceTemplate{}}, wantErr: "resource"},
		"script no command": {tmpl: wfv1.Template{Script: &wfv1.ScriptTemplate{}}, wantErr: "command"},
		"envFrom":           {tmpl: wfv1.Template{Container: &apiv1.Container{Command: cmd, EnvFrom: []apiv1.EnvFromSource{{}}}}, wantErr: "envFrom"},
		"$(VAR) in args":    {tmpl: wfv1.Template{Container: &apiv1.Container{Command: cmd, Args: []string{"echo $(X)"}, Env: []apiv1.EnvVar{{Name: "X", Value: "1"}}}}, wantErr: "$(X)"},
		"$(VAR) in env":     {tmpl: wfv1.Template{Container: &apiv1.Container{Command: cmd, Env: []apiv1.EnvVar{{Name: "X", Value: "1"}, {Name: "Y", Value: "$(X)"}}}}, wantErr: "$(X)"},
		"shell $(cmd)":      {tmpl: wfv1.Template{Container: &apiv1.Container{Command: cmd, Args: []string{"echo $(date)"}}}},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := workerSupports(&tt.tmpl)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestPlanFor_WorkerLayout(t *testing.T) {
	for _, tmpl := range []*wfv1.Template{{Container: &apiv1.Container{}}, {Script: &wfv1.ScriptTemplate{}}} {
		plan := planFor(tmpl, LayoutWorker, nil)
		assert.Equal(t, []string{"check-worker", "create-task-dirs", "stage-files", "load-artifacts"}, stageNames(plan.Prepare))
		assert.Equal(t, []string{"initialize-output", "run-main"}, stageNames(plan.Run))
		assert.Equal(t, []string{"capture-script-result", "save-parameters", "save-artifacts", "save-logs", "report-outputs"}, stageNames(plan.Collect))
	}
}

// A retried node runs again with the same node ID, so its root must start
// empty: the driver appends to stdout, and a stale one would corrupt the
// script result.
func TestCreateTaskDirs_ClearsStaleRoot(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	paths, err := WorkerPaths(t.TempDir(), "node-1")
	require.NoError(t, err)
	stale := filepath.Join(paths.VarRunArgo, "ctr", "main", "stdout")
	require.NoError(t, os.MkdirAll(filepath.Dir(stale), 0o755))
	require.NoError(t, os.WriteFile(stale, []byte("stale"), 0o644))
	we := &WorkflowExecutor{paths: paths}

	require.NoError(t, createTaskDirs.Run(ctx, we))

	assert.NoFileExists(t, stale)
	for _, dir := range []string{filepath.Join(paths.VarRunArgo, "ctr", "main"), paths.InputArtifacts, paths.Staging, paths.Scratch} {
		assert.DirExists(t, dir)
	}
}

func TestCreateTaskDirs_RefusesPodPaths(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	assert.Error(t, createTaskDirs.Run(ctx, &WorkflowExecutor{paths: PodPaths()}))
}

// newWorkerExecutor returns an executor for one in-process worker task,
// with its task dirs created and a recording sink.
func newWorkerExecutor(t *testing.T, tmpl wfv1.Template, deadline time.Time) (context.Context, *WorkflowExecutor, *recordingSink) {
	t.Helper()
	ctx := logging.TestContext(t.Context())
	tr, err := tracing.New(ctx, `argoexec`)
	require.NoError(t, err)
	paths, err := WorkerPaths(t.TempDir(), "node-1")
	require.NoError(t, err)
	p := &Process{
		Tracing:                tr,
		newRuntime:             func(TaskPaths) ContainerRuntimeExecutor { return &mocks.ContainerRuntimeExecutor{} },
		environ:                os.Environ(), //nolint:forbidigo // commands in the test need the test process's PATH
		terminationGracePeriod: 100 * time.Millisecond,
		mainDriver:             maindriver.Container{},
	}
	sink := &recordingSink{}
	we := p.NewExecutor(Task{
		Config: TaskConfig{NodeID: "node-1", Template: tmpl, Deadline: deadline},
		Paths:  paths,
	}, LayoutWorker, sink)
	require.NoError(t, createTaskDirs.Run(ctx, we))
	return ctx, we, sink
}

func TestWorkerCommand(t *testing.T) {
	paths, err := WorkerPaths("w", "n")
	require.NoError(t, err)
	assert.Equal(t, []string{"sh", "-c", "echo hi"},
		workerCommand(&wfv1.Template{Container: &apiv1.Container{Command: []string{"sh", "-c"}, Args: []string{"echo hi"}}}, paths))
	assert.Equal(t, []string{"sh", "-e", paths.ScriptSource()},
		workerCommand(&wfv1.Template{Script: &wfv1.ScriptTemplate{Container: apiv1.Container{Command: []string{"sh"}, Args: []string{"-e"}}, Source: "echo hi"}}, paths))
	// As the controller does, a script with no source runs without one.
	assert.Equal(t, []string{"sh", "-e"},
		workerCommand(&wfv1.Template{Script: &wfv1.ScriptTemplate{Container: apiv1.Container{Command: []string{"sh"}, Args: []string{"-e"}}}}, paths))
}

func TestRunMain_NonZeroExitIsFailureNotError(t *testing.T) {
	ctx, we, sink := newWorkerExecutor(t, wfv1.Template{Container: &apiv1.Container{Command: []string{"sh", "-c", "exit 3"}}}, time.Time{})
	require.NoError(t, runMain.Run(ctx, we))
	require.Len(t, sink.failures, 1)
	assert.Equal(t, "exit status 3", sink.failures[0])
}

func TestRunMain_DriverErrorReturned(t *testing.T) {
	ctx, we, sink := newWorkerExecutor(t, wfv1.Template{Container: &apiv1.Container{Command: []string{"/no/such/binary"}}}, time.Time{})
	require.Error(t, runMain.Run(ctx, we))
	assert.Empty(t, sink.failures)
}

func TestRunMain_DeadlineKillsChild(t *testing.T) {
	ctx, we, sink := newWorkerExecutor(t, wfv1.Template{Container: &apiv1.Container{Command: []string{"sh", "-c", "exec sleep 30"}}}, time.Now().Add(200*time.Millisecond))
	start := time.Now()
	require.NoError(t, runMain.Run(ctx, we))
	assert.Less(t, time.Since(start), 5*time.Second)
	require.NotEmpty(t, sink.failures)
	assert.Equal(t, "Step exceeded its deadline", sink.failures[0])
}

// The command sees the template's literal env and its outputs are staged
// where Collect reads them.
func TestRunMain_StagesOutputParameter(t *testing.T) {
	out := filepath.Join(t.TempDir(), "p")
	tmpl := wfv1.Template{
		Container: &apiv1.Container{Command: []string{"sh", "-c", `printf "$X" > ` + out}, Env: []apiv1.EnvVar{{Name: "X", Value: "v"}}},
		Outputs:   wfv1.Outputs{Parameters: []wfv1.Parameter{{Name: "p", ValueFrom: &wfv1.ValueFrom{Path: out}}}},
	}
	ctx, we, _ := newWorkerExecutor(t, tmpl, time.Time{})
	require.NoError(t, runMain.Run(ctx, we))
	staged, err := os.ReadFile(filepath.Join(we.paths.VarRunArgo, "outputs", "parameters", out))
	require.NoError(t, err)
	assert.Equal(t, "v", string(staged))
}

// Cleanup removes the worker's own input symlinks and task root, never a
// file the user put at an input path.
func TestRemoveTaskRoot_LeavesUserFiles(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	userDir := t.TempDir()
	userFile := filepath.Join(userDir, "a")
	require.NoError(t, os.WriteFile(userFile, []byte("mine"), 0o644))
	ours := filepath.Join(userDir, "b")
	tmpl := wfv1.Template{
		Container: &apiv1.Container{Command: []string{"true"}},
		Inputs:    wfv1.Inputs{Artifacts: wfv1.Artifacts{{Name: "a", Path: userFile}, {Name: "b", Path: ours}}},
	}
	_, we, _ := newWorkerExecutor(t, tmpl, time.Time{})
	require.NoError(t, os.WriteFile(filepath.Join(we.paths.InputArtifacts, "b"), []byte("x"), 0o644))
	require.NoError(t, os.Symlink(filepath.Join(we.paths.InputArtifacts, "b"), ours))

	require.NoError(t, removeTaskRoot.Run(ctx, we))

	assert.FileExists(t, userFile)
	_, err := os.Lstat(ours)
	assert.True(t, os.IsNotExist(err), "our symlink should be gone")
	assert.NoDirExists(t, we.paths.Root)
}

// blockingSink stands in for an unreachable API server: Fail blocks until
// released.
type blockingSink struct {
	recordingSink
	entered chan struct{}
	release chan struct{}
}

func (s *blockingSink) Fail(ctx context.Context, err error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.release
	s.recordingSink.Fail(ctx, err)
}

// Reporting a deadline must not delay stopping main: the sink may retry
// for a long time.
func TestRunMain_DeadlineKillsBeforeReporting(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "terminated")
	ctx, we, _ := newWorkerExecutor(t, wfv1.Template{Container: &apiv1.Container{Command: []string{"sh", "-c",
		`trap 'touch ` + marker + `; exit 1' TERM; sleep 30 & wait`}}}, time.Now().Add(200*time.Millisecond))
	sink := &blockingSink{entered: make(chan struct{}, 1), release: make(chan struct{})}
	we.sink = sink
	done := make(chan error, 1)
	go func() { done <- runMain.Run(ctx, we) }()

	assert.Eventually(t, func() bool { _, err := os.Stat(marker); return err == nil }, 5*time.Second, 20*time.Millisecond,
		"main must get SIGTERM while the sink is still blocked")
	close(sink.release)
	require.NoError(t, <-done)
	assert.Equal(t, []string{"Step exceeded its deadline"}, sink.failures)
}

// startSleeper starts a child the test owns, for a fake driver to report
// as main.
func startSleeper(t *testing.T) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "sleep", "60")
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	return cmd
}

// driverFunc adapts a function to maindriver.MainDriver.
type driverFunc func(context.Context, maindriver.Task, maindriver.OutputStager) (int, error)

func (f driverFunc) Run(ctx context.Context, task maindriver.Task, stager maindriver.OutputStager) (int, error) {
	return f(ctx, task, stager)
}

// Once main has exited the watch is over: a deadline passing while its
// outputs are staged is not a failure, and the (reaped, maybe reused) pid
// is never signalled.
func TestRunMain_DeadlineAfterMainExitsIsIgnored(t *testing.T) {
	sleeper := startSleeper(t)
	ctx, we, sink := newWorkerExecutor(t, wfv1.Template{Container: &apiv1.Container{Command: []string{"true"}}}, time.Now().Add(100*time.Millisecond))
	we.mainDriver = driverFunc(func(_ context.Context, task maindriver.Task, _ maindriver.OutputStager) (int, error) {
		task.OnStart(sleeper.Process.Pid)
		task.OnExit()
		time.Sleep(400 * time.Millisecond) // staging outlasts the deadline
		return 0, nil
	})
	require.NoError(t, runMain.Run(ctx, we))
	assert.Empty(t, sink.failures)
	assert.NoError(t, sleeper.Process.Signal(syscall.Signal(0)), "the pid must not have been signalled")
}

// A panic after main starts must still end the watch.
func TestRunMain_PanicEndsWatch(t *testing.T) {
	sleeper := startSleeper(t)
	ctx, we, sink := newWorkerExecutor(t, wfv1.Template{Container: &apiv1.Container{Command: []string{"true"}}}, time.Time{})
	ctx, cancel := context.WithCancel(ctx)
	we.mainDriver = driverFunc(func(_ context.Context, task maindriver.Task, _ maindriver.OutputStager) (int, error) {
		task.OnStart(sleeper.Process.Pid)
		panic("stager blew up")
	})
	func() {
		defer func() { _ = recover() }()
		_ = runMain.Run(ctx, we)
	}()
	cancel() // the task ends; a leaked watcher would now report "worker stopped" and kill
	time.Sleep(300 * time.Millisecond)
	assert.Empty(t, sink.failures)
	assert.NoError(t, sleeper.Process.Signal(syscall.Signal(0)))
}

// The worker's filesystem is the host's: an input artifact may only be
// linked where nothing of the user's would be replaced.
func TestCheckWorker_InputArtifactPaths(t *testing.T) {
	existing := t.TempDir()
	tmplWithInput := func(path string) wfv1.Template {
		return wfv1.Template{
			Container: &apiv1.Container{Command: []string{"true"}},
			Inputs:    wfv1.Inputs{Artifacts: wfv1.Artifacts{{Name: "in", Path: path}}},
		}
	}
	ctx, we, _ := newWorkerExecutor(t, tmplWithInput(existing), time.Time{})
	err := checkWorker.Run(ctx, we)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	ctx, we, _ = newWorkerExecutor(t, tmplWithInput("/"), time.Time{})
	err = checkWorker.Run(ctx, we)
	require.Error(t, err)

	ctx, we, _ = newWorkerExecutor(t, tmplWithInput(filepath.Join(t.TempDir(), "new")), time.Time{})
	require.NoError(t, checkWorker.Run(ctx, we))
}

func TestCheckWorker_NeedsDriver(t *testing.T) {
	ctx, we, _ := newWorkerExecutor(t, wfv1.Template{Container: &apiv1.Container{Command: []string{"true"}}}, time.Time{})
	we.mainDriver = nil
	assert.Error(t, checkWorker.Run(ctx, we))
}

func TestWorkerPaths_RootIsAbsolute(t *testing.T) {
	p, err := WorkerPaths("relative", "n")
	require.NoError(t, err)
	assert.True(t, filepath.IsAbs(p.Root), p.Root)
}

// Only the exact link runMain made is ours to remove.
func TestRemoveTaskRoot_KeepsLinksThatAreNotOurs(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	userDir := t.TempDir()
	elsewhere := filepath.Join(userDir, "elsewhere")
	require.NoError(t, os.WriteFile(elsewhere, nil, 0o644))
	foreign := filepath.Join(userDir, "foreign")
	require.NoError(t, os.Symlink(elsewhere, foreign))
	lookalike := filepath.Join(userDir, "lookalike")
	tmpl := wfv1.Template{
		Container: &apiv1.Container{Command: []string{"true"}},
		Inputs:    wfv1.Inputs{Artifacts: wfv1.Artifacts{{Name: "f", Path: foreign}, {Name: "l", Path: lookalike}}},
	}
	_, we, _ := newWorkerExecutor(t, tmpl, time.Time{})
	// A link into our inputs, but not to this artifact's own file.
	require.NoError(t, os.Symlink(filepath.Join(we.paths.InputArtifacts, "other"), lookalike))

	require.NoError(t, removeTaskRoot.Run(ctx, we))

	for _, p := range []string{foreign, lookalike} {
		_, err := os.Lstat(p)
		assert.NoError(t, err, "%s must be kept", p)
	}
}

func TestRemoveTaskRoot_NoRootIsNoOp(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	assert.NoError(t, removeTaskRoot.Run(ctx, &WorkflowExecutor{paths: PodPaths()}))
}

// A main that ignores SIGTERM is killed after the grace period.
func TestRunMain_DeadlineEscalatesToSIGKILL(t *testing.T) {
	ctx, we, sink := newWorkerExecutor(t, wfv1.Template{Container: &apiv1.Container{Command: []string{"sh", "-c",
		`trap '' TERM; while true; do sleep 0.1; done`}}}, time.Now().Add(200*time.Millisecond))
	start := time.Now()
	require.NoError(t, runMain.Run(ctx, we))
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Equal(t, []string{"Step exceeded its deadline"}, sink.failures)
}

// When the worker stops (its context ends), main is stopped and the task
// records why.
func TestRunMain_WorkerStopStopsMain(t *testing.T) {
	ctx, we, sink := newWorkerExecutor(t, wfv1.Template{Container: &apiv1.Container{Command: []string{"sh", "-c", "exec sleep 30"}}}, time.Time{})
	ctx, cancel := context.WithCancel(ctx)
	time.AfterFunc(200*time.Millisecond, cancel)
	start := time.Now()
	require.NoError(t, runMain.Run(ctx, we))
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Equal(t, []string{"worker stopped"}, sink.failures)
}
