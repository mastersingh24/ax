// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/metadata"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// idleHarness is a reconciler wired to a mock Substrate whose actor state the
// test sets, and a fake runner serving the status endpoint.
type idleHarness struct {
	mock       *mockControlServer
	reconciler *controller.TaskReconciler
	runner     *httptest.Server

	mu          sync.Mutex
	actorState  ateapipb.ActorState
	workerIP    string
	status      metadata.RunnerStatus
	statusCode  int
	statusCalls atomic.Int32
	getCalls    atomic.Int32
	lastTarget  string

	clock time.Time
}

func newIdleHarness(t *testing.T) *idleHarness {
	t.Helper()
	h := &idleHarness{
		actorState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
		statusCode: http.StatusOK,
		clock:      time.Unix(1_000_000, 0),
	}

	h.runner = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != metadata.StatusPath {
			http.NotFound(w, r)
			return
		}
		h.statusCalls.Add(1)
		h.mu.Lock()
		defer h.mu.Unlock()
		h.lastTarget = r.Header.Get("ate-target-actor")
		if h.statusCode != http.StatusOK {
			w.WriteHeader(h.statusCode)
			return
		}
		_ = json.NewEncoder(w).Encode(h.status)
	}))
	t.Cleanup(h.runner.Close)
	h.workerIP = strings.TrimPrefix(h.runner.URL, "http://")

	h.mock = &mockControlServer{}
	h.mock.getActorFunc = func(_ context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
		h.getCalls.Add(1)
		h.mu.Lock()
		defer h.mu.Unlock()
		st := &ateapipb.ActorStatus{State: h.actorState}
		if h.actorState == ateapipb.ActorState_ACTOR_STATE_RUNNING {
			st.WorkerAssignment = &ateapipb.WorkerAssignment{WorkerPodIps: []string{h.workerIP}}
		}
		return &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: req.GetActor().GetName()}, Status: st}, nil
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, h.mock)
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)

	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })

	h.reconciler = controller.NewTaskReconciler(client, "test-template", "ax-system")
	h.reconciler.SecretResolver = noSecrets
	h.reconciler.RouterAddr = ""
	h.reconciler.Now = func() time.Time {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.clock
	}
	return h
}

func (h *idleHarness) set(f func(h *idleHarness)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f(h)
}

func (h *idleHarness) advance(d time.Duration) {
	h.set(func(h *idleHarness) { h.clock = h.clock.Add(d) })
}

func (h *idleHarness) check(t *testing.T, task *v1alpha1.Task) (*v1alpha1.Task, bool) {
	t.Helper()
	got, changed, err := h.reconciler.CheckIdle(context.Background(), task)
	if err != nil {
		t.Fatalf("CheckIdle: %v", err)
	}
	return got, changed
}

func idleTask(name, phase string) *v1alpha1.Task {
	return &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: name, Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Http: &v1alpha1.TaskHTTP{Port: 8484},
			Idle: &v1alpha1.TaskIdle{SuspendAfter: "10m"},
		},
		Status: &v1alpha1.TaskStatus{Phase: phase, WorkerIp: "10.0.0.1"},
	}
}

func TestCheckIdle_SuspendsIdleTask(t *testing.T) {
	h := newIdleHarness(t)
	task := idleTask("idler", "Running")

	// The runner's clock says idle for an hour, but the control plane has only
	// just seen the actor running: it might have been restored from an old
	// snapshot a moment ago.
	h.set(func(h *idleHarness) { h.status = metadata.RunnerStatus{IdleSeconds: 3600} })
	if got, _ := h.check(t, task); got.Status.Phase != "Running" || len(h.mock.suspendedActors) != 0 {
		t.Fatal("suspended a task first seen running just now")
	}

	// A busy agent is left alone however long it has been quiet.
	h.advance(15 * time.Minute)
	h.set(func(h *idleHarness) { h.status = metadata.RunnerStatus{IdleSeconds: 4500, Busy: true} })
	if got, _ := h.check(t, task); got.Status.Phase != "Running" || len(h.mock.suspendedActors) != 0 {
		t.Fatal("suspended a busy task")
	}

	// So is one with a request open.
	h.set(func(h *idleHarness) { h.status = metadata.RunnerStatus{InFlight: 1} })
	if got, _ := h.check(t, task); got.Status.Phase != "Running" || len(h.mock.suspendedActors) != 0 {
		t.Fatal("suspended a task with a request in flight")
	}

	h.set(func(h *idleHarness) { h.status = metadata.RunnerStatus{IdleSeconds: 700} })
	got, changed := h.check(t, task)
	if !changed {
		t.Fatal("idle task was not suspended")
	}
	if len(h.mock.suspendedActors) != 1 || h.mock.suspendedActors[0] != "idler" {
		t.Errorf("suspended actors = %v, want [idler]", h.mock.suspendedActors)
	}
	if got.Status.Phase != "Suspended" || got.Status.WorkerIp != "" {
		t.Errorf("status = %+v, want Suspended with no worker", got.Status)
	}
	assertCondition(t, got, "Ready", "False", controller.ReasonIdleSuspended)
}

func TestCheckIdle_SuspendsOnCompletion(t *testing.T) {
	h := newIdleHarness(t)
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "batch", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{Command: []string{"true"}, OnCompletion: v1alpha1.OnCompletionSuspend},
		Status:   &v1alpha1.TaskStatus{Phase: "Running"},
	}

	h.set(func(h *idleHarness) { h.status = metadata.RunnerStatus{IdleSeconds: 3600} })
	if got, _ := h.check(t, task); got.Status.Phase != "Running" || len(h.mock.suspendedActors) != 0 {
		t.Fatal("suspended a task whose command is still running")
	}

	// No waiting period: the command is done.
	h.set(func(h *idleHarness) { h.status = metadata.RunnerStatus{Exited: true, ExitCode: 7} })
	got, changed := h.check(t, task)
	if !changed || got.Status.Phase != "Suspended" {
		t.Fatalf("finished task was not suspended: %+v", got.Status)
	}
	assertCondition(t, got, "Ready", "False", controller.ReasonCompletedSuspended)
	for _, c := range got.Status.Conditions {
		if c.Type == "Ready" && !strings.Contains(c.Message, "code 7") {
			t.Errorf("Ready message should carry the exit code: %q", c.Message)
		}
	}
}

func TestCheckIdle_NeverPollsSuspendedActors(t *testing.T) {
	h := newIdleHarness(t)
	h.set(func(h *idleHarness) {
		h.actorState = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
		h.status = metadata.RunnerStatus{Exited: true, IdleSeconds: 3600}
	})

	for _, state := range []ateapipb.ActorState{
		ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
		ateapipb.ActorState_ACTOR_STATE_RESUMING,
		ateapipb.ActorState_ACTOR_STATE_CRASHED,
	} {
		h.set(func(h *idleHarness) { h.actorState = state })
		if _, changed := h.check(t, idleTask("sleeper", "Suspended")); changed {
			t.Errorf("%v: status changed for a suspended task", state)
		}
	}
	if n := h.statusCalls.Load(); n != 0 {
		t.Errorf("runner was asked for status %d times while not running; that would wake it", n)
	}
	if len(h.mock.suspendedActors) != 0 {
		t.Errorf("suspend called on a suspended actor: %v", h.mock.suspendedActors)
	}
}

func TestCheckIdle_RecordsResumeByRequest(t *testing.T) {
	h := newIdleHarness(t)
	h.set(func(h *idleHarness) { h.status = metadata.RunnerStatus{InFlight: 1} })

	// The router woke the actor for a request; ax still says Suspended.
	got, changed := h.check(t, idleTask("woken", "Suspended"))
	if !changed || got.Status.Phase != "Running" || got.Status.WorkerIp != h.workerIP {
		t.Fatalf("resume by request not recorded: changed=%v status=%+v", changed, got.Status)
	}
	assertCondition(t, got, "Ready", "True", controller.ReasonResumedByRequest)
	if len(h.mock.suspendedActors) != 0 {
		t.Error("suspended a task serving a request")
	}
}

func TestCheckIdle_RecordsSuspendOutsideAX(t *testing.T) {
	h := newIdleHarness(t)
	h.set(func(h *idleHarness) { h.actorState = ateapipb.ActorState_ACTOR_STATE_SUSPENDED })

	got, changed := h.check(t, idleTask("frozen", "Running"))
	if !changed || got.Status.Phase != "Suspended" || got.Status.WorkerIp != "" {
		t.Fatalf("suspend outside AX not recorded: changed=%v status=%+v", changed, got.Status)
	}
	assertCondition(t, got, "Ready", "False", controller.ReasonActorSuspended)
	if h.statusCalls.Load() != 0 {
		t.Error("runner polled for a suspended actor")
	}
}

func TestCheckIdle_IgnoresTasksWithoutPolicy(t *testing.T) {
	h := newIdleHarness(t)
	for _, task := range []*v1alpha1.Task{
		{Metadata: &v1alpha1.ObjectMeta{Name: "plain"}, Spec: &v1alpha1.TaskSpec{}, Status: &v1alpha1.TaskStatus{Phase: "Running"}},
		{Metadata: &v1alpha1.ObjectMeta{Name: "keep"}, Spec: &v1alpha1.TaskSpec{OnCompletion: v1alpha1.OnCompletionKeep}, Status: &v1alpha1.TaskStatus{Phase: "Running"}},
		idleTask("failed", "Failed"),
		idleTask("terminating", v1alpha1.PhaseTerminating),
	} {
		if _, changed := h.check(t, task); changed {
			t.Errorf("%s: status changed", task.Metadata.Name)
		}
	}
	if n := h.getCalls.Load(); n != 0 {
		t.Errorf("GetActor called %d times for tasks CheckIdle should skip", n)
	}
}

func TestCheckIdle_OldRunnerWithoutStatus(t *testing.T) {
	h := newIdleHarness(t)
	h.set(func(h *idleHarness) { h.statusCode = http.StatusNotFound })
	task := idleTask("old-runner", "Running")
	h.check(t, task)
	h.advance(time.Hour)
	if got, _ := h.check(t, task); got.Status.Phase != "Running" || len(h.mock.suspendedActors) != 0 {
		t.Fatal("suspended a task whose runner reports no status")
	}
}

func TestCheckIdle_FallsBackToRouter(t *testing.T) {
	h := newIdleHarness(t)
	routerAddr := h.workerIP
	h.reconciler.RouterAddr = routerAddr
	h.set(func(h *idleHarness) {
		h.workerIP = "127.0.0.1:1" // not reachable from the control plane
		h.status = metadata.RunnerStatus{Exited: true}
	})
	task := idleTask("routed", "Running")
	task.Spec.OnCompletion = v1alpha1.OnCompletionSuspend

	got, changed := h.check(t, task)
	if !changed || got.Status.Phase != "Suspended" {
		t.Fatalf("status through the router was not used: %+v", got.Status)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lastTarget != "default/routed" {
		t.Errorf("ate-target-actor = %q, want default/routed", h.lastTarget)
	}
}

// A worker address that swallows the request instead of refusing it (network
// policy dropping traffic to worker pods) must not use up the router
// fallback's time as well.
func TestCheckIdle_FallsBackToRouterWhenWorkerHangs(t *testing.T) {
	hang, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer hang.Close() // never accepted: requests to it just wait

	h := newIdleHarness(t)
	routerAddr := h.workerIP
	h.reconciler.RouterAddr = routerAddr
	h.set(func(h *idleHarness) {
		h.workerIP = hang.Addr().String()
		h.status = metadata.RunnerStatus{Exited: true}
	})
	task := idleTask("hanging-worker", "Running")
	task.Spec.OnCompletion = v1alpha1.OnCompletionSuspend

	got, changed := h.check(t, task)
	if !changed || got.Status.Phase != "Suspended" {
		t.Fatalf("router fallback was not reached after a hanging worker read: %+v", got.Status)
	}
}

func TestTaskReconciler_PassesOnlyBusyPathToRunner(t *testing.T) {
	h := newIdleHarness(t)
	task := idleTask("launch", "Suspended")
	task.Spec.Idle.BusyPath = "/agent/busy"
	task.Spec.OnCompletion = v1alpha1.OnCompletionSuspend

	if _, err := h.reconciler.Reconcile(context.Background(), task); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var taskYAML string
	for _, e := range h.mock.lastTemplate.GetContainers()[0].GetEnv() {
		if e.GetName() == "AX_TASK_YAML" {
			taskYAML = e.GetValue()
		}
	}
	if !strings.Contains(taskYAML, "busyPath: /agent/busy") {
		t.Errorf("AX_TASK_YAML should carry the busy path:\n%s", taskYAML)
	}
	for _, unwanted := range []string{"suspendAfter", "onCompletion"} {
		if strings.Contains(taskYAML, unwanted) {
			t.Errorf("AX_TASK_YAML should not carry %s, which only the control plane uses:\n%s", unwanted, taskYAML)
		}
	}
	// The stored task keeps its full spec.
	if task.Spec.GetIdle().GetSuspendAfter() != "10m" || task.Spec.GetOnCompletion() != v1alpha1.OnCompletionSuspend {
		t.Errorf("Reconcile modified the task's own spec: %v", task.Spec)
	}
}
