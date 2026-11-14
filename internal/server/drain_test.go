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

package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/metadata"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// drainHarness is a server over a fake Substrate with one task, "t1", whose
// actor the test puts in whatever state it needs.
type drainHarness struct {
	sub *substratetest.Server
	srv *server.Server
	// baseline is how many suspends CreateTask made, so the tests count
	// only the suspends the drain check makes.
	baseline int
}

func newDrainHarness(t *testing.T) *drainHarness {
	t.Helper()
	sub := substratetest.NewServer(t)
	srv := server.NewServer(memory.NewStore(), newTestReconciler(sub))
	if _, err := srv.CreateTask(context.Background(), &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "t1"},
		Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
	}}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	return &drainHarness{sub: sub, srv: srv, baseline: len(sub.SuspendModes("default", "t1"))}
}

// drainSuspends returns the durability modes of t1's suspends since setup.
func (h *drainHarness) drainSuspends() []ateapipb.DurabilityMode {
	return h.sub.SuspendModes("default", "t1")[h.baseline:]
}

// runOn puts t1's actor in RUNNING on a worker whose pod serves the runner
// status endpoint at workerAddr (empty: no worker address).
func (h *drainHarness) runOn(t *testing.T, workerAddr string) {
	t.Helper()
	a, ok := h.sub.Actor("default", "t1")
	if !ok {
		t.Fatal("t1 has no actor")
	}
	a.Status = &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING}
	if workerAddr != "" {
		a.Status.WorkerAssignment = &ateapipb.WorkerAssignment{WorkerPod: "ax-pool-1", WorkerPodIps: []string{workerAddr}}
	}
	h.sub.SetActor(a)
}

func (h *drainHarness) crash(t *testing.T, message string) {
	t.Helper()
	a, ok := h.sub.Actor("default", "t1")
	if !ok {
		t.Fatal("t1 has no actor")
	}
	a.Status = &ateapipb.ActorStatus{
		State: ateapipb.ActorState_ACTOR_STATE_CRASHED,
		Crash: &ateapipb.ActorCrash{Message: message, CrashTime: timestamppb.New(time.Unix(1_800_000_000, 0))},
	}
	h.sub.SetActor(a)
}

func (h *drainHarness) task(t *testing.T) *v1alpha1.Task {
	t.Helper()
	task, err := h.srv.GetTask(context.Background(), &v1alpha1.GetTaskRequest{Name: "t1"})
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	return task
}

func drainCondition(task *v1alpha1.Task) *v1alpha1.Condition {
	for _, c := range task.GetStatus().GetConditions() {
		if c.GetType() == controller.ConditionAutoSuspended {
			return c
		}
	}
	return nil
}

// runnerStatusServer serves the runner status endpoint with inFlight open
// requests and returns its host:port.
func runnerStatusServer(t *testing.T, inFlight int) string {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != metadata.StatusPath {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(metadata.RunnerStatus{InFlight: inFlight})
	}))
	t.Cleanup(ts.Close)
	return strings.TrimPrefix(ts.URL, "http://")
}

func TestHandleWorkerLoss_SuspendsSynchronouslyOncePerDrain(t *testing.T) {
	h := newDrainHarness(t)
	h.runOn(t, runnerStatusServer(t, 2))
	h.sub.SetWorker("w1", ateapipb.WorkerState_WORKER_STATE_DRAINING)
	h.sub.AssignActor("w1", "default", "t1")

	h.srv.HandleWorkerLoss(context.Background())
	h.srv.HandleWorkerLoss(context.Background())

	modes := h.drainSuspends()
	if len(modes) != 1 || modes[0] != ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS {
		t.Fatalf("suspends = %v, want exactly one DURABILITY_MODE_SYNCHRONOUS", modes)
	}
	task := h.task(t)
	if got := task.GetStatus().GetPhase(); got != "Suspended" {
		t.Errorf("phase = %q, want Suspended", got)
	}
	c := drainCondition(task)
	if c == nil || c.GetStatus() != "True" || c.GetReason() != controller.ReasonWorkerDraining {
		t.Fatalf("AutoSuspended = %v, want True/%s", c, controller.ReasonWorkerDraining)
	}
	if !strings.Contains(c.GetMessage(), "w1") || !strings.Contains(c.GetMessage(), "2 requests in flight were interrupted") {
		t.Errorf("message = %q, want the worker and the 2 interrupted requests", c.GetMessage())
	}
}

func TestHandleWorkerLoss_DrainMessageInFlight(t *testing.T) {
	for _, tc := range []struct {
		name     string
		addr     func(t *testing.T) string
		want     string
		notWants []string
	}{
		{"none in flight", func(t *testing.T) string { return runnerStatusServer(t, 0) }, "; no requests in flight", nil},
		{"one in flight", func(t *testing.T) string { return runnerStatusServer(t, 1) }, "(1 request in flight was interrupted)", nil},
		// No worker address and no router: the status can't be read, so the
		// message says nothing about requests, and the suspend still happens.
		{"status unreadable", func(*testing.T) string { return "" }, "suspended synchronously.", []string{"in flight"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newDrainHarness(t)
			h.runOn(t, tc.addr(t))
			h.sub.SetWorker("w1", ateapipb.WorkerState_WORKER_STATE_DRAINING)
			h.sub.AssignActor("w1", "default", "t1")
			h.srv.HandleWorkerLoss(context.Background())

			c := drainCondition(h.task(t))
			if c == nil || c.GetReason() != controller.ReasonWorkerDraining {
				t.Fatalf("AutoSuspended = %v, want reason %s", c, controller.ReasonWorkerDraining)
			}
			if !strings.Contains(c.GetMessage(), tc.want) {
				t.Errorf("message = %q, want it to contain %q", c.GetMessage(), tc.want)
			}
			for _, nw := range tc.notWants {
				if strings.Contains(c.GetMessage(), nw) {
					t.Errorf("message = %q, should not mention %q", c.GetMessage(), nw)
				}
			}
		})
	}
}

func TestHandleWorkerLoss_IgnoresActiveWorkersAndNonTaskActors(t *testing.T) {
	h := newDrainHarness(t)
	h.runOn(t, runnerStatusServer(t, 0))
	h.sub.SetWorker("w1", ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	h.sub.AssignActor("w1", "default", "t1")
	// An actor no task owns, on a draining worker.
	h.sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "not-a-task"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})
	h.sub.SetWorker("w2", ateapipb.WorkerState_WORKER_STATE_DRAINING)
	h.sub.AssignActor("w2", "default", "not-a-task")

	h.srv.HandleWorkerLoss(context.Background())

	if modes := h.drainSuspends(); len(modes) != 0 {
		t.Errorf("t1 on an active worker was suspended: %v", modes)
	}
	if modes := h.sub.SuspendModes("default", "not-a-task"); len(modes) != 0 {
		t.Errorf("an actor no task owns was suspended: %v", modes)
	}
}

func TestHandleWorkerLoss_SkipsSuspendedActorsOnDrainingWorkers(t *testing.T) {
	h := newDrainHarness(t) // CreateTask leaves the actor suspended
	h.sub.SetWorker("w1", ateapipb.WorkerState_WORKER_STATE_DRAINING)
	h.sub.AssignActor("w1", "default", "t1")
	h.srv.HandleWorkerLoss(context.Background())
	if modes := h.drainSuspends(); len(modes) != 0 {
		t.Errorf("a suspended actor was suspended again: %v", modes)
	}
}

func TestHandleWorkerLoss_RevertsWorkerLossCrashOnce(t *testing.T) {
	h := newDrainHarness(t)
	h.crash(t, "worker pod went away while hosting the actor")
	if got := h.task(t).GetStatus().GetPhase(); got != "Failed" {
		t.Fatalf("phase before revert = %q, want Failed", got)
	}

	h.srv.HandleWorkerLoss(context.Background())
	h.srv.HandleWorkerLoss(context.Background())

	if n := h.sub.RevertCalls("default", "t1"); n != 1 {
		t.Fatalf("RevertActor calls = %d, want 1", n)
	}
	task := h.task(t)
	if got := task.GetStatus().GetPhase(); got != "Suspended" {
		t.Errorf("phase after revert = %q, want Suspended", got)
	}
	c := drainCondition(task)
	if c == nil || c.GetStatus() != "True" || c.GetReason() != controller.ReasonRevertedAfterWorkerLoss {
		t.Fatalf("AutoSuspended = %v, want True/%s", c, controller.ReasonRevertedAfterWorkerLoss)
	}
}

func TestHandleWorkerLoss_GivesUpOnARevertThatKeepsFailing(t *testing.T) {
	h := newDrainHarness(t)
	h.crash(t, "worker restarted while hosting the actor")
	h.sub.SetErrors(substratetest.Errors{RevertActor: status.Error(codes.FailedPrecondition, "no snapshot")})

	for range 6 {
		h.srv.HandleWorkerLoss(context.Background())
	}
	if n := h.sub.RevertCalls("default", "t1"); n != 3 {
		t.Fatalf("RevertActor calls = %d, want 3 (then give up)", n)
	}
	if got := h.task(t).GetStatus().GetPhase(); got != "Failed" {
		t.Errorf("phase = %q, want Failed after giving up", got)
	}
}

func TestHandleWorkerLoss_LeavesOtherCrashesAlone(t *testing.T) {
	h := newDrainHarness(t)
	h.crash(t, "atelet RunWorkload: wakeup probe never returned 200")
	h.srv.HandleWorkerLoss(context.Background())
	if n := h.sub.RevertCalls("default", "t1"); n != 0 {
		t.Fatalf("RevertActor calls = %d, want 0 for a crash the worker didn't cause", n)
	}
	if got := h.task(t).GetStatus().GetPhase(); got != "Failed" {
		t.Errorf("phase = %q, want Failed", got)
	}
}

func TestHandleWorkerLoss_MarksTheTaskResumedOnAnotherWorker(t *testing.T) {
	h := newDrainHarness(t) // t1 sets no spec.idle, so the idle check never visits it
	h.runOn(t, runnerStatusServer(t, 0))
	h.sub.SetWorker("w1", ateapipb.WorkerState_WORKER_STATE_DRAINING)
	h.sub.AssignActor("w1", "default", "t1")
	h.srv.HandleWorkerLoss(context.Background())
	if c := drainCondition(h.task(t)); c == nil || c.GetStatus() != "True" {
		t.Fatalf("AutoSuspended = %v, want True after the drain suspend", c)
	}

	// The router wakes it on another worker; the old worker still drains.
	h.sub.SetWorker("w2", ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	h.sub.AssignActor("w2", "default", "t1")
	h.runOn(t, runnerStatusServer(t, 0))
	h.srv.HandleWorkerLoss(context.Background())

	c := drainCondition(h.task(t))
	if c == nil || c.GetStatus() != "False" || c.GetReason() != controller.ReasonResumedByRequest {
		t.Fatalf("AutoSuspended = %v, want False/%s once it runs again", c, controller.ReasonResumedByRequest)
	}
	if got := h.task(t).GetStatus().GetPhase(); got != "Running" {
		t.Errorf("phase = %q, want Running", got)
	}
	if modes := h.drainSuspends(); len(modes) != 1 {
		t.Errorf("suspends = %v, want only the one for the drain", modes)
	}
}
