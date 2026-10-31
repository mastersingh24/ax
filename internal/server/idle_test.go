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
	"sync"
	"testing"
	"time"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"

	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/metadata"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// idleServer is an ax-server on a fake Substrate whose actors run on one
// fake runner. The runner answers workspace readiness and reports the status
// the test sets.
type idleServer struct {
	srv    *server.Server
	sub    *substratetest.Server
	locker lock.Locker

	mu     sync.Mutex
	status metadata.RunnerStatus
}

func newIdleServer(t *testing.T) *idleServer {
	t.Helper()
	h := &idleServer{sub: substratetest.NewServer(t), locker: lock.NewMemoryLocker()}
	runner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/readyz":
			w.WriteHeader(http.StatusOK)
		case metadata.StatusPath:
			h.mu.Lock()
			defer h.mu.Unlock()
			_ = json.NewEncoder(w).Encode(h.status)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(runner.Close)
	h.sub.SetWorkerIP(strings.TrimPrefix(runner.URL, "http://"))

	reconciler := controller.NewTaskReconciler(h.sub.Client(), "test-template", v1alpha1.DefaultSystemNamespace, "")
	reconciler.RouterAddr = ""
	h.srv = server.NewServer(memory.NewStore(), reconciler, server.Options{Locker: h.locker})
	return h
}

func (h *idleServer) setStatus(st metadata.RunnerStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.status = st
}

// startTask creates a task and resumes it, so its actor is running.
func (h *idleServer) startTask(t *testing.T, name string, spec *v1alpha1.TaskSpec) {
	t.Helper()
	ctx := context.Background()
	if _, err := h.srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: name},
		Spec:     spec,
	}}); err != nil {
		t.Fatalf("CreateTask(%s): %v", name, err)
	}
	if _, err := h.srv.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Name: name}); err != nil {
		t.Fatalf("ResumeTask(%s): %v", name, err)
	}
}

func (h *idleServer) actorState(t *testing.T, name string) ateapipb.ActorState {
	t.Helper()
	actor, ok := h.sub.Actor("default", name)
	if !ok {
		t.Fatalf("no actor for %s", name)
	}
	return actor.GetStatus().GetState()
}

func autoSuspended(t *testing.T, srv *server.Server, name string) *v1alpha1.Condition {
	t.Helper()
	task, err := srv.GetTask(context.Background(), &v1alpha1.GetTaskRequest{Name: name})
	if err != nil {
		t.Fatalf("GetTask(%s): %v", name, err)
	}
	for _, c := range task.GetStatus().GetConditions() {
		if c.GetType() == controller.ConditionAutoSuspended {
			return c
		}
	}
	return nil
}

func finishingSpec() *v1alpha1.TaskSpec {
	return &v1alpha1.TaskSpec{
		Image:        "alpine",
		Http:         &v1alpha1.TaskHTTP{Port: 8484},
		OnCompletion: v1alpha1.OnCompletionSuspend,
	}
}

func TestSuspendIdleTasks_SuspendsAFinishedTaskAndRecordsWhy(t *testing.T) {
	h := newIdleServer(t)
	h.startTask(t, "finisher", finishingSpec())
	h.startTask(t, "plain", &v1alpha1.TaskSpec{Image: "alpine"})
	h.setStatus(metadata.RunnerStatus{Exited: true, ExitCode: 0})

	h.srv.SuspendIdleTasks(context.Background())

	if got := h.actorState(t, "finisher"); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("finisher actor is %v, want suspended", got)
	}
	if got := h.actorState(t, "plain"); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("a task without spec.onCompletion or spec.idle was touched: %v", got)
	}
	// The reason is stored, and survives a read: only Ready is stripped from
	// the stored status, and AutoSuspended is ax's own condition.
	c := autoSuspended(t, h.srv, "finisher")
	if c == nil || c.GetStatus() != "True" || c.GetReason() != controller.ReasonCompletedSuspended {
		t.Fatalf("AutoSuspended = %v, want True/%s", c, controller.ReasonCompletedSuspended)
	}
}

func TestSuspendIdleTasks_RecordsAWakeByRequest(t *testing.T) {
	h := newIdleServer(t)
	h.startTask(t, "woken", finishingSpec())
	h.setStatus(metadata.RunnerStatus{Exited: true})
	h.srv.SuspendIdleTasks(context.Background())
	if c := autoSuspended(t, h.srv, "woken"); c.GetStatus() != "True" {
		t.Fatalf("not auto-suspended first: %v", c)
	}

	// The router wakes the actor for a request, without ax.
	actor, _ := h.sub.Actor("default", "woken")
	actor.Status = &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING}
	h.sub.SetActor(actor)
	// The woken task is still busy with the request: the runner reports it
	// resumed after its command exited, and not yet idle.
	h.setStatus(metadata.RunnerStatus{Exited: true, ResumedAfterExit: true, InFlight: 1})

	h.srv.SuspendIdleTasks(context.Background())

	c := autoSuspended(t, h.srv, "woken")
	if c.GetStatus() != "False" || c.GetReason() != controller.ReasonResumedByRequest {
		t.Fatalf("AutoSuspended = %v, want False/%s after a wake by request", c, controller.ReasonResumedByRequest)
	}
	if got := h.actorState(t, "woken"); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("a task woken for a request was suspended on the spot: %v", got)
	}
}

func TestSuspendIdleTasks_SkipsLockedTasks(t *testing.T) {
	h := newIdleServer(t)
	h.startTask(t, "busy", finishingSpec())
	h.setStatus(metadata.RunnerStatus{Exited: true})

	unlock, err := h.locker.Lock(context.Background(), "task", "default", "busy")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()

	start := time.Now()
	h.srv.SuspendIdleTasks(context.Background())
	if got := h.actorState(t, "busy"); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("suspended a task whose lock someone else holds: %v", got)
	}
	if time.Since(start) > 10*time.Second {
		t.Error("the pass waited far longer than its lock timeout for a locked task")
	}
}

func TestSuspendIdleTasks_LeavesFailedTasksAlone(t *testing.T) {
	h := newIdleServer(t)
	h.startTask(t, "broken", finishingSpec())
	h.setStatus(metadata.RunnerStatus{Exited: true})
	// A failed resume leaves an ax-owned phase stored; the pass must not
	// act on such a task.
	h.sub.SetErrors(substratetest.Errors{ResumeActor: context.DeadlineExceeded})
	_, _ = h.srv.ResumeTask(context.Background(), &v1alpha1.ResumeTaskRequest{Name: "broken"})
	h.sub.SetErrors(substratetest.Errors{})

	task, err := h.srv.GetTask(context.Background(), &v1alpha1.GetTaskRequest{Name: "broken"})
	if err != nil {
		t.Fatal(err)
	}
	if !v1alpha1.IsAXPhase(task.GetStatus().GetPhase()) {
		t.Skipf("the failed resume left phase %q, not an ax-owned phase; nothing to check", task.GetStatus().GetPhase())
	}
	h.srv.SuspendIdleTasks(context.Background())
	if got := h.actorState(t, "broken"); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("the pass acted on a task in ax phase %q: actor %v", task.GetStatus().GetPhase(), got)
	}
}
