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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// idleReconciler suspends every task named in suspend and records which tasks
// it was asked about.
type idleReconciler struct {
	fakeReconciler
	mu      sync.Mutex
	checked []string
	suspend map[string]bool
}

func (r *idleReconciler) CheckIdle(_ context.Context, task *v1alpha1.Task) (*v1alpha1.Task, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checked = append(r.checked, task.GetMetadata().GetName())
	if !r.suspend[task.GetMetadata().GetName()] {
		return task, false, nil
	}
	task.Status.Phase = "Suspended"
	task.Status.WorkerIp = ""
	return task, true, nil
}

func (r *idleReconciler) checkedNames() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := append([]string(nil), r.checked...)
	sort.Strings(out)
	return out
}

func saveIdleTask(t *testing.T, st *memory.MemoryStore, name, phase string, spec *v1alpha1.TaskSpec) {
	t.Helper()
	err := st.SaveTask(context.Background(), &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: name, Atespace: "default"},
		Spec:     spec,
		Status:   &v1alpha1.TaskStatus{Phase: phase, WorkerIp: "10.0.0.1"},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSuspendIdleTasks(t *testing.T) {
	ctx := context.Background()
	st := memory.NewStore()
	idle := &v1alpha1.TaskSpec{Http: &v1alpha1.TaskHTTP{Port: 8484}, Idle: &v1alpha1.TaskIdle{SuspendAfter: "10m"}}
	onExit := &v1alpha1.TaskSpec{OnCompletion: v1alpha1.OnCompletionSuspend}

	saveIdleTask(t, st, "idle-running", "Running", idle)
	saveIdleTask(t, st, "idle-suspended", "Suspended", idle)
	saveIdleTask(t, st, "batch-running", "Running", onExit)
	saveIdleTask(t, st, "plain-running", "Running", &v1alpha1.TaskSpec{})
	saveIdleTask(t, st, "idle-failed", "Failed", idle)
	saveIdleTask(t, st, "idle-terminating", v1alpha1.PhaseTerminating, idle)

	rec := &idleReconciler{suspend: map[string]bool{"idle-running": true}}
	srv := server.NewServer(st, server.Options{Reconciler: rec})
	srv.SuspendIdleTasks(ctx)

	want := []string{"batch-running", "idle-running", "idle-suspended"}
	got := rec.checkedNames()
	if len(got) != len(want) {
		t.Fatalf("checked %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("checked %v, want %v", got, want)
		}
	}

	// The decision is recorded where the CLI reads it.
	task, err := st.GetTask(ctx, "default", "idle-running")
	if err != nil {
		t.Fatal(err)
	}
	if task.Status.Phase != "Suspended" || task.Status.WorkerIp != "" {
		t.Errorf("stored status = %+v, want Suspended", task.Status)
	}
	other, _ := st.GetTask(ctx, "default", "batch-running")
	if other.Status.Phase != "Running" {
		t.Errorf("unchanged task's stored phase = %q", other.Status.Phase)
	}
}

func TestSuspendIdleTasks_SkipsLockedTasks(t *testing.T) {
	ctx := context.Background()
	st := memory.NewStore()
	idle := &v1alpha1.TaskSpec{Http: &v1alpha1.TaskHTTP{Port: 8484}, Idle: &v1alpha1.TaskIdle{SuspendAfter: "10m"}}
	saveIdleTask(t, st, "busy-with-api", "Running", idle)

	locker := lock.NewMemoryLocker()
	unlock, err := locker.Lock(ctx, "task", "default", "busy-with-api")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()

	rec := &idleReconciler{suspend: map[string]bool{"busy-with-api": true}}
	srv := server.NewServer(st, server.Options{Reconciler: rec, Locker: locker})
	start := time.Now()
	srv.SuspendIdleTasks(ctx)
	if got := rec.checkedNames(); len(got) != 0 {
		t.Errorf("checked a task another call holds the lock for: %v", got)
	}
	if time.Since(start) > 10*time.Second {
		t.Error("pass waited too long for a locked task")
	}
}

func TestRunIdleSuspender_NoopWithoutIdleChecker(t *testing.T) {
	srv := server.NewServer(memory.NewStore(), server.Options{Reconciler: &fakeReconciler{}})
	done := make(chan struct{})
	go func() {
		srv.RunIdleSuspender(context.Background(), time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunIdleSuspender should return at once for a reconciler without CheckIdle")
	}
}
