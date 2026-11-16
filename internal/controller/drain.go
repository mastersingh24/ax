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

package controller

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Reasons recorded on the AutoSuspended condition when a worker goes away.
const (
	// ReasonWorkerDraining: the actor's worker started draining and AX
	// suspended the actor before the worker shut down.
	ReasonWorkerDraining = "WorkerDraining"
	// ReasonRevertedAfterWorkerLoss: the actor crashed because its worker
	// went away, and AX reverted it to its latest snapshot.
	ReasonRevertedAfterWorkerLoss = "RevertedAfterWorkerLoss"
	// ReasonRevertedAfterFailedSuspend: AX tried to suspend the actor off a
	// draining worker, the suspend failed, the actor ended CRASHED, and AX
	// reverted it to its latest snapshot.
	ReasonRevertedAfterFailedSuspend = "RevertedAfterFailedSuspend"

	// maxRevertAttempts bounds how often one crash is retried before AX
	// leaves the task Failed.
	maxRevertAttempts = 3
	// drainStatusTimeout bounds the best-effort read of the runner's status
	// that only feeds the condition message.
	drainStatusTimeout = 2 * time.Second
	// failedSuspendTTL is how long the drain check keeps watching an actor
	// whose drain suspend failed. Agent Substrate v1.0 crashes an actor that
	// is not suspended within 30 minutes of its worker starting to drain, so
	// this outlasts that window.
	failedSuspendTTL = 45 * time.Minute
	// maxErrorSummary bounds the suspend error quoted in the condition
	// message.
	maxErrorSummary = 300
)

// failedDrainSuspend is what the drain check remembers about a drain suspend
// that failed.
type failedDrainSuspend struct {
	// worker is the draining worker the actor was on.
	worker string
	// err is the latest suspend error.
	err string
	// since is when the first suspend of this episode failed.
	since time.Time
	// revertAttempts counts failed reverts.
	revertAttempts int
}

// DrainingActors returns the actors hosted on draining workers, keyed by
// atespace/name, with the name of the worker each is on.
//
// Agent Substrate marks a worker DRAINING when its pod starts terminating,
// stops placing actors on it, and gives the actors it hosts SIGTERM and a
// grace period (30 minutes in v1.0) to be suspended. It does not suspend them
// itself: an actor still running at the end of the grace period becomes
// CRASHED and loses its work since its last snapshot.
func (r *TaskReconciler) DrainingActors(ctx context.Context) (map[string]string, error) {
	workers, err := r.client.ListDrainingWorkers(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string)
	for _, w := range workers {
		actors, err := r.client.ListWorkerActors(ctx, w)
		if err != nil {
			return out, err
		}
		for _, a := range actors {
			out[a.GetAtespace()+"/"+a.GetName()] = w.GetMetadata().GetName()
		}
	}
	return out, nil
}

// SuspendForDrain suspends the task's actor, synchronously, because the
// worker hosting it is draining. It does nothing unless the actor is running,
// and suspends an actor at most once per drain episode. It returns the task
// and whether its stored status changed.
//
// The suspend waits for the snapshot to reach durable storage: the node is
// going away, and an asynchronous suspend can lose the upload with it. The
// next request resumes the task on another worker. A request in flight when
// the actor is frozen is cut off.
func (r *TaskReconciler) SuspendForDrain(ctx context.Context, task *v1alpha1.Task, worker string) (*v1alpha1.Task, bool, error) {
	if task.GetMetadata() == nil || v1alpha1.IsAXPhase(task.GetStatus().GetPhase()) {
		return task, false, nil
	}
	atespace, name := taskKey(task)
	key := atespace + "/" + name

	r.drainMu.Lock()
	done := r.drainSuspended[key] == worker
	r.drainMu.Unlock()
	if done {
		return task, false, nil
	}

	actor, err := r.client.GetActor(ctx, atespace, name)
	if err != nil {
		return task, false, fmt.Errorf("getting actor %s: %w", key, err)
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		// Suspending, suspended, resuming and so on: nothing to save, or
		// Substrate is already moving it.
		return task, false, nil
	}

	// Whether requests were open, for the message only. A read that fails
	// says nothing and must not delay or block the suspend.
	inFlight := -1
	sctx, cancel := context.WithTimeout(ctx, drainStatusTimeout)
	if st, err := r.runnerStatus(sctx, atespace, name, substrate.WorkerIP(actor)); err == nil {
		inFlight = st.InFlight
	}
	cancel()

	slog.Info("worker is draining; suspending task", "task", key, "worker", worker)
	if err := r.client.SuspendActorSynchronous(ctx, atespace, name); err != nil {
		// Remember the failure: if the actor ends up CRASHED, which
		// happens when the worker goes away, RevertIfDrainSuspendFailed
		// reverts it to its latest snapshot.
		r.drainMu.Lock()
		f := r.suspendFailed[key]
		if f == nil || f.worker != worker {
			f = &failedDrainSuspend{worker: worker, since: r.Now()}
			r.suspendFailed[key] = f
		}
		f.err = err.Error()
		r.drainMu.Unlock()
		return task, false, fmt.Errorf("suspending actor %s off draining worker %s: %w", key, worker, err)
	}
	r.forgetRunning(key)
	r.drainMu.Lock()
	r.drainSuspended[key] = worker
	delete(r.suspendFailed, key)
	r.drainMu.Unlock()

	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	setCondition(task, ConditionAutoSuspended, "True", ReasonWorkerDraining, drainMessage(worker, inFlight), r.Now())
	return task, true, nil
}

// drainMessage is the AutoSuspended message for a drain suspend. inFlight is
// negative when the runner's status could not be read.
func drainMessage(worker string, inFlight int) string {
	msg := fmt.Sprintf("Worker %s is draining; suspended synchronously", worker)
	switch {
	case inFlight == 1:
		msg += " (1 request in flight was interrupted)"
	case inFlight > 1:
		msg += fmt.Sprintf(" (%d requests in flight were interrupted)", inFlight)
	case inFlight == 0:
		msg += "; no requests in flight"
	}
	return msg + ". The next request resumes it on another worker."
}

// RevertIfWorkerLost reverts the task's actor to its latest snapshot when it
// crashed because its worker went away: a drain that ended before the actor
// was suspended, a failed snapshot, a reclaim with too little notice, or a
// worker restart. A crash with any other cause is left alone, so a task that
// fails as it starts does not loop. One crash is reverted at most once, and a
// revert that keeps failing is given up after maxRevertAttempts.
//
// It returns the task and whether its stored status changed.
func (r *TaskReconciler) RevertIfWorkerLost(ctx context.Context, task *v1alpha1.Task) (*v1alpha1.Task, bool, error) {
	if task.GetMetadata() == nil || task.GetStatus().GetPhase() == v1alpha1.PhaseTerminating || task.GetStatus().GetPhase() == v1alpha1.PhasePending {
		return task, false, nil
	}
	atespace, name := taskKey(task)
	key := atespace + "/" + name
	actor, err := r.client.GetActor(ctx, atespace, name)
	if err != nil {
		return task, false, fmt.Errorf("getting actor %s: %w", key, err)
	}
	if !substrate.IsWorkerLossCrash(actor) {
		return task, false, nil
	}
	crash := actor.GetStatus().GetCrash()
	crashID := crash.GetCrashTime().AsTime().UTC().Format(time.RFC3339Nano) + " " + crash.GetMessage()

	r.drainMu.Lock()
	if r.reverted[key] == crashID || r.revertAttempts[key+"\x00"+crashID] >= maxRevertAttempts {
		r.drainMu.Unlock()
		return task, false, nil
	}
	r.drainMu.Unlock()

	slog.Info("actor crashed because its worker went away; reverting it to its latest snapshot", "task", key, "crash", crash.GetMessage())
	if _, err := r.client.RevertActor(ctx, atespace, name); err != nil {
		r.drainMu.Lock()
		r.revertAttempts[key+"\x00"+crashID]++
		r.drainMu.Unlock()
		return task, false, fmt.Errorf("reverting actor %s: %w", key, err)
	}
	r.drainMu.Lock()
	r.reverted[key] = crashID
	delete(r.revertAttempts, key+"\x00"+crashID)
	delete(r.drainSuspended, key)
	r.drainMu.Unlock()
	r.forgetRunning(key)

	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	if task.Status.Phase == v1alpha1.PhaseFailed {
		// AX recorded the failure itself (a resume that hit the crash). The
		// actor is suspended again, so let the phase follow it.
		task.Status.Phase = ""
	}
	setCondition(task, ConditionAutoSuspended, "True", ReasonRevertedAfterWorkerLoss,
		fmt.Sprintf("The actor crashed because its worker went away (%q); AX reverted it to its latest snapshot. The next request resumes it.", crash.GetMessage()), r.Now())
	return task, true, nil
}

// DrainSuspendFailed returns the atespace/name of every actor whose drain
// suspend failed and that the drain check is still watching.
func (r *TaskReconciler) DrainSuspendFailed() []string {
	r.drainMu.Lock()
	defer r.drainMu.Unlock()
	out := make([]string, 0, len(r.suspendFailed))
	for key := range r.suspendFailed {
		out = append(out, key)
	}
	return out
}

// RevertIfDrainSuspendFailed reverts the task's actor to its latest snapshot
// when AX tried to suspend it off a draining worker, the suspend failed, and
// the actor then ended CRASHED. That happens when the agent cannot be
// checkpointed, for example because an older runner stopped the agent on
// the worker's SIGTERM, and the worker then goes away.
//
// It acts only on actors in AX's own record of failed drain suspends, never
// on Substrate's crash message, so a task that fails as it starts is never
// reverted. It keeps watching the actor across passes, since it crashes
// only when its worker goes, and forgets it once it is suspended or running
// on another worker, or after failedSuspendTTL. A revert that keeps failing
// is given up after maxRevertAttempts.
//
// It returns the task and whether its stored status changed.
func (r *TaskReconciler) RevertIfDrainSuspendFailed(ctx context.Context, task *v1alpha1.Task) (*v1alpha1.Task, bool, error) {
	atespace, name := taskKey(task)
	key := atespace + "/" + name

	r.drainMu.Lock()
	f := r.suspendFailed[key]
	var rec failedDrainSuspend
	if f != nil {
		rec = *f
	}
	r.drainMu.Unlock()
	if f == nil {
		return task, false, nil
	}
	if r.Now().Sub(rec.since) > failedSuspendTTL {
		slog.Warn("drain check: stopped watching an actor whose drain suspend failed", "task", key, "worker", rec.worker, "since", rec.since)
		r.forgetFailedSuspend(key)
		return task, false, nil
	}
	if task.GetMetadata() == nil || task.GetStatus().GetPhase() == v1alpha1.PhaseTerminating || task.GetStatus().GetPhase() == v1alpha1.PhasePending {
		return task, false, nil
	}

	actor, err := r.client.GetActor(ctx, atespace, name)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			r.forgetFailedSuspend(key)
			return task, false, nil
		}
		return task, false, fmt.Errorf("getting actor %s: %w", key, err)
	}
	switch actor.GetStatus().GetState() {
	case ateapipb.ActorState_ACTOR_STATE_CRASHED:
		// Revert below.
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
		// A later suspend went through, or something else suspended it.
		r.forgetFailedSuspend(key)
		return task, false, nil
	case ateapipb.ActorState_ACTOR_STATE_RUNNING:
		if w := actor.GetStatus().GetWorkerAssignment().GetWorker().GetName(); w != "" && w != rec.worker {
			// Suspended after all and resumed on another worker.
			r.forgetFailedSuspend(key)
		}
		// Still on the draining worker: the next pass retries the suspend.
		return task, false, nil
	default:
		// Moving between states; look again on the next pass.
		return task, false, nil
	}

	crash := actor.GetStatus().GetCrash()
	crashID := crash.GetCrashTime().AsTime().UTC().Format(time.RFC3339Nano) + " " + crash.GetMessage()
	slog.Info("actor crashed after its drain suspend failed; reverting it to its latest snapshot", "task", key, "worker", rec.worker, "suspend_error", rec.err)
	if _, err := r.client.RevertActor(ctx, atespace, name); err != nil {
		r.drainMu.Lock()
		if f := r.suspendFailed[key]; f != nil {
			f.revertAttempts++
			if f.revertAttempts >= maxRevertAttempts {
				delete(r.suspendFailed, key)
				slog.Warn("drain check: giving up reverting an actor whose drain suspend failed", "task", key, "error", err)
			}
		}
		r.drainMu.Unlock()
		return task, false, fmt.Errorf("reverting actor %s: %w", key, err)
	}
	r.drainMu.Lock()
	delete(r.suspendFailed, key)
	delete(r.drainSuspended, key)
	r.reverted[key] = crashID
	r.drainMu.Unlock()
	r.forgetRunning(key)

	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	if task.Status.Phase == v1alpha1.PhaseFailed {
		// The actor is suspended again, so let the phase follow it.
		task.Status.Phase = ""
	}
	setCondition(task, ConditionAutoSuspended, "True", ReasonRevertedAfterFailedSuspend,
		fmt.Sprintf("AX could not suspend the actor off draining worker %s (%s), and the actor then crashed; AX reverted it to its latest snapshot. The next request resumes it.", rec.worker, summarize(rec.err)), r.Now())
	return task, true, nil
}

// forgetFailedSuspend drops the record of a failed drain suspend.
func (r *TaskReconciler) forgetFailedSuspend(key string) {
	r.drainMu.Lock()
	delete(r.suspendFailed, key)
	r.drainMu.Unlock()
}

// summarize shortens an error for a condition message.
func summarize(msg string) string {
	msg = strings.Join(strings.Fields(msg), " ")
	if len(msg) > maxErrorSummary {
		msg = msg[:maxErrorSummary] + "…"
	}
	return msg
}

// DrainHandled returns the atespace/name of every task the drain check
// suspended or reverted that it has not yet seen running again.
func (r *TaskReconciler) DrainHandled() []string {
	r.drainMu.Lock()
	defer r.drainMu.Unlock()
	seen := make(map[string]bool)
	var out []string
	for _, m := range []map[string]string{r.drainSuspended, r.reverted} {
		for key := range m {
			if !seen[key] {
				seen[key] = true
				out = append(out, key)
			}
		}
	}
	return out
}

// ForgetDrain drops what the drain check remembers about a task.
func (r *TaskReconciler) ForgetDrain(key string) {
	r.drainMu.Lock()
	defer r.drainMu.Unlock()
	delete(r.drainSuspended, key)
	delete(r.reverted, key)
	delete(r.suspendFailed, key)
	for k := range r.revertAttempts {
		if strings.HasPrefix(k, key+"\x00") {
			delete(r.revertAttempts, k)
		}
	}
}

// NoteResumedAfterDrain turns AutoSuspended False with ReasonResumedByRequest
// once a task the drain check suspended or reverted is running again, and
// forgets the task. CheckIdle does the same, but only for tasks that set
// spec.idle or spec.onCompletion; a drain suspends any task. It returns the
// task and whether its stored status changed.
func (r *TaskReconciler) NoteResumedAfterDrain(ctx context.Context, task *v1alpha1.Task) (*v1alpha1.Task, bool, error) {
	atespace, name := taskKey(task)
	key := atespace + "/" + name
	actor, err := r.client.GetActor(ctx, atespace, name)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			r.ForgetDrain(key)
			return task, false, nil
		}
		return task, false, fmt.Errorf("getting actor %s: %w", key, err)
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		return task, false, nil
	}
	r.ForgetDrain(key)
	if !conditionTrue(task, ConditionAutoSuspended) {
		return task, false, nil
	}
	setCondition(task, ConditionAutoSuspended, "False", ReasonResumedByRequest, "Agent Substrate resumed the task to deliver a request", r.Now())
	return task, true, nil
}

// WorkerLostActors returns the atespace/name of every actor that crashed
// because its worker went away. RevertIfWorkerLost re-reads each one.
func (r *TaskReconciler) WorkerLostActors(ctx context.Context) ([]string, error) {
	actors, err := r.client.ListActors(ctx, "")
	if err != nil {
		return nil, err
	}
	var out []string
	for _, a := range actors {
		if substrate.IsWorkerLossCrash(a) {
			out = append(out, a.GetMetadata().GetAtespace()+"/"+a.GetMetadata().GetName())
		}
	}
	return out, nil
}

func taskKey(task *v1alpha1.Task) (atespace, name string) {
	atespace = task.GetMetadata().GetAtespace()
	if atespace == "" {
		atespace = "default"
	}
	return atespace, task.GetMetadata().GetName()
}
