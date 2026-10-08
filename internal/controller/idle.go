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
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/internal/metadata"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// Reasons recorded on the Ready condition by automatic suspension.
const (
	// ReasonIdleSuspended: no requests for spec.idle.suspendAfter.
	ReasonIdleSuspended = "IdleSuspended"
	// ReasonCompletedSuspended: the command exited and spec.onCompletion is Suspend.
	ReasonCompletedSuspended = "CompletedSuspended"
	// ReasonResumedByRequest: Agent Substrate's router resumed a suspended task
	// to deliver a request.
	ReasonResumedByRequest = "ResumedByRequest"
	// ReasonActorSuspended: the actor was suspended by something other than AX.
	ReasonActorSuspended = "ActorSuspended"

	// runnerStatusTimeout bounds a status read, which includes the runner's
	// own call to spec.idle.busyPath.
	runnerStatusTimeout = 5 * time.Second
	runnerBodyLimit     = 64 << 10
)

// idleDecision is what CheckIdle concluded about a running task.
type idleDecision struct {
	suspend bool
	reason  string
	message string
}

// decideIdle applies the task's spec.idle and spec.onCompletion to what its
// runner reported. runningFor is how long the control plane has seen the
// actor running without a break; idleness never exceeds it, so a task that
// was just resumed is not suspended on the strength of a runner clock that
// comes from an old snapshot.
func decideIdle(spec *v1alpha1.TaskSpec, st metadata.RunnerStatus, runningFor time.Duration) idleDecision {
	if st.Exited && spec.SuspendOnCompletion() {
		return idleDecision{
			suspend: true,
			reason:  ReasonCompletedSuspended,
			message: fmt.Sprintf("Task command exited with code %d; suspended because spec.onCompletion is Suspend", st.ExitCode),
		}
	}
	after := spec.IdleSuspendAfter()
	if after <= 0 || st.InFlight > 0 || st.Busy {
		return idleDecision{}
	}
	idle := time.Duration(st.IdleSeconds) * time.Second
	if runningFor < idle {
		idle = runningFor
	}
	if idle < after {
		return idleDecision{}
	}
	return idleDecision{
		suspend: true,
		reason:  ReasonIdleSuspended,
		message: fmt.Sprintf("No requests for %s; suspended because spec.idle.suspendAfter is %s. The next request through the router resumes it.", idle.Truncate(time.Second), after),
	}
}

// CheckIdle suspends the task if its spec.idle or spec.onCompletion policy
// says so, and brings the recorded phase in line with the actor's real state.
// It returns the task and whether its status changed.
//
// The actor's state comes from the Substrate control API, which never wakes
// an actor. The runner is only asked for its status when the actor is
// running, so a suspended task stays suspended. A task recorded as Suspended
// whose actor is running was resumed by Agent Substrate's router for an
// incoming request, and is recorded as Running again.
func (r *TaskReconciler) CheckIdle(ctx context.Context, task *v1alpha1.Task) (*v1alpha1.Task, bool, error) {
	spec := task.GetSpec()
	if !spec.AutoSuspends() || task.GetMetadata() == nil {
		return task, false, nil
	}
	phase := task.GetStatus().GetPhase()
	if phase != "Running" && phase != "Suspended" {
		return task, false, nil
	}
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	atespace := task.Metadata.Atespace
	if atespace == "" {
		atespace = "default"
	}
	name := task.Metadata.Name
	key := atespace + "/" + name

	actor, err := r.client.GetActor(ctx, atespace, name)
	if err != nil {
		return task, false, fmt.Errorf("getting actor %s: %w", key, err)
	}
	now := r.Now()

	switch actor.GetStatus().GetState() {
	case ateapipb.ActorState_ACTOR_STATE_RUNNING:
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
		r.forgetRunning(key)
		if phase != "Running" {
			return task, false, nil
		}
		task.Status.Phase = "Suspended"
		task.Status.WorkerIp = ""
		r.setCondition(task, condReady, "False", ReasonActorSuspended, "The task's actor was suspended outside AX", now)
		return task, true, nil
	default:
		// Resuming, crashed, deleting and so on: leave it to the next pass or
		// to an explicit reconcile.
		return task, false, nil
	}

	since := r.markRunning(key, now)
	workerIP := actor.GetStatus().GetWorkerAssignment().GetWorkerPodIp()
	changed := false
	if phase == "Suspended" {
		task.Status.Phase = "Running"
		task.Status.WorkerIp = workerIP
		r.setCondition(task, condReady, "True", ReasonResumedByRequest, "Agent Substrate resumed the task to deliver a request", now)
		changed = true
		slog.Info("task was resumed by a request", "task", key)
	}

	st, err := r.runnerStatus(ctx, atespace, name, workerIP)
	if err != nil {
		// Runners built before the status endpoint, or one that is still
		// starting: nothing to decide on.
		slog.Debug("could not read runner status", "task", key, "error", err)
		return task, changed, nil
	}

	d := decideIdle(spec, st, now.Sub(since))
	if !d.suspend {
		return task, changed, nil
	}
	slog.Info("suspending task automatically", "task", key, "reason", d.reason, "idleSeconds", st.IdleSeconds, "exited", st.Exited)
	if err := r.client.SuspendActor(ctx, atespace, name); err != nil {
		return task, changed, fmt.Errorf("suspending actor %s: %w", key, err)
	}
	r.forgetRunning(key)
	task.Status.Phase = "Suspended"
	task.Status.WorkerIp = ""
	r.setCondition(task, condReady, "False", d.reason, d.message, now)
	return task, true, nil
}

// runnerStatus reads the runner's status endpoint, directly from the worker
// when it can be reached and otherwise through Agent Substrate's router. The
// caller has just seen the actor running, so the router does not have to wake
// it to answer.
func (r *TaskReconciler) runnerStatus(ctx context.Context, atespace, name, workerIP string) (metadata.RunnerStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, runnerStatusTimeout)
	defer cancel()

	var errs []error
	if workerIP != "" {
		host, port := workerIP, "80"
		if h, p, err := net.SplitHostPort(workerIP); err == nil {
			host, port = h, p
		}
		st, err := r.getRunnerStatus(ctx, fmt.Sprintf("http://%s%s", net.JoinHostPort(host, port), metadata.StatusPath), "")
		if err == nil {
			return st, nil
		}
		errs = append(errs, err)
	}
	if r.RouterAddr != "" {
		st, err := r.getRunnerStatus(ctx, fmt.Sprintf("http://%s%s", r.RouterAddr, metadata.StatusPath), atespace+"/"+name)
		if err == nil {
			return st, nil
		}
		errs = append(errs, err)
	}
	if len(errs) == 0 {
		return metadata.RunnerStatus{}, fmt.Errorf("actor has no worker address and no router is configured")
	}
	return metadata.RunnerStatus{}, fmt.Errorf("reading runner status: %v", errs)
}

func (r *TaskReconciler) getRunnerStatus(ctx context.Context, url, targetActor string) (metadata.RunnerStatus, error) {
	var st metadata.RunnerStatus
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return st, err
	}
	if targetActor != "" {
		req.Header.Set("ate-target-actor", targetActor)
	}
	resp, err := r.statusClient.Do(req)
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return st, fmt.Errorf("%s returned %s", url, resp.Status)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, runnerBodyLimit)).Decode(&st); err != nil {
		return st, fmt.Errorf("decoding runner status from %s: %w", url, err)
	}
	return st, nil
}

// markRunning records that the actor is running and returns since when the
// control plane has seen it running without a break.
func (r *TaskReconciler) markRunning(key string, now time.Time) time.Time {
	r.runningMu.Lock()
	defer r.runningMu.Unlock()
	if since, ok := r.runningSince[key]; ok {
		return since
	}
	r.runningSince[key] = now
	return now
}

// resetRunning restarts the running clock, for an explicit resume.
func (r *TaskReconciler) resetRunning(key string, now time.Time) {
	r.runningMu.Lock()
	defer r.runningMu.Unlock()
	r.runningSince[key] = now
}

func (r *TaskReconciler) forgetRunning(key string) {
	r.runningMu.Lock()
	defer r.runningMu.Unlock()
	delete(r.runningSince, key)
}
