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

package server

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

const (
	// drainSuspendTimeout bounds one synchronous suspend off a draining
	// worker, which waits for the snapshot to be uploaded. Agent Substrate
	// v1.0 gives the actor 30 minutes, so this leaves room for a retry on the
	// next pass.
	drainSuspendTimeout = 10 * time.Minute
	// drainListTimeout bounds listing workers, their actors, and crashed actors.
	drainListTimeout = 30 * time.Second
	// revertTimeout bounds one revert of a crashed actor.
	revertTimeout = 2 * time.Minute
)

// RunDrainWatcher calls HandleWorkerLoss every interval until ctx is done. It
// does nothing when interval is not positive.
func (s *Server) RunDrainWatcher(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	slog.Info("suspending tasks off draining workers", "interval", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.HandleWorkerLoss(ctx)
		}
	}
}

// HandleWorkerLoss makes one pass over draining workers and crashed actors:
// it suspends, synchronously, every running task actor on a worker that is
// draining, reverts task actors that crashed because their worker went away
// or after a drain suspend AX made failed, and marks the tasks it handled
// earlier resumed once they run again. Each task is handled under its lock and re-read first, like the idle
// check, so a pass never races a concurrent suspend, resume or delete.
func (s *Server) HandleWorkerLoss(ctx context.Context) {
	lctx, cancel := context.WithTimeout(ctx, drainListTimeout)
	draining, err := s.reconciler.DrainingActors(lctx)
	cancel()
	if err != nil {
		slog.Warn("drain check: listing draining workers failed", "error", err)
	}
	for key, worker := range draining {
		atespace, name, ok := strings.Cut(key, "/")
		if !ok {
			continue
		}
		s.withTask(ctx, atespace, name, drainSuspendTimeout, func(cctx context.Context, task *v1alpha1.Task) (*v1alpha1.Task, bool, error) {
			return s.reconciler.SuspendForDrain(cctx, task, worker)
		})
	}

	// A task suspended above reads SUSPENDED here (the suspend was
	// synchronous), so only one resumed since an earlier pass is marked.
	for _, key := range s.reconciler.DrainHandled() {
		atespace, name, ok := strings.Cut(key, "/")
		if !ok {
			continue
		}
		if !s.withTask(ctx, atespace, name, drainListTimeout, s.reconciler.NoteResumedAfterDrain) {
			s.reconciler.ForgetDrain(key)
		}
	}

	// An actor AX failed to suspend off a draining worker crashes when the
	// worker goes away; revert it then. Its crash message says why the
	// checkpoint failed rather than that the worker went away, so the
	// worker-loss pass below would not touch it.
	for _, key := range s.reconciler.DrainSuspendFailed() {
		atespace, name, ok := strings.Cut(key, "/")
		if !ok {
			continue
		}
		if !s.withTask(ctx, atespace, name, revertTimeout, s.reconciler.RevertIfDrainSuspendFailed) {
			s.reconciler.ForgetDrain(key)
		}
	}

	lctx, cancel = context.WithTimeout(ctx, drainListTimeout)
	lost, err := s.reconciler.WorkerLostActors(lctx)
	cancel()
	if err != nil {
		slog.Warn("drain check: listing crashed actors failed", "error", err)
		return
	}
	for _, key := range lost {
		atespace, name, ok := strings.Cut(key, "/")
		if !ok {
			continue
		}
		s.withTask(ctx, atespace, name, revertTimeout, s.reconciler.RevertIfWorkerLost)
	}
}

// withTask runs fn on the task under its lock, read the way handlers read it,
// and records the status fn returns when it reports a change. Actors that
// belong to no task are skipped. It returns false only when there is no such
// task.
func (s *Server) withTask(ctx context.Context, atespace, name string, timeout time.Duration, fn func(context.Context, *v1alpha1.Task) (*v1alpha1.Task, bool, error)) bool {
	lockCtx, cancelLock := context.WithTimeout(ctx, idleLockTimeout)
	unlock, err := s.locker.Lock(lockCtx, "task", atespace, name)
	cancelLock()
	if err != nil {
		// Someone else is working on the task; try again next pass.
		return true
	}
	defer unlock()

	task, err := s.getTask(ctx, atespace, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false
		}
		slog.Warn("drain check: reading task failed", "task", atespace+"/"+name, "error", err)
		return true
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	updated, changed, err := fn(cctx, task)
	if err != nil {
		slog.Warn("drain check failed", "task", atespace+"/"+name, "error", err)
	}
	if !changed {
		return true
	}
	if err := s.updateTask(cctx, atespace, name, updated.GetStatus()); err != nil {
		slog.Error("drain check: recording task status failed", "task", atespace+"/"+name, "error", err)
	}
	return true
}
