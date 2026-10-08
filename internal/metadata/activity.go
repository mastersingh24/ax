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

package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// StatusPath is where the runner reports activity and command state. The
// control plane reads it to decide whether to suspend an idle or finished
// task.
const StatusPath = "/metadata/v1alpha1/ax/status"

// RunnerStatus is the body served at StatusPath.
type RunnerStatus struct {
	// IdleSeconds is how long ago the last request through the pass-through or
	// to the guest services finished. It is zero while one is open.
	IdleSeconds int64 `json:"idleSeconds"`
	// InFlight is the number of requests open right now. A streaming response
	// (server-sent events, a WebSocket) counts for as long as it stays open.
	InFlight int `json:"inFlight"`
	// Busy is the task's own answer at spec.idle.busyPath, false when unset.
	Busy bool `json:"busy"`
	// BusyError explains why the busy check didn't return busy, when it failed.
	BusyError string `json:"busyError,omitempty"`
	// Exited reports whether spec.command has exited.
	Exited bool `json:"exited"`
	// ExitCode is the command's exit status, -1 if killed by a signal. Only
	// meaningful when Exited is true.
	ExitCode int `json:"exitCode"`
}

const (
	// busyCheckTimeout bounds the call to spec.idle.busyPath.
	busyCheckTimeout = 2 * time.Second
	// busyBodyLimit caps how much of the busy answer is read.
	busyBodyLimit = 4096
	// resumeGap is how far apart two clock ticks must be for the runner to
	// conclude it was frozen (suspended) and restored in between.
	resumeGap  = 5 * time.Second
	resumeTick = time.Second
)

// activity tracks the requests that mean someone is using the task. Only
// traffic the runner forwards to the task and guest service calls (ax ssh)
// count; the runner's own endpoints are polled by the control plane and
// Agent Substrate and must not keep a task awake.
type activity struct {
	mu       sync.Mutex
	now      func() time.Time
	last     time.Time
	inFlight int
	exited   bool
	exitCode int
}

func newActivity(now func() time.Time) *activity {
	if now == nil {
		now = time.Now
	}
	return &activity{now: now, last: now()}
}

func (a *activity) begin() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.inFlight++
	a.last = a.now()
}

func (a *activity) end() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.inFlight--
	a.last = a.now()
}

// reset restarts the idle clock without a request, for example after the
// task was restored from a snapshot taken long ago.
func (a *activity) reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.last = a.now()
}

func (a *activity) setExit(code int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.exited = true
	a.exitCode = code
}

// snapshot returns the traffic and command part of the status.
func (a *activity) snapshot() RunnerStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	st := RunnerStatus{InFlight: a.inFlight, Exited: a.exited, ExitCode: a.exitCode}
	if a.inFlight == 0 {
		if idle := a.now().Sub(a.last); idle > 0 {
			st.IdleSeconds = int64(idle / time.Second)
		}
	}
	return st
}

// track wraps h so every request it serves counts as activity.
func (a *activity) track(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.begin()
		defer a.end()
		h.ServeHTTP(w, r)
	})
}

// watchResume resets the idle clock when the process has been frozen. A task
// resumes from a snapshot whose memory still holds the time of the last
// request before it, possibly hours ago; without this it would look idle the
// moment it came back. A suspend shows up as consecutive ticks much further
// apart than the tick interval.
func (a *activity) watchResume(ctx context.Context, tick, gap time.Duration) {
	t := time.NewTicker(tick)
	defer t.Stop()
	a.resetOnGaps(ctx, t.C, time.Now(), gap)
}

// resetOnGaps resets the idle clock whenever two consecutive ticks are more
// than gap apart in wall-clock time.
func (a *activity) resetOnGaps(ctx context.Context, ticks <-chan time.Time, prev time.Time, gap time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticks:
			// Round(0) drops the monotonic reading, which may not advance while
			// the sandbox is frozen; wall-clock time does.
			if now.Round(0).Sub(prev.Round(0)) > gap {
				slog.Info("runner was suspended and resumed; restarting the idle clock")
				a.reset()
			}
			prev = now
		}
	}
}

// busyAnswer is the body expected from spec.idle.busyPath.
type busyAnswer struct {
	Busy bool `json:"busy"`
}

// checkBusy asks the task whether it is in the middle of work. Only a 200
// with {"busy": true} means busy; anything else is reported as an error and
// treated as not busy, so a task whose server is gone can still be suspended.
func checkBusy(ctx context.Context, client *http.Client, port int, path string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, busyCheckTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d%s", port, path), nil)
	if err != nil {
		return false, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("busy check returned %s", resp.Status)
	}
	var ans busyAnswer
	if err := json.NewDecoder(io.LimitReader(resp.Body, busyBodyLimit)).Decode(&ans); err != nil {
		return false, fmt.Errorf("busy check returned an unreadable body: %w", err)
	}
	return ans.Busy, nil
}
