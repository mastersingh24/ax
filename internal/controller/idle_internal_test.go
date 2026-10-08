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
	"testing"
	"time"

	"github.com/google/ax/internal/metadata"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

func TestDecideIdle(t *testing.T) {
	idle10m := &v1alpha1.TaskSpec{Http: &v1alpha1.TaskHTTP{Port: 8484}, Idle: &v1alpha1.TaskIdle{SuspendAfter: "10m"}}
	onExit := &v1alpha1.TaskSpec{OnCompletion: v1alpha1.OnCompletionSuspend}
	keep := &v1alpha1.TaskSpec{OnCompletion: v1alpha1.OnCompletionKeep}
	both := &v1alpha1.TaskSpec{Http: &v1alpha1.TaskHTTP{Port: 8484}, Idle: &v1alpha1.TaskIdle{SuspendAfter: "10m"}, OnCompletion: v1alpha1.OnCompletionSuspend}
	const long = 24 * time.Hour

	tests := []struct {
		name       string
		spec       *v1alpha1.TaskSpec
		status     metadata.RunnerStatus
		runningFor time.Duration
		want       string // reason, or "" for no suspend
	}{
		{"idle long enough", idle10m, metadata.RunnerStatus{IdleSeconds: 600}, long, ReasonIdleSuspended},
		{"not idle long enough", idle10m, metadata.RunnerStatus{IdleSeconds: 599}, long, ""},
		{"request in flight", idle10m, metadata.RunnerStatus{IdleSeconds: 3600, InFlight: 1}, long, ""},
		{"busy", idle10m, metadata.RunnerStatus{IdleSeconds: 3600, Busy: true}, long, ""},
		{"busy check failed counts as not busy", idle10m, metadata.RunnerStatus{IdleSeconds: 3600, BusyError: "connection refused"}, long, ReasonIdleSuspended},
		{"just resumed with a stale runner clock", idle10m, metadata.RunnerStatus{IdleSeconds: 3600}, time.Minute, ""},
		{"resumed long enough ago", idle10m, metadata.RunnerStatus{IdleSeconds: 3600}, 10 * time.Minute, ReasonIdleSuspended},
		{"exited with onCompletion Suspend", onExit, metadata.RunnerStatus{Exited: true, ExitCode: 1}, 0, ReasonCompletedSuspended},
		{"exit wins over an open request", both, metadata.RunnerStatus{Exited: true, InFlight: 1, Busy: true}, 0, ReasonCompletedSuspended},
		{"still running with onCompletion Suspend", onExit, metadata.RunnerStatus{IdleSeconds: 3600}, long, ""},
		{"exited with onCompletion Keep", keep, metadata.RunnerStatus{Exited: true, IdleSeconds: 3600}, long, ""},
		{"exited, idle policy only, still idles out", idle10m, metadata.RunnerStatus{Exited: true, IdleSeconds: 600}, long, ReasonIdleSuspended},
		{"no policy", &v1alpha1.TaskSpec{}, metadata.RunnerStatus{Exited: true, IdleSeconds: 3600}, long, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := decideIdle(tc.spec, tc.status, tc.runningFor)
			if d.suspend != (tc.want != "") || d.reason != tc.want {
				t.Errorf("decideIdle = %+v, want reason %q", d, tc.want)
			}
			if d.suspend && d.message == "" {
				t.Error("a suspend decision needs a message for the Ready condition")
			}
		})
	}
}
