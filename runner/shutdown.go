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

package runner

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"syscall"
	"time"
)

// DefaultSIGTERMGrace is how long the runner keeps the task running after a
// first SIGTERM. Agent Substrate sends the actor's main process SIGTERM when
// its worker drains and allows 30 minutes for the actor to be suspended
// before killing it; AX suspends it from the control plane within seconds.
// Stopping the command on SIGTERM would leave nothing alive to snapshot, so
// the runner waits, and stops only if no suspend has come by the end of this
// period, a little short of Substrate's.
const DefaultSIGTERMGrace = 25 * time.Minute

// SIGTERMGraceEnv overrides DefaultSIGTERMGrace with a Go duration. "0" makes
// the first SIGTERM stop the task at once, as before.
const SIGTERMGraceEnv = "AX_RUNNER_SIGTERM_GRACE"

// SIGTERMGrace returns the grace period from SIGTERMGraceEnv, or the default.
func SIGTERMGrace() (time.Duration, error) {
	v := os.Getenv(SIGTERMGraceEnv)
	if v == "" {
		return DefaultSIGTERMGrace, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("%s=%q: want a non-negative duration such as 25m or 0", SIGTERMGraceEnv, v)
	}
	return d, nil
}

// shutdownTick is how often the shutdown watcher checks the clock.
const shutdownTick = time.Second

// freezeGap is how far the wall clock may jump between two ticks before the
// watcher concludes the sandbox was frozen and restored, i.e. suspended.
const freezeGap = 30 * time.Second

// ShutdownContext returns a context that is cancelled when the task should
// stop:
//
//   - at once on SIGINT, or on a second SIGTERM;
//   - grace after a first SIGTERM, if the task is still running then.
//
// A first SIGTERM comes from a worker that is draining. AX suspends the actor
// before grace is up, which freezes this process; when it is restored on
// another worker, the wall clock has jumped while the process was frozen. The
// watcher takes such a jump as a resume and forgets the SIGTERM, which came
// from the old worker. With grace zero, a first SIGTERM stops the task at once.
func ShutdownContext(parent context.Context, sigs <-chan os.Signal, grace time.Duration) (context.Context, context.CancelFunc) {
	return shutdownContext(parent, sigs, grace, time.NewTicker(shutdownTick).C, time.Now)
}

func shutdownContext(parent context.Context, sigs <-chan os.Signal, grace time.Duration, tick <-chan time.Time, now func() time.Time) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		var deadline time.Time // zero: no SIGTERM pending
		last := now().Round(0)
		for {
			select {
			case <-ctx.Done():
				return
			case sig := <-sigs:
				if sig != syscall.SIGTERM || grace == 0 || !deadline.IsZero() {
					slog.Info("stopping the task", "signal", sig.String())
					cancel()
					return
				}
				deadline = now().Round(0).Add(grace)
				slog.Info("SIGTERM: the worker is draining; keeping the task running so it can be suspended",
					"grace", grace.String(), "stopAt", deadline.Format(time.RFC3339))
			case <-tick:
				t := now().Round(0) // wall clock: a frozen process doesn't see it pass
				if gap := t.Sub(last); gap > freezeGap && !deadline.IsZero() {
					slog.Info("resumed after being suspended; forgetting the SIGTERM from the old worker", "frozenFor", gap.Round(time.Second).String())
					deadline = time.Time{}
				}
				last = t
				if !deadline.IsZero() && !t.Before(deadline) {
					slog.Info("no suspend came within the SIGTERM grace period; stopping the task", "grace", grace.String())
					cancel()
					return
				}
			}
		}
	}()
	return ctx, cancel
}
