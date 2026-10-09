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
	"sync"
	"testing"
	"time"
)

// fakeClock is a settable clock safe for concurrent use.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestActivity_IdleAndInFlight(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1000, 0)}
	a := newActivity(clock.Now)

	clock.Advance(30 * time.Second)
	if st := a.snapshot(); st.IdleSeconds != 30 || st.InFlight != 0 {
		t.Fatalf("after 30s without traffic: %+v", st)
	}

	// An open request keeps the task active no matter how long it lasts.
	a.begin()
	clock.Advance(10 * time.Minute)
	if st := a.snapshot(); st.IdleSeconds != 0 || st.InFlight != 1 {
		t.Fatalf("with a request open: %+v", st)
	}

	// Idle time counts from when the last request finished.
	a.end()
	clock.Advance(5 * time.Second)
	if st := a.snapshot(); st.IdleSeconds != 5 || st.InFlight != 0 {
		t.Fatalf("5s after the request closed: %+v", st)
	}

	a.setExit(3)
	if st := a.snapshot(); !st.Exited || st.ExitCode != 3 {
		t.Fatalf("after exit: %+v", st)
	}
}

func TestActivity_ResetsIdleClockAfterFreeze(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1000, 0)}
	a := newActivity(clock.Now)
	a.wall = nil // only the ticks below show the passage of wall-clock time

	ticks := make(chan time.Time)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Unix(1000, 0)
	done := make(chan struct{})
	go func() {
		a.resetOnGaps(ctx, ticks, start, 5*time.Second)
		close(done)
	}()

	// Regular ticks leave the idle clock alone.
	clock.Advance(3 * time.Hour)
	ticks <- start.Add(time.Second)
	ticks <- start.Add(2 * time.Second)
	if st := a.snapshot(); st.IdleSeconds != int64((3 * time.Hour).Seconds()) {
		t.Fatalf("regular ticks changed the idle clock: %+v", st)
	}

	// A jump between ticks means the sandbox was frozen and restored.
	ticks <- start.Add(3 * time.Hour)
	ticks <- start.Add(3*time.Hour + time.Second) // wait for the reset to land
	if st := a.snapshot(); st.IdleSeconds != 0 {
		t.Fatalf("idle clock not restarted after a freeze: %+v", st)
	}

	cancel()
	<-done
}

// With full snapshots the runner comes back as it was, including a command
// that had already exited. The status says so, which the control plane uses
// to avoid suspending a just-woken finished task on the spot.
func TestActivity_ResumedAfterExit(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1000, 0)}
	a := newActivity(clock.Now)
	a.wall = nil

	ticks := make(chan time.Time)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Unix(1000, 0)
	done := make(chan struct{})
	go func() {
		a.resetOnGaps(ctx, ticks, start, 5*time.Second)
		close(done)
	}()

	// A freeze before the command exits is an ordinary resume.
	ticks <- start.Add(time.Hour)
	ticks <- start.Add(time.Hour + time.Second)
	a.setExit(0)
	if st := a.snapshot(); !st.Exited || st.ResumedAfterExit {
		t.Fatalf("exit after the only resume: %+v", st)
	}

	ticks <- start.Add(2 * time.Hour)
	ticks <- start.Add(2*time.Hour + time.Second)
	if st := a.snapshot(); !st.Exited || !st.ResumedAfterExit {
		t.Fatalf("resume after the exit not reported: %+v", st)
	}

	cancel()
	<-done
}

// A status read that lands right after a restore, before the watch ticks,
// must not report the idle time held in the snapshot's memory.
func TestActivity_StatusDetectsFreezeBeforeTick(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1000, 0)}
	a := newActivity(clock.Now)
	wall := &fakeClock{t: time.Unix(1000, 0)}
	a.wall = wall.Now
	a.mu.Lock()
	a.gap = 5 * time.Second
	a.lastTick = wall.Now()
	a.mu.Unlock()

	a.setExit(0)
	clock.Advance(3 * time.Hour)
	wall.Advance(3 * time.Hour)
	st := a.snapshot()
	if st.IdleSeconds != 0 || !st.ResumedAfterExit {
		t.Fatalf("status right after a restore: %+v", st)
	}

	// Without a gap, the clock runs on.
	clock.Advance(4 * time.Second)
	wall.Advance(4 * time.Second)
	if st := a.snapshot(); st.IdleSeconds != 4 {
		t.Fatalf("status 4s later: %+v", st)
	}
}
