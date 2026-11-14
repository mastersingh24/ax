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
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

// fakeClock is a wall clock the test moves by hand.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type shutdownHarness struct {
	clock *fakeClock
	sigs  chan os.Signal
	tick  chan time.Time
	ctx   context.Context
}

func newShutdownHarness(t *testing.T, grace time.Duration) *shutdownHarness {
	t.Helper()
	h := &shutdownHarness{
		clock: &fakeClock{t: time.Unix(1_800_000_000, 0)},
		sigs:  make(chan os.Signal),
		tick:  make(chan time.Time),
	}
	ctx, cancel := shutdownContext(context.Background(), h.sigs, grace, h.tick, h.clock.now)
	t.Cleanup(cancel)
	h.ctx = ctx
	return h
}

// step moves the clock by d and delivers one tick, waiting until the watcher
// has taken it.
func (h *shutdownHarness) step(d time.Duration) {
	h.clock.add(d)
	select {
	case h.tick <- h.clock.now():
	case <-h.ctx.Done():
	}
}

func (h *shutdownHarness) stopped() bool {
	select {
	case <-h.ctx.Done():
		return true
	case <-time.After(50 * time.Millisecond):
		return false
	}
}

func TestShutdown_FirstSIGTERMKeepsRunningUntilGrace(t *testing.T) {
	h := newShutdownHarness(t, 10*time.Minute)
	h.sigs <- syscall.SIGTERM
	for range 9 {
		h.step(time.Minute / 4) // ticks well under the freeze gap
	}
	if h.stopped() {
		t.Fatal("stopped right after the first SIGTERM; it should keep running for the grace period")
	}
	// Walk to the deadline in small steps, as a live process would.
	for i := 0; i < 50 && !h.stopped(); i++ {
		h.step(15 * time.Second)
	}
	if !h.stopped() {
		t.Fatal("still running after the grace period")
	}
}

func TestShutdown_SecondSIGTERMStopsAtOnce(t *testing.T) {
	h := newShutdownHarness(t, 10*time.Minute)
	h.sigs <- syscall.SIGTERM
	h.sigs <- syscall.SIGTERM
	if !h.stopped() {
		t.Fatal("a second SIGTERM did not stop the task")
	}
}

func TestShutdown_SIGINTStopsAtOnce(t *testing.T) {
	h := newShutdownHarness(t, 10*time.Minute)
	h.sigs <- os.Interrupt
	if !h.stopped() {
		t.Fatal("SIGINT did not stop the task")
	}
}

func TestShutdown_ZeroGraceStopsOnFirstSIGTERM(t *testing.T) {
	h := newShutdownHarness(t, 0)
	h.sigs <- syscall.SIGTERM
	if !h.stopped() {
		t.Fatal("with grace 0 the first SIGTERM should stop the task, as before")
	}
}

func TestShutdown_ResumeAfterSuspendForgetsTheSIGTERM(t *testing.T) {
	h := newShutdownHarness(t, 10*time.Minute)
	h.sigs <- syscall.SIGTERM
	h.step(5 * time.Second)
	// Suspended: the process is frozen and restored an hour later on another
	// worker, past the old deadline. The first tick sees the wall-clock jump.
	h.step(time.Hour)
	for range 5 {
		h.step(time.Second)
	}
	if h.stopped() {
		t.Fatal("the task stopped after being resumed; the old worker's SIGTERM should have been forgotten")
	}
	// A new drain on the new worker is honoured again.
	h.sigs <- syscall.SIGTERM
	h.sigs <- syscall.SIGTERM
	if !h.stopped() {
		t.Fatal("a fresh second SIGTERM after the resume did not stop the task")
	}
}

func TestShutdown_NoSIGTERMNoStop(t *testing.T) {
	h := newShutdownHarness(t, time.Minute)
	h.step(time.Hour) // a freeze with nothing pending is harmless
	h.step(time.Second)
	if h.stopped() {
		t.Fatal("stopped without any signal")
	}
}

func TestSIGTERMGrace(t *testing.T) {
	for _, tc := range []struct {
		env     string
		want    time.Duration
		wantErr bool
	}{
		{"", DefaultSIGTERMGrace, false},
		{"0", 0, false},
		{"90s", 90 * time.Second, false},
		{"-1m", 0, true},
		{"soon", 0, true},
	} {
		t.Setenv(SIGTERMGraceEnv, tc.env)
		got, err := SIGTERMGrace()
		if (err != nil) != tc.wantErr || (!tc.wantErr && got != tc.want) {
			t.Errorf("%s=%q: got %v, %v; want %v, err=%v", SIGTERMGraceEnv, tc.env, got, err, tc.want, tc.wantErr)
		}
	}
}
