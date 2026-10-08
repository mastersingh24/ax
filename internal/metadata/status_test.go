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

package metadata_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/ax/internal/metadata"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// startBackend serves mux on a free local port and returns the port.
func startBackend(t *testing.T, mux http.Handler) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: mux}
	go srv.Serve(lis)
	t.Cleanup(func() { srv.Close() })
	return lis.Addr().(*net.TCPAddr).Port
}

func getStatus(t *testing.T, base string) metadata.RunnerStatus {
	t.Helper()
	resp, err := http.Get(base + metadata.StatusPath)
	if err != nil {
		t.Fatalf("GET status: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %s", resp.Status)
	}
	var st metadata.RunnerStatus
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decoding status: %v", err)
	}
	return st
}

func drain(t *testing.T, url string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// waitNoneInFlight waits for the runner to finish the requests it forwarded;
// a client can see a whole response before the handler has returned.
func waitNoneInFlight(t *testing.T, base string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st := getStatus(t, base)
		if st.InFlight == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("requests still counted in flight: %+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestMetadataServer_StatusTracksForwardedTrafficOnly(t *testing.T) {
	release := make(chan struct{})
	streaming := make(chan struct{})
	backend := http.NewServeMux()
	backend.HandleFunc("/sessions", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	})
	backend.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: hello\n\n")
		w.(http.Flusher).Flush()
		close(streaming)
		<-release
	})
	backendPort := startBackend(t, backend)

	clock := &testClock{t: time.Unix(1000, 0)}
	runnerPort := freeLocalPort(t)
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "t", Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Http: &v1alpha1.TaskHTTP{Port: int32(backendPort)},
			Idle: &v1alpha1.TaskIdle{SuspendAfter: "1m"},
		},
	}
	srv := metadata.NewServer(runnerPort, task, nil, metadata.ServerOptions{Now: clock.Now})
	srv.SetWorkspaceReady(true)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop(context.Background())
	base := fmt.Sprintf("http://127.0.0.1:%d", runnerPort)

	clock.Advance(40 * time.Second)
	if st := getStatus(t, base); st.IdleSeconds != 40 || st.InFlight != 0 || st.Busy || st.Exited {
		t.Fatalf("initial status = %+v", st)
	}

	// The control plane and Substrate poll these; they must not count.
	for _, p := range []string{"/healthz", "/readyz", "/readyz?check=workspace", "/metadata/v1alpha1/ax/task", metadata.StatusPath} {
		drain(t, base+p)
	}
	clock.Advance(5 * time.Second)
	if st := getStatus(t, base); st.IdleSeconds != 45 {
		t.Fatalf("runner endpoints counted as activity: %+v", st)
	}

	// A forwarded request restarts the idle clock.
	drain(t, base+"/sessions")
	waitNoneInFlight(t, base)
	clock.Advance(2 * time.Second)
	if st := getStatus(t, base); st.IdleSeconds != 2 || st.InFlight != 0 {
		t.Fatalf("after a forwarded request: %+v", st)
	}

	// An open stream keeps the task active for as long as it is open.
	resp, err := http.Get(base + "/events")
	if err != nil {
		t.Fatal(err)
	}
	<-streaming
	clock.Advance(time.Hour)
	if st := getStatus(t, base); st.IdleSeconds != 0 || st.InFlight != 1 {
		t.Fatalf("with a stream open: %+v", st)
	}
	close(release)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	waitNoneInFlight(t, base)
	clock.Advance(3 * time.Second)
	if st := getStatus(t, base); st.IdleSeconds != 3 {
		t.Fatalf("3s after the stream closed: %+v", st)
	}

	srv.SetCommandExit(2)
	if st := getStatus(t, base); !st.Exited || st.ExitCode != 2 {
		t.Fatalf("after command exit: %+v", st)
	}
}

func TestMetadataServer_StatusBusyCheck(t *testing.T) {
	var answer atomic.Value // the busy endpoint's response, as "status body"
	answer.Store([2]string{"200", `{"busy": true}`})
	var busyCalls atomic.Int32
	backend := http.NewServeMux()
	backend.HandleFunc("/agent/busy", func(w http.ResponseWriter, r *http.Request) {
		busyCalls.Add(1)
		a := answer.Load().([2]string)
		var code int
		fmt.Sscan(a[0], &code)
		w.WriteHeader(code)
		fmt.Fprint(w, a[1])
	})
	backendPort := startBackend(t, backend)

	clock := &testClock{t: time.Unix(1000, 0)}
	runnerPort := freeLocalPort(t)
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "t", Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Http: &v1alpha1.TaskHTTP{Port: int32(backendPort)},
			Idle: &v1alpha1.TaskIdle{SuspendAfter: "1m", BusyPath: "/agent/busy"},
		},
	}
	srv := metadata.NewServer(runnerPort, task, nil, metadata.ServerOptions{Now: clock.Now})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop(context.Background())
	base := fmt.Sprintf("http://127.0.0.1:%d", runnerPort)

	clock.Advance(time.Minute)
	st := getStatus(t, base)
	if !st.Busy || st.BusyError != "" {
		t.Fatalf("busy task reported %+v", st)
	}
	// The runner's own busy check is not traffic.
	if st.IdleSeconds != 60 {
		t.Errorf("busy check counted as activity: %+v", st)
	}

	answer.Store([2]string{"200", `{"busy": false}`})
	if st := getStatus(t, base); st.Busy || st.BusyError != "" {
		t.Errorf("idle task reported %+v", st)
	}

	// Anything but a 200 with {"busy": true} means not busy, with the reason.
	for _, bad := range [][2]string{{"500", "oops"}, {"200", "not json"}, {"404", ""}} {
		answer.Store(bad)
		if st := getStatus(t, base); st.Busy || st.BusyError == "" {
			t.Errorf("answer %v reported %+v, want not busy with an error", bad, st)
		}
	}
	if busyCalls.Load() == 0 {
		t.Fatal("busy path was never called")
	}
}

func TestMetadataServer_StatusWithoutPassThrough(t *testing.T) {
	runnerPort := freeLocalPort(t)
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "t", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{OnCompletion: v1alpha1.OnCompletionSuspend},
	}
	srv := metadata.NewServer(runnerPort, task, nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop(context.Background())
	base := fmt.Sprintf("http://127.0.0.1:%d", runnerPort)

	if st := getStatus(t, base); st.Exited || st.Busy || st.InFlight != 0 {
		t.Fatalf("initial status = %+v", st)
	}
	srv.SetCommandExit(0)
	if st := getStatus(t, base); !st.Exited || st.ExitCode != 0 {
		t.Fatalf("after exit: %+v", st)
	}
}
