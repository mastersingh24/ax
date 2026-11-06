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
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/ax/internal/metadata"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

func freeLocalPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestMetadataServer_PassThrough(t *testing.T) {
	// The task's own server: an API endpoint and a slow event stream.
	release := make(chan struct{})
	backendMux := http.NewServeMux()
	backendMux.HandleFunc("/sessions", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "task server saw %s %s", r.Method, r.URL.Path)
	})
	backendMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "task server healthz")
	})
	backendMux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-release
		fmt.Fprint(w, "data: second\n\n")
	})
	backendLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	backend := &http.Server{Handler: backendMux}
	go backend.Serve(backendLis)
	defer backend.Close()
	backendPort := backendLis.Addr().(*net.TCPAddr).Port

	runnerPort := freeLocalPort(t)
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "t", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{Http: &v1alpha1.TaskHTTP{Port: int32(backendPort)}},
	}
	srv := metadata.NewServer(runnerPort, task, nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop(context.Background())
	base := fmt.Sprintf("http://127.0.0.1:%d", runnerPort)

	get := func(path string) string {
		t.Helper()
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}

	if got := get("/sessions"); got != "task server saw GET /sessions" {
		t.Errorf("/sessions was not forwarded: %q", got)
	}
	// The runner keeps its own endpoints.
	if got := get("/healthz"); strings.Contains(got, "task server") {
		t.Errorf("/healthz should be served by the runner, got %q", got)
	}

	// Streams are flushed through as they arrive.
	resp, err := http.Get(base + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	lines := make(chan string, 4)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			if sc.Text() != "" {
				lines <- sc.Text()
			}
		}
	}()
	select {
	case l := <-lines:
		if l != "data: first" {
			t.Errorf("first event = %q", l)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first event was buffered instead of streamed")
	}
	close(release)
	select {
	case l := <-lines:
		if l != "data: second" {
			t.Errorf("second event = %q", l)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second event never arrived")
	}
}

func TestMetadataServer_NoPassThroughByDefault(t *testing.T) {
	runnerPort := freeLocalPort(t)
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "t", Atespace: "default"}, Spec: &v1alpha1.TaskSpec{}}
	srv := metadata.NewServer(runnerPort, task, nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop(context.Background())
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/sessions", runnerPort))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("without spec.http, /sessions = %d, want 404", resp.StatusCode)
	}
}

func TestMetadataServer_ReadyWaitsForTaskServer(t *testing.T) {
	taskPort := freeLocalPort(t) // nothing listening yet
	runnerPort := freeLocalPort(t)
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "t", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{Http: &v1alpha1.TaskHTTP{Port: int32(taskPort)}},
	}
	srv := metadata.NewServer(runnerPort, task, nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop(context.Background())
	srv.SetWorkspaceReady(true)

	status := func(path string) int {
		t.Helper()
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d%s", runnerPort, path))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if got := status("/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("/readyz before the task server listens = %d, want 503", got)
	}
	if got := status("/readyz?check=workspace"); got != http.StatusOK {
		t.Errorf("/readyz?check=workspace = %d, want 200 (workspace only)", got)
	}

	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", taskPort))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if got := status("/readyz"); got != http.StatusOK {
		t.Errorf("/readyz once the task server listens = %d, want 200", got)
	}
}

// startPassThrough serves backend on a loopback port and returns the base URL
// of a runner whose spec.http.port forwards to it, plus the backend's port.
func startPassThrough(t *testing.T, backend http.Handler) (string, int) {
	t.Helper()
	backendLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	bs := &http.Server{Handler: backend}
	go bs.Serve(backendLis)
	t.Cleanup(func() { bs.Close() })
	backendPort := backendLis.Addr().(*net.TCPAddr).Port

	runnerPort := freeLocalPort(t)
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "t", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{Http: &v1alpha1.TaskHTTP{Port: int32(backendPort)}},
	}
	srv := metadata.NewServer(runnerPort, task, nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Stop(context.Background()) })
	return fmt.Sprintf("http://127.0.0.1:%d", runnerPort), backendPort
}

// The task's server sees the loopback Host it was dialed on, not the router's
// name: a loopback server that guards against DNS rebinding refuses a foreign
// Host. The router's name moves to X-Forwarded-Host.
func TestMetadataServer_PassThroughSendsLoopbackHost(t *testing.T) {
	type seen struct{ host, fwdHost string }
	got := make(chan seen, 2)
	base, backendPort := startPassThrough(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- seen{r.Host, r.Header.Get("X-Forwarded-Host")}
	}))
	wantHost := fmt.Sprintf("127.0.0.1:%d", backendPort)
	const router = "atenet-router.ate-system.svc.cluster.local"

	for _, tc := range []struct {
		name, prior, wantFwd string
	}{
		{"no prior header", "", router},
		{"appends to a prior header", "edge.example.com", "edge.example.com, " + router},
	} {
		req, _ := http.NewRequest(http.MethodGet, base+"/sessions", nil)
		req.Host = router
		if tc.prior != "" {
			req.Header.Set("X-Forwarded-Host", tc.prior)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		resp.Body.Close()
		s := <-got
		if s.host != wantHost {
			t.Errorf("%s: backend saw Host %q, want %q", tc.name, s.host, wantHost)
		}
		if s.fwdHost != tc.wantFwd {
			t.Errorf("%s: backend saw X-Forwarded-Host %q, want %q", tc.name, s.fwdHost, tc.wantFwd)
		}
	}
}

// A connection upgrade still passes through, with the loopback Host.
func TestMetadataServer_PassThroughForwardsUpgrades(t *testing.T) {
	hostSeen := make(chan string, 1)
	base, backendPort := startPassThrough(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hostSeen <- r.Host
		if r.Header.Get("Upgrade") != "echo" {
			http.Error(w, "expected an upgrade", http.StatusBadRequest)
			return
		}
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		fmt.Fprint(rw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n")
		rw.Flush()
		line, _ := rw.ReadString('\n')
		fmt.Fprint(rw, "echo: "+line)
		rw.Flush()
	}))

	conn, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	fmt.Fprint(conn, "GET /ws HTTP/1.1\r\nHost: atenet-router.ate-system.svc\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n")
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	if h := <-hostSeen; h != fmt.Sprintf("127.0.0.1:%d", backendPort) {
		t.Errorf("upgrade reached the backend with Host %q", h)
	}
	fmt.Fprint(conn, "hello\n")
	line, err := br.ReadString('\n')
	if err != nil || line != "echo: hello\n" {
		t.Fatalf("after upgrade got %q, %v", line, err)
	}
}
