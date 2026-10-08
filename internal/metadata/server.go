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
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/env/guest"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

// Server is the cloud-style metadata server running inside the task actor container,
// multiplexing HTTP metadata endpoints and guest gRPC daemon services on a single port.
type Server struct {
	port int
	// passThroughPort is the task server's port when spec.http.port is set.
	passThroughPort int
	// busyPath is spec.idle.busyPath, asked on passThroughPort.
	busyPath       string
	activity       *activity
	busyClient     *http.Client
	stopWatch      context.CancelFunc
	server         *http.Server
	grpcServer     *grpc.Server
	grpcCleanup    func()
	mu             sync.RWMutex
	task           *v1alpha1.Task
	workspaces     []*v1alpha1.Workspace
	workspaceReady bool
}

// ServerOptions configures optional settings for the metadata and guest server.
type ServerOptions struct {
	WorkspacePath string
	LogDir        string
	// Now overrides the clock used for idle tracking; tests set it.
	Now func() time.Time
}

// NewServer creates a new metadata and guest server serving the task and its
// bound workspaces, in the task's declaration order. Nil workspaces are dropped.
func NewServer(port int, task *v1alpha1.Task, workspaces []*v1alpha1.Workspace, opts ...ServerOptions) *Server {
	if port <= 0 {
		port = 9999
	}
	var opt ServerOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	s := &Server{
		port:       port,
		task:       task,
		workspaces: compactWorkspaces(workspaces),
		activity:   newActivity(opt.Now),
		busyClient: &http.Client{},
	}

	// Guest services expose process execution and file access inside the container,
	// so they are only served when the task opts in via spec.debug.
	if task.GetSpec().GetDebug() {
		guestCfg := guest.DefaultConfig()
		if opt.WorkspacePath != "" {
			guestCfg.Workspace = opt.WorkspacePath
		}
		if opt.LogDir != "" {
			guestCfg.LogDir = opt.LogDir
		}

		grpcServer, grpcCleanup, err := guest.NewServer(guestCfg)
		if err != nil {
			slog.Error("failed to initialize guest gRPC server", "error", err)
		} else {
			s.grpcServer = grpcServer
			s.grpcCleanup = grpcCleanup
			slog.Info("guest services enabled (spec.debug is true)")
		}
	} else {
		slog.Info("guest services disabled; set spec.debug: true on the Task to enable ax ssh")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)

	// Metadata endpoints:
	// /metadata/v1alpha1/ax/task        the Task
	// /metadata/v1alpha1/ax/workspaces  every bound Workspace, as a YAML stream
	mux.HandleFunc("/metadata/v1alpha1/ax/task", s.handleTask)
	mux.HandleFunc("/metadata/v1alpha1/ax/workspaces", s.handleWorkspaces)
	// StatusPath reports activity and command state for automatic suspension.
	mux.HandleFunc(StatusPath, s.handleStatus)

	// spec.http.port: everything the runner doesn't serve itself goes to the
	// task's own server, since the router can only reach this port.
	if p := task.GetSpec().GetHttp().GetPort(); p > 0 && int(p) != port {
		s.passThroughPort = int(p)
		s.busyPath = task.GetSpec().GetIdle().GetBusyPath()
		// Only forwarded requests count as activity, never the endpoints above.
		mux.Handle("/", s.activity.track(newPassThrough(int(p))))
		slog.Info("forwarding other requests to the task", "port", p)
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.grpcServer != nil && r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			// Guest service calls are someone working in the sandbox (ax ssh).
			s.activity.begin()
			defer s.activity.end()
			s.grpcServer.ServeHTTP(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: handler,
	}
	s.server.Protocols = new(http.Protocols)
	s.server.Protocols.SetHTTP1(true)
	s.server.Protocols.SetUnencryptedHTTP2(true)

	return s
}

// Start begins serving the metadata HTTP and guest gRPC service in a background goroutine.
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return fmt.Errorf("metadata server listening on %s: %w", s.server.Addr, err)
	}

	slog.Info("metadata and guest server started", "addr", s.server.Addr)

	watchCtx, cancel := context.WithCancel(context.Background())
	s.stopWatch = cancel
	go s.activity.watchResume(watchCtx, resumeTick, resumeGap)

	go func() {
		if err := s.server.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metadata server error", "error", err)
		}
	}()

	return nil
}

// Stop gracefully stops the metadata and guest server.
func (s *Server) Stop(ctx context.Context) error {
	if s.stopWatch != nil {
		s.stopWatch()
	}
	if s.grpcCleanup != nil {
		s.grpcCleanup()
	}
	return s.server.Shutdown(ctx)
}

// UpdateState allows dynamic updates to the current Task and Workspaces.
func (s *Server) UpdateState(task *v1alpha1.Task, workspaces []*v1alpha1.Workspace) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.task = task
	s.workspaces = compactWorkspaces(workspaces)
}

// compactWorkspaces returns a copy of workspaces without nil entries.
func compactWorkspaces(workspaces []*v1alpha1.Workspace) []*v1alpha1.Workspace {
	out := make([]*v1alpha1.Workspace, 0, len(workspaces))
	for _, ws := range workspaces {
		if ws != nil {
			out = append(out, ws)
		}
	}
	return out
}

// SetWorkspaceReady updates whether the maiden run workspace setup has completed.
func (s *Server) SetWorkspaceReady(ready bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workspaceReady = ready
}

// IsWorkspaceReady returns whether the workspace setup has completed.
func (s *Server) IsWorkspaceReady() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.workspaceReady
}

// SetCommandExit records that the task command exited with the given code,
// for StatusPath.
func (s *Server) SetCommandExit(code int) {
	s.activity.setExit(code)
}

// Status returns what StatusPath serves, asking the task whether it is busy
// when spec.idle.busyPath is set.
func (s *Server) Status(ctx context.Context) RunnerStatus {
	st := s.activity.snapshot()
	if s.busyPath != "" && s.passThroughPort > 0 {
		busy, err := checkBusy(ctx, s.busyClient, s.passThroughPort, s.busyPath)
		st.Busy = busy
		if err != nil {
			st.BusyError = err.Error()
		}
	}
	return st
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.Status(r.Context()))
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	ready := s.workspaceReady
	s.mu.RUnlock()

	if !ready {
		http.Error(w, "workspace initializing", http.StatusServiceUnavailable)
		return
	}

	// With a pass-through, the task isn't ready until its own server accepts
	// connections. Substrate's wakeup probe polls this endpoint, so a request
	// that wakes a suspended task is held until the server is up instead of
	// failing while the command restarts. ?check=workspace is the control
	// plane asking about workspace setup only.
	if s.passThroughPort > 0 && r.URL.Query().Get("check") != "workspace" {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", s.passThroughPort), 500*time.Millisecond)
		if err != nil {
			http.Error(w, fmt.Sprintf("task server on port %d not listening yet", s.passThroughPort), http.StatusServiceUnavailable)
			return
		}
		_ = conn.Close()
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) handleTask(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	task := s.task
	s.mu.RUnlock()

	if task == nil {
		http.Error(w, "task metadata not available", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/yaml")
	if err := yaml.NewEncoder(w).Encode(task); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleWorkspaces serves every bound workspace as a multi-document YAML stream
// in the task's declaration order.
func (s *Server) handleWorkspaces(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	workspaces := s.workspaces
	s.mu.RUnlock()

	if len(workspaces) == 0 {
		http.Error(w, "workspace metadata not available", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/yaml")
	enc := yaml.NewEncoder(w)
	for _, ws := range workspaces {
		if err := enc.Encode(ws); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if err := enc.Close(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
