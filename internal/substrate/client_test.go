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

package substrate

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type mockControlClient struct {
	ateapipb.ControlClient

	mu                  sync.Mutex
	deleteCalls         []*ateapipb.DeleteActorRequest
	getCalls            []*ateapipb.GetActorRequest
	deleteErr           error
	actorRemaining      int // Number of GetActor calls before returning NotFound
	resumeErrs          []error
	resumeActor         *ateapipb.Actor
	deleteTemplateErrs  []error
	deleteTemplateCalls int
	templates           map[string]*ateapipb.ActorTemplate
	createTemplateErrs  []error
	createdTemplates    []*ateapipb.ActorTemplate
	suspendCalls        []*ateapipb.SuspendActorRequest
	suspendErrs         []error
	revertCalls         int
	revertErrs          []error
	workerPages         [][]*ateapipb.Worker
	workerPageTokens    []string
	assignments         map[string][]*ateapipb.ObjectRef
}

// popErr returns and drops the first queued error, if any.
func popErr(errs *[]error) error {
	if len(*errs) == 0 {
		return nil
	}
	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}

func (m *mockControlClient) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest, opts ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.suspendCalls = append(m.suspendCalls, req)
	if err := popErr(&m.suspendErrs); err != nil {
		return nil, err
	}
	return &ateapipb.SuspendActorResponse{}, nil
}

func (m *mockControlClient) RevertActor(ctx context.Context, req *ateapipb.RevertActorRequest, opts ...grpc.CallOption) (*ateapipb.RevertActorResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revertCalls++
	if err := popErr(&m.revertErrs); err != nil {
		return nil, err
	}
	return &ateapipb.RevertActorResponse{Actor: &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: req.GetActor().GetAtespace(), Name: req.GetActor().GetName()},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	}}, nil
}

// ListWorkers serves workerPages one page at a time, keyed by page token.
func (m *mockControlClient) ListWorkers(ctx context.Context, req *ateapipb.ListWorkersRequest, opts ...grpc.CallOption) (*ateapipb.ListWorkersResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	page := 0
	for i, tok := range m.workerPageTokens {
		if tok == req.GetOptions().GetPageToken() {
			page = i
		}
	}
	resp := &ateapipb.ListWorkersResponse{}
	if page < len(m.workerPages) {
		resp.Workers = m.workerPages[page]
	}
	if page+1 < len(m.workerPageTokens) {
		resp.NextPageToken = m.workerPageTokens[page+1]
	}
	return resp, nil
}

func (m *mockControlClient) ListWorkerActorAssignments(ctx context.Context, req *ateapipb.ListWorkerActorAssignmentsRequest, opts ...grpc.CallOption) (*ateapipb.ListWorkerActorAssignmentsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	refs, ok := m.assignments[req.GetWorker().GetName()]
	if !ok {
		return nil, status.Error(codes.NotFound, "worker not found")
	}
	resp := &ateapipb.ListWorkerActorAssignmentsResponse{}
	for _, r := range refs {
		resp.ActorAssignments = append(resp.ActorAssignments, &ateapipb.ActorAssignment{Actor: r})
	}
	return resp, nil
}

func (m *mockControlClient) GetActorTemplate(ctx context.Context, req *ateapipb.GetActorTemplateRequest, opts ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := req.GetActorTemplate().GetAtespace() + "/" + req.GetActorTemplate().GetName()
	if tmpl, ok := m.templates[key]; ok {
		return tmpl, nil
	}
	return nil, status.Error(codes.NotFound, "template not found")
}

func (m *mockControlClient) CreateActorTemplate(ctx context.Context, req *ateapipb.CreateActorTemplateRequest, opts ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.createTemplateErrs) > 0 {
		err := m.createTemplateErrs[0]
		m.createTemplateErrs = m.createTemplateErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	m.createdTemplates = append(m.createdTemplates, req.GetActorTemplate())
	return req.GetActorTemplate(), nil
}

func (m *mockControlClient) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.resumeErrs) > 0 {
		err := m.resumeErrs[0]
		m.resumeErrs = m.resumeErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return &ateapipb.ResumeActorResponse{Actor: m.resumeActor, Resumed: true}, nil
}

func (m *mockControlClient) DeleteActorTemplate(ctx context.Context, req *ateapipb.DeleteActorTemplateRequest, opts ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteTemplateCalls++
	if len(m.deleteTemplateErrs) > 0 {
		err := m.deleteTemplateErrs[0]
		m.deleteTemplateErrs = m.deleteTemplateErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return &ateapipb.ActorTemplate{}, nil
}

func (m *mockControlClient) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalls = append(m.deleteCalls, req)
	if m.deleteErr != nil {
		return nil, m.deleteErr
	}
	return &ateapipb.Actor{}, nil
}

func (m *mockControlClient) GetActor(ctx context.Context, req *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getCalls = append(m.getCalls, req)
	if m.actorRemaining > 0 {
		m.actorRemaining--
		return &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: req.GetActor().GetAtespace(),
				Name:     req.GetActor().GetName(),
			},
			Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
		}, nil
	}
	return nil, status.Error(codes.NotFound, "actor not found")
}

func TestClient_DeleteActor_AnyStateTrue(t *testing.T) {
	mock := &mockControlClient{
		actorRemaining: 0,
	}
	c := &Client{control: mock}
	ctx := context.Background()

	err := c.DeleteActor(ctx, "test-ns", "test-actor")
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.deleteCalls) != 1 {
		t.Fatalf("expected 1 DeleteActor call, got %d", len(mock.deleteCalls))
	}
	req := mock.deleteCalls[0]
	if req.GetActor().GetAtespace() != "test-ns" || req.GetActor().GetName() != "test-actor" {
		t.Errorf("unexpected actor ref in request: %v", req.GetActor())
	}
}

func TestClient_DeleteActor_PollsUntilNotFound(t *testing.T) {
	oldInterval := defaultRetryInterval
	defaultRetryInterval = 10 * time.Millisecond
	defer func() { defaultRetryInterval = oldInterval }()

	mock := &mockControlClient{
		actorRemaining: 3,
	}
	c := &Client{control: mock}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := c.DeleteActor(ctx, "test-ns", "test-actor")
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	// 1 fast-path GetActor call + 3 polling calls (last one returns NotFound)
	if len(mock.getCalls) != 4 {
		t.Errorf("expected 4 GetActor calls before NotFound, got %d", len(mock.getCalls))
	}
}

func TestClient_DeleteActor_NotFoundIsIgnored(t *testing.T) {
	mock := &mockControlClient{
		deleteErr: status.Error(codes.NotFound, "actor already gone"),
	}
	c := &Client{control: mock}
	ctx := context.Background()

	err := c.DeleteActor(ctx, "test-ns", "test-actor")
	if err != nil {
		t.Fatalf("expected nil error when actor is NotFound, got: %v", err)
	}
}

func TestClient_DeleteActor_PropagatesRPCError(t *testing.T) {
	mock := &mockControlClient{
		deleteErr: errors.New("internal atelet failure"),
	}
	c := &Client{control: mock}
	ctx := context.Background()

	err := c.DeleteActor(ctx, "test-ns", "test-actor")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestBuildActorTemplate(t *testing.T) {
	tmpl := BuildActorTemplate("", "test-tmpl", "", map[string]string{"FOO": "BAR"}, nil, "", nil, SnapshotScopeData)
	if tmpl.GetMetadata().GetAtespace() != "default" {
		t.Errorf("expected default atespace, got %q", tmpl.GetMetadata().GetAtespace())
	}
	if len(tmpl.GetContainers()) != 1 {
		t.Fatalf("expected 1 container, got %d", len(tmpl.GetContainers()))
	}
	container := tmpl.GetContainers()[0]
	if container.GetImage() != v1alpha1.DefaultTaskImage {
		t.Errorf("unexpected image: %q", container.GetImage())
	}
	probe := container.GetWakeupProbe().GetHttpGet()
	if probe == nil || probe.GetPath() != "/readyz" || probe.GetPort() != 80 {
		t.Errorf("unexpected wakeupProbe: %+v", container.GetWakeupProbe())
	}
	snapCfg := tmpl.GetSnapshotConfig()
	if snapCfg == nil {
		t.Fatal("expected SnapshotConfig to be set")
	}
	if snapCfg.GetStorageLocation() != "" {
		t.Errorf("expected empty StorageLocation by default in Substrate v1.0+, got %q", snapCfg.GetStorageLocation())
	}
	if snapCfg.GetPreferredFidelity() != ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES {
		t.Errorf("expected PreferredFidelity VOLUMES, got %v", snapCfg.GetPreferredFidelity())
	}

	// When snapshotsBucket is explicitly provided, StorageLocation is set.
	tmplWithBucket := BuildActorTemplate("", "test-tmpl", "", nil, nil, "gs://ate-snapshots/ax/", nil, SnapshotScopeData)
	if tmplWithBucket.GetSnapshotConfig().GetStorageLocation() != "gs://ate-snapshots/ax/" {
		t.Errorf("expected StorageLocation gs://ate-snapshots/ax/, got %q", tmplWithBucket.GetSnapshotConfig().GetStorageLocation())
	}
}

func TestClient_ResumeActor_WorkerPodIpsAndRetry(t *testing.T) {
	oldInterval := defaultRetryInterval
	defaultRetryInterval = 5 * time.Millisecond
	defer func() { defaultRetryInterval = oldInterval }()

	mock := &mockControlClient{
		resumeErrs: []error{
			status.Error(codes.Aborted, "operation aborted"),
			nil,
		},
		resumeActor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "task-1"},
			Status: &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
				WorkerAssignment: &ateapipb.WorkerAssignment{
					// The first (primary) IP is used, even when it is IPv6.
					WorkerPodIps: []string{"fd00::99", "10.244.0.99"},
				},
			},
		},
	}
	c := &Client{control: mock}
	actor, ip, err := c.ResumeActor(context.Background(), "default", "task-1")
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	if actor == nil {
		t.Fatal("expected non-nil actor")
	}
	if ip != "fd00::99" {
		t.Errorf("expected primary worker IP fd00::99, got %q", ip)
	}
}

func TestClient_ResumeActor_ResourceExhaustedNotRetried(t *testing.T) {
	mock := &mockControlClient{
		resumeErrs: []error{
			status.Error(codes.ResourceExhausted, "no worker available"),
		},
	}
	c := &Client{control: mock}
	_, _, err := c.ResumeActor(context.Background(), "default", "task-1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if status.Code(err) != codes.ResourceExhausted {
		t.Errorf("expected ResourceExhausted error, got %v", err)
	}
}

func TestClient_DeleteActorTemplate_RetriesOnAborted(t *testing.T) {
	oldInterval := defaultRetryInterval
	defaultRetryInterval = 5 * time.Millisecond
	defer func() { defaultRetryInterval = oldInterval }()

	mock := &mockControlClient{
		deleteTemplateErrs: []error{
			status.Error(codes.Aborted, "another operation is in progress for this ActorTemplate"),
			status.Error(codes.Aborted, "another operation is in progress for this ActorTemplate"),
			nil,
		},
	}
	c := &Client{control: mock}
	if err := c.DeleteActorTemplate(context.Background(), "default", "tmpl-1"); err != nil {
		t.Fatalf("DeleteActorTemplate failed: %v", err)
	}
	if mock.deleteTemplateCalls != 3 {
		t.Errorf("expected 3 DeleteActorTemplate calls, got %d", mock.deleteTemplateCalls)
	}
}

func TestClient_EnsureActorTemplateWithImage_Success(t *testing.T) {
	mock := &mockControlClient{}
	c := &Client{control: mock}

	created, err := c.EnsureActorTemplateWithImage(
		context.Background(),
		v1alpha1.DefaultSystemNamespace,
		"default-template",
		"default",
		"task-1-tmpl-abcd",
		"custom-image:v1",
		nil,
		map[string]string{"FOO": "BAR"},
	)
	if err != nil {
		t.Fatalf("EnsureActorTemplateWithImage failed: %v", err)
	}
	if created.GetMetadata().GetAtespace() != "default" || created.GetMetadata().GetName() != "task-1-tmpl-abcd" {
		t.Errorf("unexpected metadata: %+v", created.GetMetadata())
	}
	if len(created.GetContainers()) != 1 || created.GetContainers()[0].GetImage() != "custom-image:v1" {
		t.Errorf("expected container image custom-image:v1, got %+v", created.GetContainers())
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.createdTemplates) != 1 {
		t.Fatalf("expected 1 CreateActorTemplate request, got %d", len(mock.createdTemplates))
	}
}

func TestClient_EnsureActorTemplateWithImage_PropagatesError(t *testing.T) {
	mock := &mockControlClient{
		createTemplateErrs: []error{
			status.Error(codes.InvalidArgument, "actor_template.snapshot_config: invalid field"),
		},
	}
	c := &Client{control: mock}

	_, err := c.EnsureActorTemplateWithImage(
		context.Background(),
		v1alpha1.DefaultSystemNamespace,
		"default-template",
		"default",
		"task-1-tmpl-abcd",
		"custom-image:v1",
		nil,
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument error, got %v", err)
	}
	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.createdTemplates) != 0 {
		t.Errorf("expected no created templates on failure, got %d", len(mock.createdTemplates))
	}
}

func TestRetryOnCodes(t *testing.T) {
	oldInterval, oldMax := defaultRetryInterval, maxRetryInterval
	defaultRetryInterval, maxRetryInterval = time.Millisecond, 2*time.Millisecond
	defer func() { defaultRetryInterval, maxRetryInterval = oldInterval, oldMax }()

	t.Run("non-retryable returns immediately", func(t *testing.T) {
		calls := 0
		wantErr := status.Error(codes.Internal, "boom")
		err := retryOnCodes(context.Background(), func() error {
			calls++
			return wantErr
		}, codes.Aborted)
		if err != wantErr || calls != 1 {
			t.Errorf("got err=%v (%T) calls=%d, want original Internal error after 1 call", err, err, calls)
		}
	})

	t.Run("non-retryable on final attempt is unwrapped", func(t *testing.T) {
		calls := 0
		wantErr := status.Error(codes.Internal, "boom")
		err := retryOnCodes(context.Background(), func() error {
			calls++
			if calls == defaultRetryAttempts {
				return wantErr
			}
			return status.Error(codes.Aborted, "busy")
		}, codes.Aborted)
		if err != wantErr || calls != defaultRetryAttempts {
			t.Errorf("got err=%v (%T) calls=%d, want original Internal error after %d calls", err, err, calls, defaultRetryAttempts)
		}
	})

	t.Run("exhausts attempts", func(t *testing.T) {
		calls := 0
		err := retryOnCodes(context.Background(), func() error {
			calls++
			return status.Error(codes.Aborted, "busy")
		}, codes.Aborted)
		if status.Code(err) != codes.Aborted || calls != defaultRetryAttempts {
			t.Errorf("got err=%v calls=%d, want Aborted after %d calls", err, calls, defaultRetryAttempts)
		}
	})

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := retryOnCodes(ctx, func() error {
			return status.Error(codes.Aborted, "busy")
		}, codes.Aborted)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("retries Unavailable and succeeds", func(t *testing.T) {
		calls := 0
		err := retryOnCodes(context.Background(), func() error {
			calls++
			if calls < 3 {
				return status.Error(codes.Unavailable, "ateapi rolling out")
			}
			return nil
		}, codes.Unavailable)
		if err != nil || calls != 3 {
			t.Errorf("got err=%v calls=%d, want nil after 3 calls", err, calls)
		}
	})
}

func TestIsTransientError(t *testing.T) {
	if IsTransientError(nil) {
		t.Error("expected nil error to be non-transient")
	}
	if !IsTransientError(status.Error(codes.Unavailable, "connection refused")) {
		t.Error("expected codes.Unavailable to be transient")
	}
	if !IsTransientError(status.Error(codes.DeadlineExceeded, "rpc deadline exceeded")) {
		t.Error("expected codes.DeadlineExceeded to be transient")
	}
	if !IsTransientError(context.DeadlineExceeded) {
		t.Error("expected context.DeadlineExceeded to be transient")
	}
	if IsTransientError(status.Error(codes.InvalidArgument, "bad spec")) {
		t.Error("expected codes.InvalidArgument to be non-transient")
	}
}

func TestResourceLimits(t *testing.T) {
	tests := []struct {
		name string
		reqs *v1alpha1.ResourcesSpec
		want *ateapipb.ResourceRequirements
	}{
		{name: "nil"},
		{name: "empty", reqs: &v1alpha1.ResourcesSpec{}},
		{name: "empty limits", reqs: &v1alpha1.ResourcesSpec{Limits: &v1alpha1.ResourceList{}}},
		{
			name: "cpu limit",
			reqs: &v1alpha1.ResourcesSpec{Limits: &v1alpha1.ResourceList{Cpu: "2"}},
			want: &ateapipb.ResourceRequirements{Limits: &ateapipb.Resources{CpuMillicores: 2000, MemoryBytes: DefaultMemoryBytes}},
		},
		{
			name: "memory limit",
			reqs: &v1alpha1.ResourcesSpec{Limits: &v1alpha1.ResourceList{Memory: "4Gi"}},
			want: &ateapipb.ResourceRequirements{Limits: &ateapipb.Resources{CpuMillicores: DefaultCPUMillicores, MemoryBytes: 4 << 30}},
		},
		{
			name: "cpu and memory limits",
			reqs: &v1alpha1.ResourcesSpec{
				Limits: &v1alpha1.ResourceList{Cpu: "2", Memory: "4Gi"},
			},
			want: &ateapipb.ResourceRequirements{Limits: &ateapipb.Resources{
				CpuMillicores: 2000,
				MemoryBytes:   4 << 30,
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResourceLimits(tt.reqs)
			if !proto.Equal(got, tt.want) {
				t.Fatalf("ResourceLimits() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildActorTemplate_Resources(t *testing.T) {
	limits := &ateapipb.ResourceRequirements{Limits: &ateapipb.Resources{
		CpuMillicores: 2000,
		MemoryBytes:   4 << 30,
	}}
	tmpl := BuildActorTemplate("default", "task-tmpl-01234567", "ghcr.io/my-org/agent@sha256:abc", nil, nil, "", limits, "")
	if !proto.Equal(tmpl.GetResources(), limits) {
		t.Fatalf("template resources = %v, want %v", tmpl.GetResources(), limits)
	}

	// Without explicit limits the template carries the default CPU and memory
	// limits required by Substrate.
	wantDefault := &ateapipb.ResourceRequirements{Limits: &ateapipb.Resources{
		CpuMillicores: DefaultCPUMillicores,
		MemoryBytes:   DefaultMemoryBytes,
	}}
	tmpl = BuildActorTemplate("default", "task-tmpl-01234567", "ghcr.io/my-org/agent@sha256:abc", nil, nil, "", nil, "")
	if !proto.Equal(tmpl.GetResources(), wantDefault) {
		t.Fatalf("template without limits has resources %v, want %v", tmpl.GetResources(), wantDefault)
	}
}

func TestClient_SuspendActorSynchronous(t *testing.T) {
	for _, tc := range []struct {
		name      string
		errs      []error
		wantCalls int
		wantErr   bool
	}{
		{"ok", nil, 1, false},
		{"retries Aborted", []error{status.Error(codes.Aborted, "busy")}, 2, false},
		{"fails on FailedPrecondition", []error{status.Error(codes.FailedPrecondition, "not running")}, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockControlClient{suspendErrs: tc.errs}
			c := &Client{control: mock}
			err := c.SuspendActorSynchronous(context.Background(), "ns", "a1")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			mock.mu.Lock()
			defer mock.mu.Unlock()
			if len(mock.suspendCalls) != tc.wantCalls {
				t.Fatalf("SuspendActor calls = %d, want %d", len(mock.suspendCalls), tc.wantCalls)
			}
			req := mock.suspendCalls[0]
			if req.GetActor().GetAtespace() != "ns" || req.GetActor().GetName() != "a1" {
				t.Errorf("actor = %v, want ns/a1", req.GetActor())
			}
			if got := req.GetOptions().GetMode(); got != ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS {
				t.Errorf("mode = %v, want DURABILITY_MODE_SYNCHRONOUS", got)
			}
		})
	}
}

func TestClient_RevertActor(t *testing.T) {
	for _, tc := range []struct {
		name      string
		errs      []error
		wantCalls int
		wantErr   bool
	}{
		{"ok", nil, 1, false},
		{"retries Unavailable", []error{status.Error(codes.Unavailable, "down")}, 2, false},
		{"fails on FailedPrecondition", []error{status.Error(codes.FailedPrecondition, "no snapshot")}, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockControlClient{revertErrs: tc.errs}
			c := &Client{control: mock}
			actor, err := c.RevertActor(context.Background(), "ns", "a1")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
				t.Errorf("actor state = %v, want SUSPENDED", actor.GetStatus().GetState())
			}
			mock.mu.Lock()
			defer mock.mu.Unlock()
			if mock.revertCalls != tc.wantCalls {
				t.Errorf("RevertActor calls = %d, want %d", mock.revertCalls, tc.wantCalls)
			}
		})
	}
}

func worker(name string, state ateapipb.WorkerState) *ateapipb.Worker {
	return &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Status:   &ateapipb.WorkerStatus{State: state},
	}
}

func TestClient_ListDrainingWorkers(t *testing.T) {
	for _, tc := range []struct {
		name   string
		pages  [][]*ateapipb.Worker
		tokens []string
		want   []string
	}{
		{"none", nil, nil, nil},
		{"filters by state", [][]*ateapipb.Worker{{
			worker("w1", ateapipb.WorkerState_WORKER_STATE_ACTIVE),
			worker("w2", ateapipb.WorkerState_WORKER_STATE_DRAINING),
		}}, []string{""}, []string{"w2"}},
		{"follows pages", [][]*ateapipb.Worker{
			{worker("w1", ateapipb.WorkerState_WORKER_STATE_DRAINING)},
			{worker("w2", ateapipb.WorkerState_WORKER_STATE_ACTIVE), worker("w3", ateapipb.WorkerState_WORKER_STATE_DRAINING)},
		}, []string{"", "p2"}, []string{"w1", "w3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{control: &mockControlClient{workerPages: tc.pages, workerPageTokens: tc.tokens}}
			got, err := c.ListDrainingWorkers(context.Background())
			if err != nil {
				t.Fatalf("ListDrainingWorkers: %v", err)
			}
			var names []string
			for _, w := range got {
				names = append(names, w.GetMetadata().GetName())
			}
			if strings.Join(names, ",") != strings.Join(tc.want, ",") {
				t.Errorf("draining = %v, want %v", names, tc.want)
			}
		})
	}
}

func TestClient_ListWorkerActors(t *testing.T) {
	mock := &mockControlClient{assignments: map[string][]*ateapipb.ObjectRef{
		"w1": {{Atespace: "ns", Name: "a1"}, {Atespace: "ns", Name: "a2"}},
	}}
	c := &Client{control: mock}
	for _, tc := range []struct {
		worker  string
		want    string
		wantErr bool
	}{
		{"w1", "ns/a1,ns/a2", false},
		{"gone", "", true},
	} {
		got, err := c.ListWorkerActors(context.Background(), worker(tc.worker, ateapipb.WorkerState_WORKER_STATE_DRAINING))
		if (err != nil) != tc.wantErr {
			t.Fatalf("%s: err = %v, wantErr %v", tc.worker, err, tc.wantErr)
		}
		var refs []string
		for _, r := range got {
			refs = append(refs, r.GetAtespace()+"/"+r.GetName())
		}
		if strings.Join(refs, ",") != tc.want {
			t.Errorf("%s: actors = %v, want %s", tc.worker, refs, tc.want)
		}
	}
}

func TestIsWorkerLossCrash(t *testing.T) {
	crashed := func(msg string) *ateapipb.Actor {
		return &ateapipb.Actor{Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			Crash: &ateapipb.ActorCrash{Message: msg},
		}}
	}
	for _, tc := range []struct {
		name  string
		actor *ateapipb.Actor
		want  bool
	}{
		{"pod went away", crashed("worker pod went away while hosting the actor"), true},
		{"worker restarted", crashed("worker restarted while hosting the actor"), true},
		{"worker draining", crashed("assigned worker is draining"), true},
		{"boot failure", crashed("atelet RunWorkload: wakeup probe never returned 200"), false},
		{"no crash info", &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_CRASHED}}, false},
		{"running", &ateapipb.Actor{Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			Crash: &ateapipb.ActorCrash{Message: "worker pod went away while hosting the actor"},
		}}, false},
		{"nil", nil, false},
	} {
		if got := IsWorkerLossCrash(tc.actor); got != tc.want {
			t.Errorf("%s: IsWorkerLossCrash = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestBuildEgressPolicy(t *testing.T) {
	creds := []*v1alpha1.CredentialInjection{{
		Header:        "Authorization",
		Prefix:        "Bearer ",
		CredentialUri: "ate-secret://k8s.io/default/ate-credentials/tok/token",
	}}
	wantEffects := &ateapipb.HttpRuleEffects{
		ReplaceHeaders: []*ateapipb.CredentialHeader{{
			Header:        "Authorization",
			Prefix:        "Bearer ",
			CredentialUri: "ate-secret://k8s.io/default/ate-credentials/tok/token",
		}},
	}

	got := BuildEgressPolicy("cred-test", []*v1alpha1.EgressRule{
		{
			Hosts:       []string{"aiplatform.googleapis.com", "https://api.github.com"},
			Credentials: creds,
		},
		{
			Hosts:       []string{"http://switchboard.switchboard.svc.cluster.local"},
			Ports:       []int32{8080},
			Credentials: creds,
		},
		{
			Hosts: []string{"http://plain-default-port.svc.cluster.local"},
		},
		{
			Hosts: []string{"tls.example.com", "http://plain.example.com"},
			Ports: []int32{8443},
		},
	})

	want := &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "cred-test", Name: "default"},
		Rules: []*ateapipb.EgressRule{
			{
				Https: &ateapipb.HttpsRule{
					Hostnames: []string{"aiplatform.googleapis.com", "api.github.com"},
					Ports:     &ateapipb.Ports{Numbers: []int32{443}},
					Effects:   wantEffects,
				},
			},
			{
				Http: &ateapipb.HttpRule{
					Hostnames: []string{"switchboard.switchboard.svc.cluster.local"},
					Ports:     &ateapipb.Ports{Numbers: []int32{8080}},
					Effects:   wantEffects,
				},
			},
			{
				Http: &ateapipb.HttpRule{
					Hostnames: []string{"plain-default-port.svc.cluster.local"},
					Ports:     &ateapipb.Ports{Numbers: []int32{80}},
				},
			},
			{
				Https: &ateapipb.HttpsRule{
					Hostnames: []string{"tls.example.com"},
					Ports:     &ateapipb.Ports{Numbers: []int32{8443}},
				},
			},
			{
				Http: &ateapipb.HttpRule{
					Hostnames: []string{"plain.example.com"},
					Ports:     &ateapipb.Ports{Numbers: []int32{8443}},
				},
			},
		},
	}
	if !proto.Equal(got, want) {
		t.Fatalf("BuildEgressPolicy() =\n%v\nwant:\n%v", got, want)
	}
}
