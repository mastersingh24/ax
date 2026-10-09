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
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func envValue(c *ateapipb.Container, name string) (string, bool) {
	for _, e := range c.GetEnv() {
		if e.GetName() == name {
			return e.GetValue(), true
		}
	}
	return "", false
}

func TestBuildActorTemplate_EgressTrustBundle(t *testing.T) {
	t.Setenv("AX_EGRESS_MITM_TRUST_BUNDLE", "true")
	tmpl := BuildActorTemplate("default", "t", "img", map[string]string{"CURL_CA_BUNDLE": "/mine.pem"}, nil, "gs://b/", nil, "")

	var found bool
	for _, v := range tmpl.GetVolumes() {
		tb := v.GetSystemInfo().GetDataSources()
		if len(tb) != 1 {
			continue
		}
		// One file holding the gateway CA and Substrate's public roots.
		names := tb[0].GetTrustBundle().GetNames()
		if slices.Contains(names, egressTrustBundleName) && slices.Contains(names, systemRootsBundleName) {
			found = true
		}
	}
	if !found {
		t.Fatalf("no egress trust bundle volume in %v", tmpl.GetVolumes())
	}
	c := tmpl.GetContainers()[0]
	var mounted bool
	for _, m := range c.GetVolumeMounts() {
		if m.GetMountPath() == egressTrustBundleDir {
			mounted = true
		}
	}
	if !mounted {
		t.Errorf("trust bundle not mounted at %s: %v", egressTrustBundleDir, c.GetVolumeMounts())
	}
	if v, _ := envValue(c, "SSL_CERT_FILE"); v != egressTrustBundleFile {
		t.Errorf("SSL_CERT_FILE = %q, want %q", v, egressTrustBundleFile)
	}
	if v, _ := envValue(c, "CURL_CA_BUNDLE"); v != "/mine.pem" {
		t.Errorf("task env should win: CURL_CA_BUNDLE = %q", v)
	}
	// SSL_CERT_DIR would replace the image's trust store instead of adding to it.
	if v, ok := envValue(c, "SSL_CERT_DIR"); ok {
		t.Errorf("SSL_CERT_DIR = %q, want unset", v)
	}
}

func TestBuildActorTemplate_NoTrustBundleByDefault(t *testing.T) {
	t.Setenv("AX_EGRESS_MITM_TRUST_BUNDLE", "")
	tmpl := BuildActorTemplate("default", "t", "img", nil, nil, "gs://b/", nil, "")
	if got := len(tmpl.GetVolumes()); got != 1 {
		t.Errorf("expected only the workspace volume, got %d", got)
	}
	if _, ok := envValue(tmpl.GetContainers()[0], "SSL_CERT_FILE"); ok {
		t.Error("SSL_CERT_FILE should not be set without AX_EGRESS_MITM_TRUST_BUNDLE")
	}
}

func TestBuildActorTemplate_SnapshotScope(t *testing.T) {
	for _, tc := range []struct {
		scope SnapshotScope
		want  ateapipb.SnapshotContentScope
	}{
		{"", ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL},
		{SnapshotScopeFull, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL},
		{SnapshotScopeData, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA},
	} {
		cfg := BuildActorTemplate("default", "t", "img", nil, nil, "gs://b/", nil, tc.scope).GetSnapshotConfig()
		if cfg.GetOnCommit() != tc.want {
			t.Errorf("scope %q: onCommit = %v, want %v", tc.scope, cfg.GetOnCommit(), tc.want)
		}
		if cfg.GetStorageLocation() != "gs://b/" {
			t.Errorf("scope %q: storageLocation = %q", tc.scope, cfg.GetStorageLocation())
		}
	}
}

func TestParseSnapshotScope(t *testing.T) {
	for in, want := range map[string]SnapshotScope{"": SnapshotScopeFull, "full": SnapshotScopeFull, "FULL": SnapshotScopeFull, " data ": SnapshotScopeData} {
		got, err := ParseSnapshotScope(in)
		if err != nil || got != want {
			t.Errorf("ParseSnapshotScope(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"golden", "data_on_golden", "memory"} {
		if _, err := ParseSnapshotScope(in); err == nil {
			t.Errorf("ParseSnapshotScope(%q) succeeded, want error", in)
		}
	}
}

// templateRecorder captures the template EnsureActorTemplateWithImage creates.
type templateRecorder struct {
	ateapipb.ControlClient
	created *ateapipb.ActorTemplate
}

func (r *templateRecorder) GetActorTemplate(context.Context, *ateapipb.GetActorTemplateRequest, ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	return nil, status.Error(codes.NotFound, "no template")
}

func (r *templateRecorder) CreateActorTemplate(_ context.Context, req *ateapipb.CreateActorTemplateRequest, _ ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	r.created = req.GetActorTemplate()
	return r.created, nil
}

func TestEnsureActorTemplateWithImage_UsesClientScope(t *testing.T) {
	for _, tc := range []struct {
		scope SnapshotScope
		want  ateapipb.SnapshotContentScope
	}{
		{"", ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL},
		{SnapshotScopeData, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA},
	} {
		rec := &templateRecorder{}
		c := &Client{control: rec, snapshotScope: tc.scope}
		limits := &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "cpu", Quantity: "2"}}}
		if _, err := c.EnsureActorTemplateWithImage(context.Background(), "ax-system", "base", "default", "t", "img", limits); err != nil {
			t.Fatal(err)
		}
		if got := rec.created.GetSnapshotConfig().GetOnCommit(); got != tc.want {
			t.Errorf("client scope %q: onCommit = %v, want %v", tc.scope, got, tc.want)
		}
		if !proto.Equal(rec.created.GetResources(), limits) {
			t.Errorf("resources = %v, want %v", rec.created.GetResources(), limits)
		}
	}
}

func TestWorkerIP(t *testing.T) {
	actor := func(ips ...string) *ateapipb.Actor {
		return &ateapipb.Actor{Status: &ateapipb.ActorStatus{WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIps: ips}}}
	}
	for _, tc := range []struct {
		actor *ateapipb.Actor
		want  string
	}{
		{nil, ""},
		{&ateapipb.Actor{}, ""},
		{actor(), ""},
		{actor(""), ""},
		{actor("10.0.0.5"), "10.0.0.5"},
		{actor("10.0.0.5", "fd00::5"), "10.0.0.5"},
		{actor("", "fd00::5"), "fd00::5"},
	} {
		if got := WorkerIP(tc.actor); got != tc.want {
			t.Errorf("WorkerIP(%v) = %q, want %q", tc.actor, got, tc.want)
		}
	}
}

func TestResourceLimits(t *testing.T) {
	tests := []struct {
		name string
		reqs *v1alpha1.ResourceReqs
		want *ateapipb.Resources
	}{
		{name: "nil"},
		{name: "empty", reqs: &v1alpha1.ResourceReqs{}},
		{name: "empty limits", reqs: &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{}}},
		{
			name: "cpu limit",
			reqs: &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Cpu: "2"}},
			want: &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "cpu", Quantity: "2"}}},
		},
		{
			name: "memory limit",
			reqs: &v1alpha1.ResourceReqs{Limits: &v1alpha1.ResourceList{Memory: "4Gi"}},
			want: &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "memory", Quantity: "4Gi"}}},
		},
		{
			name: "cpu and memory limits",
			reqs: &v1alpha1.ResourceReqs{
				Limits: &v1alpha1.ResourceList{Cpu: "2", Memory: "4Gi"},
			},
			want: &ateapipb.Resources{Limits: []*ateapipb.Limits{
				{Name: "cpu", Quantity: "2"},
				{Name: "memory", Quantity: "4Gi"},
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
	limits := &ateapipb.Resources{Limits: []*ateapipb.Limits{
		{Name: "cpu", Quantity: "2"},
		{Name: "memory", Quantity: "4Gi"},
	}}
	tmpl := BuildActorTemplate("default", "task-tmpl-01234567", "ghcr.io/my-org/agent@sha256:abc", nil, nil, "", limits, "")
	if !proto.Equal(tmpl.GetResources(), limits) {
		t.Fatalf("template resources = %v, want %v", tmpl.GetResources(), limits)
	}

	// Without limits the template must not carry a resources block, so the
	// worker defaults keep applying.
	tmpl = BuildActorTemplate("default", "task-tmpl-01234567", "ghcr.io/my-org/agent@sha256:abc", nil, nil, "", nil, "")
	if tmpl.GetResources() != nil {
		t.Fatalf("template without limits has resources %v, want none", tmpl.GetResources())
	}
}
