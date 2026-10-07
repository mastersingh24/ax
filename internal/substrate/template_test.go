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
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
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
	tmpl := BuildActorTemplate("default", "t", "img", map[string]string{"CURL_CA_BUNDLE": "/mine.pem"}, nil, "gs://b/")

	var found bool
	for _, v := range tmpl.GetVolumes() {
		if tb := v.GetSystemInfo().GetDataSources(); len(tb) == 1 && tb[0].GetTrustBundle().GetName() == egressTrustBundleName {
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
}

func TestBuildActorTemplate_NoTrustBundleByDefault(t *testing.T) {
	t.Setenv("AX_EGRESS_MITM_TRUST_BUNDLE", "")
	tmpl := BuildActorTemplate("default", "t", "img", nil, nil, "gs://b/")
	if got := len(tmpl.GetVolumes()); got != 1 {
		t.Errorf("expected only the workspace volume, got %d", got)
	}
	if _, ok := envValue(tmpl.GetContainers()[0], "SSL_CERT_FILE"); ok {
		t.Error("SSL_CERT_FILE should not be set without AX_EGRESS_MITM_TRUST_BUNDLE")
	}
}
