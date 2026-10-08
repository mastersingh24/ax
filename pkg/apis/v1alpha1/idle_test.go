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

package v1alpha1_test

import (
	"strings"
	"testing"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
	"gopkg.in/yaml.v3"
)

func TestValidateTask_IdleAndOnCompletion(t *testing.T) {
	task := func(spec *v1alpha1.TaskSpec) *v1alpha1.Task {
		return &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "t", Atespace: "default"}, Spec: spec}
	}
	withPort := func(idle *v1alpha1.TaskIdle) *v1alpha1.TaskSpec {
		return &v1alpha1.TaskSpec{Http: &v1alpha1.TaskHTTP{Port: 8484}, Idle: idle}
	}

	valid := map[string]*v1alpha1.TaskSpec{
		"idle":             withPort(&v1alpha1.TaskIdle{SuspendAfter: "10m"}),
		"idle with busy":   withPort(&v1alpha1.TaskIdle{SuspendAfter: "90s", BusyPath: "/busy"}),
		"minimum":          withPort(&v1alpha1.TaskIdle{SuspendAfter: v1alpha1.MinIdleSuspendAfter.String()}),
		"keep":             {OnCompletion: v1alpha1.OnCompletionKeep},
		"suspend":          {OnCompletion: v1alpha1.OnCompletionSuspend},
		"suspend, no port": {Command: []string{"true"}, OnCompletion: v1alpha1.OnCompletionSuspend},
		"idle and suspend": {Http: &v1alpha1.TaskHTTP{Port: 8484}, Idle: &v1alpha1.TaskIdle{SuspendAfter: "5m"}, OnCompletion: v1alpha1.OnCompletionSuspend},
	}
	for name, spec := range valid {
		if err := v1alpha1.ValidateTask(task(spec)); err != nil {
			t.Errorf("%s: unexpected error: %v", name, err)
		}
	}

	invalid := map[string]struct {
		spec *v1alpha1.TaskSpec
		want string
	}{
		"missing suspendAfter": {withPort(&v1alpha1.TaskIdle{}), "spec.idle.suspendAfter"},
		"bad duration":         {withPort(&v1alpha1.TaskIdle{SuspendAfter: "ten minutes"}), "invalid duration"},
		"too short":            {withPort(&v1alpha1.TaskIdle{SuspendAfter: "1s"}), "minimum"},
		"negative":             {withPort(&v1alpha1.TaskIdle{SuspendAfter: "-5m"}), "minimum"},
		"no http port":         {&v1alpha1.TaskSpec{Idle: &v1alpha1.TaskIdle{SuspendAfter: "10m"}}, "spec.http.port"},
		"relative busyPath":    {withPort(&v1alpha1.TaskIdle{SuspendAfter: "10m", BusyPath: "busy"}), "spec.idle.busyPath"},
		"unknown onCompletion": {&v1alpha1.TaskSpec{OnCompletion: "Delete"}, "spec.onCompletion"},
		"lowercase suspend":    {&v1alpha1.TaskSpec{OnCompletion: "suspend"}, "spec.onCompletion"},
	}
	for name, tc := range invalid {
		err := v1alpha1.ValidateTask(task(tc.spec))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error = %v, want one mentioning %q", name, err, tc.want)
		}
	}
}

func TestTaskSpec_AutoSuspend(t *testing.T) {
	var nilSpec *v1alpha1.TaskSpec
	if nilSpec.AutoSuspends() || nilSpec.IdleSuspendAfter() != 0 {
		t.Error("a nil spec must not auto-suspend")
	}
	spec := &v1alpha1.TaskSpec{Idle: &v1alpha1.TaskIdle{SuspendAfter: "2m"}}
	if got := spec.IdleSuspendAfter(); got != 2*time.Minute {
		t.Errorf("IdleSuspendAfter = %s, want 2m", got)
	}
	if !spec.AutoSuspends() || spec.SuspendOnCompletion() {
		t.Error("an idle policy alone should auto-suspend, but not on completion")
	}
	spec = &v1alpha1.TaskSpec{OnCompletion: v1alpha1.OnCompletionSuspend}
	if !spec.AutoSuspends() || !spec.SuspendOnCompletion() || spec.IdleSuspendAfter() != 0 {
		t.Error("onCompletion: Suspend alone should auto-suspend on completion only")
	}
	if (&v1alpha1.TaskSpec{OnCompletion: v1alpha1.OnCompletionKeep}).AutoSuspends() {
		t.Error("onCompletion: Keep must not auto-suspend")
	}
}

func TestTask_IdleYAMLRoundTrip(t *testing.T) {
	doc := strings.Join([]string{
		"apiVersion: ax.io/v1alpha1",
		"kind: Task",
		"metadata:",
		"  name: t",
		"spec:",
		"  http:",
		"    port: 8484",
		"  idle:",
		"    suspendAfter: 10m",
		"    busyPath: /busy",
		"  onCompletion: Suspend",
	}, "\n")
	var task v1alpha1.Task
	if err := yaml.Unmarshal([]byte(doc), &task); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	spec := task.GetSpec()
	if spec.GetIdle().GetSuspendAfter() != "10m" || spec.GetIdle().GetBusyPath() != "/busy" || spec.GetOnCompletion() != "Suspend" {
		t.Fatalf("decoded spec = %v", spec)
	}
	out, err := yaml.Marshal(&task)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"suspendAfter: 10m", "busyPath: /busy", "onCompletion: Suspend"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("encoded YAML missing %q:\n%s", want, out)
		}
	}
}
