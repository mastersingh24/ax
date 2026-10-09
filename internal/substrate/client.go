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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ClientOptions configures connection to the Substrate Control API server.
type ClientOptions struct {
	Target      string
	Authority   string
	TokenFile   string
	CAFile      string
	InsecureTLS bool
	Plaintext   bool
	// SnapshotScope is what the snapshots of the ActorTemplates this client
	// creates capture. Empty means SnapshotScopeFull.
	SnapshotScope SnapshotScope
}

// SnapshotScope selects what an actor's snapshots capture, and so what a
// suspended task looks like when it wakes. It becomes the ActorTemplate's
// snapshotConfig.onCommit, which on Agent Substrate v0.4 governs both the
// node-local checkpoint a pause takes and the snapshot a suspend uploads.
type SnapshotScope string

const (
	// SnapshotScopeFull captures process memory, the root filesystem and the
	// durable directories (/workspace). A woken task carries on where it was:
	// the command is not restarted and in-memory state survives. Snapshots are
	// larger and take longer to upload and restore.
	SnapshotScopeFull SnapshotScope = "full"
	// SnapshotScopeData captures only the durable directories. A woken task's
	// containers start afresh from the image with /workspace restored, so the
	// runner and spec.command start again and anything kept only in memory or
	// outside /workspace is lost. Snapshots are small.
	SnapshotScopeData SnapshotScope = "data"
)

// ParseSnapshotScope parses "full" or "data", case-insensitively. Empty
// means full.
func ParseSnapshotScope(s string) (SnapshotScope, error) {
	switch SnapshotScope(strings.ToLower(strings.TrimSpace(s))) {
	case "", SnapshotScopeFull:
		return SnapshotScopeFull, nil
	case SnapshotScopeData:
		return SnapshotScopeData, nil
	}
	return "", fmt.Errorf("invalid snapshot scope %q: must be %q or %q", s, SnapshotScopeFull, SnapshotScopeData)
}

func (s SnapshotScope) contentScope() ateapipb.SnapshotContentScope {
	if s == SnapshotScopeData {
		return ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
	}
	return ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
}

type tokenAuth struct {
	tokenFile string
}

func (t tokenAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	if t.tokenFile == "" {
		return nil, nil
	}
	b, err := os.ReadFile(t.tokenFile)
	if err != nil {
		return nil, fmt.Errorf("reading token file %q: %w", t.tokenFile, err)
	}
	return map[string]string{
		"authorization": "Bearer " + strings.TrimSpace(string(b)),
	}, nil
}

func (t tokenAuth) RequireTransportSecurity() bool {
	return false
}

// Client wraps the Substrate Control API client.
type Client struct {
	conn          *grpc.ClientConn
	control       ateapipb.ControlClient
	snapshotScope SnapshotScope
}

// NewClientWithOptions connects to Substrate with full TLS and auth configuration.
func NewClientWithOptions(opts ClientOptions, extraDialOpts ...grpc.DialOption) (*Client, error) {
	if opts.Target == "" {
		opts.Target = "api.ate-system.svc.cluster.local:443"
	}

	// Auto-detect token file if not specified
	if opts.TokenFile == "" {
		if envToken := os.Getenv("SUBSTRATE_TOKEN_FILE"); envToken != "" {
			opts.TokenFile = envToken
		} else if _, err := os.Stat("/var/run/secrets/ateapi/token"); err == nil {
			opts.TokenFile = "/var/run/secrets/ateapi/token"
		}
	}

	// Auto-detect CA file if not specified
	if opts.CAFile == "" {
		if envCA := os.Getenv("SUBSTRATE_CA_FILE"); envCA != "" {
			opts.CAFile = envCA
		} else if _, err := os.Stat("/run/servicedns-ca/trust-bundle.pem"); err == nil {
			opts.CAFile = "/run/servicedns-ca/trust-bundle.pem"
		} else if _, err := os.Stat("/run/servicedns-ca/ca.pem"); err == nil {
			opts.CAFile = "/run/servicedns-ca/ca.pem"
		}
	}

	dialOpts := append([]grpc.DialOption{}, extraDialOpts...)

	if opts.Plaintext {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		tlsConfig := &tls.Config{}
		if opts.InsecureTLS {
			tlsConfig.InsecureSkipVerify = true
		}
		if opts.Authority != "" {
			tlsConfig.ServerName = opts.Authority
		} else if strings.Contains(opts.Target, "api.ate-system.svc") {
			tlsConfig.ServerName = "api.ate-system.svc"
		}

		if opts.CAFile != "" {
			caCert, err := os.ReadFile(opts.CAFile)
			if err != nil {
				return nil, fmt.Errorf("reading CA file %q: %w", opts.CAFile, err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to parse CA certificate from %q", opts.CAFile)
			}
			tlsConfig.RootCAs = pool
		}

		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	authority := opts.Authority
	if authority == "" && strings.Contains(opts.Target, "api.ate-system.svc") {
		authority = "api.ate-system.svc"
	}
	if authority != "" {
		dialOpts = append(dialOpts, grpc.WithAuthority(authority))
	}

	if opts.TokenFile != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(tokenAuth{tokenFile: opts.TokenFile}))
	}

	conn, err := grpc.NewClient(opts.Target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Substrate control service at %s: %w", opts.Target, err)
	}
	return &Client{
		conn:          conn,
		control:       ateapipb.NewControlClient(conn),
		snapshotScope: opts.SnapshotScope,
	}, nil
}

// SnapshotScope reports the scope of the ActorTemplates this client creates.
func (c *Client) SnapshotScope() SnapshotScope {
	if c.snapshotScope == "" {
		return SnapshotScopeFull
	}
	return c.snapshotScope
}

// NewClient establishes a connection to the Substrate Control API server.
func NewClient(target string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) > 0 {
		if target == "" {
			target = "api.ate-system.svc.cluster.local:443"
		}
		conn, err := grpc.NewClient(target, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to Substrate control service at %s: %w", target, err)
		}
		return &Client{
			conn:    conn,
			control: ateapipb.NewControlClient(conn),
		}, nil
	}

	// Auto-configure options when no explicit dial options given
	return NewClientWithOptions(ClientOptions{Target: target})
}

// Close closes the gRPC connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// EnsureAtespace creates the atespace if it does not already exist.
func (c *Client) EnsureAtespace(ctx context.Context, atespace string) error {
	req := &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{
			Metadata: &ateapipb.ResourceMetadata{
				Name: atespace,
			},
		},
	}
	_, err := c.control.CreateAtespace(ctx, req)
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("creating atespace %q: %w", atespace, err)
	}
	return nil
}

// GetActorTemplate fetches an ActorTemplate by name.
func (c *Client) GetActorTemplate(ctx context.Context, atespace, templateName string) (*ateapipb.ActorTemplate, error) {
	req := &ateapipb.GetActorTemplateRequest{
		ActorTemplate: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     templateName,
		},
	}
	return c.control.GetActorTemplate(ctx, req)
}

const (
	DefaultGuestCommand    = "/usr/local/bin/ax-task-runner"
	DefaultSnapshotsBucket = "gs://dberkov-gke-dev3/ate-env/"

	// egressTrustBundleDir is where the egress gateway's CA bundle is projected
	// when AX_EGRESS_MITM_TRUST_BUNDLE is set.
	egressTrustBundleDir  = "/run/ate"
	egressTrustBundleFile = egressTrustBundleDir + "/trust-bundle.pem"
	egressTrustBundleName = "egress-mitm.ate.dev"
	// systemRootsBundleName is Substrate's built-in set of public CA roots
	// (Agent Substrate v0.4+).
	systemRootsBundleName = "system-roots.ate.dev"
)

// egressTrustBundleNames are unified into the one projected file, so it holds
// the egress gateway CA and the public roots. Tools that treat their CA
// variable as a replacement for the system store (Python requests, curl's
// CURL_CA_BUNDLE) then still trust origins the gateway does not intercept.
var egressTrustBundleNames = []string{egressTrustBundleName, systemRootsBundleName}

// egressTrustEnv points common TLS stacks at the projected trust bundle.
// SSL_CERT_DIR is deliberately not set: it would replace the image's whole
// trust store, which Agent Substrate's guidance (substrate#2044) advises
// against. SSL_CERT_FILE adds to it for Go and OpenSSL.
var egressTrustEnv = map[string]string{
	"SSL_CERT_FILE":       egressTrustBundleFile,
	"REQUESTS_CA_BUNDLE":  egressTrustBundleFile,
	"CURL_CA_BUNDLE":      egressTrustBundleFile,
	"GIT_SSL_CAINFO":      egressTrustBundleFile,
	"NODE_EXTRA_CA_CERTS": egressTrustBundleFile,
}

// BuildActorTemplate constructs a Substrate ActorTemplate based on the standard ate-env specification.
// An empty scope means SnapshotScopeFull.
func BuildActorTemplate(atespace, name, image string, envMap map[string]string, command []string, snapshotsBucket string, scope SnapshotScope) *ateapipb.ActorTemplate {
	if atespace == "" {
		atespace = "default"
	}
	if image == "" {
		image = v1alpha1.DefaultTaskImage
	}
	if len(command) == 0 {
		command = []string{DefaultGuestCommand}
	}
	if snapshotsBucket == "" {
		if envBucket := os.Getenv("AX_SNAPSHOTS_BUCKET"); envBucket != "" {
			snapshotsBucket = envBucket
		} else {
			snapshotsBucket = DefaultSnapshotsBucket
		}
	}

	// On clusters whose egress gateway intercepts TLS (Substrate's sdsmint
	// mode), tasks must trust the gateway's CA to make any HTTPS request.
	mitmTrust := os.Getenv("AX_EGRESS_MITM_TRUST_BUNDLE") == "true"
	if mitmTrust {
		merged := make(map[string]string, len(envMap)+len(egressTrustEnv))
		for k, v := range egressTrustEnv {
			merged[k] = v
		}
		for k, v := range envMap {
			merged[k] = v // a task's own env wins
		}
		envMap = merged
	}

	var envList []*ateapipb.EnvVar
	for k, v := range envMap {
		envList = append(envList, &ateapipb.EnvVar{
			Name:  k,
			Value: v,
		})
	}

	mounts := []*ateapipb.VolumeMount{{
		Name:      "workspace",
		MountPath: "/workspace",
	}}
	volumes := []*ateapipb.Volume{{
		Name:       "workspace",
		DurableDir: &ateapipb.DurableDirVolumeSource{},
	}}
	if mitmTrust {
		mounts = append(mounts, &ateapipb.VolumeMount{Name: "egress-trust", MountPath: egressTrustBundleDir})
		volumes = append(volumes, &ateapipb.Volume{
			Name: "egress-trust",
			SystemInfo: &ateapipb.SystemInfoVolumeSource{
				DataSources: []*ateapipb.SystemInfoDataSource{{
					TrustBundle: &ateapipb.TrustBundleDataSource{Names: egressTrustBundleNames, Path: "trust-bundle.pem"},
				}},
			},
		})
	}

	return &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{
			Name:     name,
			Atespace: atespace,
		},
		Containers: []*ateapipb.Container{{
			Name:    "guest",
			Image:   image,
			Command: command,
			Env:     envList,
			WakeupProbe: &ateapipb.ContainerWakeupProbe{
				HttpGet: &ateapipb.HTTPGetAction{
					Path: "/readyz",
					Port: 80,
				},
			},
			VolumeMounts: mounts,
		}},
		Volumes: volumes,
		// Agent Substrate v0.4 has one scope for every snapshot of an actor
		// (pause and suspend) and rejects the onPause and onResume fields
		// earlier versions took. See SnapshotScope for the trade-off.
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation: snapshotsBucket,
			OnCommit:        scope.contentScope(),
		},
		SandboxConfig: &ateapipb.SandboxConfig{
			SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
			ConfigName:   "gvisor-default",
		},
	}
}

// EnsureActorTemplateWithImage creates an ActorTemplate using the specified container image and optional environment variables.
// The template's snapshot scope is the client's SnapshotScope. An existing
// template is returned as it is.
func (c *Client) EnsureActorTemplateWithImage(ctx context.Context, baseAtespace, baseTemplate, targetAtespace, targetTemplate, image string, extraEnv ...map[string]string) (*ateapipb.ActorTemplate, error) {
	existing, err := c.GetActorTemplate(ctx, targetAtespace, targetTemplate)
	if err == nil && existing != nil {
		return existing, nil
	}

	envMap := make(map[string]string)
	for _, envs := range extraEnv {
		for k, v := range envs {
			envMap[k] = v
		}
	}

	tmpl := BuildActorTemplate(targetAtespace, targetTemplate, image, envMap, nil, "", c.SnapshotScope())
	req := &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: tmpl,
	}
	created, err := c.control.CreateActorTemplate(ctx, req)
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return nil, fmt.Errorf("creating actor template %s/%s: %w", targetAtespace, targetTemplate, err)
	}
	if created != nil {
		return created, nil
	}
	return c.GetActorTemplate(ctx, targetAtespace, targetTemplate)
}

// BuildEgressPolicy converts a task's egress rules into a Substrate egress
// policy. Every rule becomes an https rule, which the gateway intercepts,
// because credentials can only be injected into traffic it terminates.
// Agent Substrate v0.4 also enforces tls_passthrough rules (v0.3 ignored
// them); AX does not generate any yet.
func BuildEgressPolicy(atespace string, rules []*v1alpha1.EgressRule) *ateapipb.EgressPolicy {
	policy := &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: "default"},
	}
	for _, rule := range rules {
		ports := rule.GetPorts()
		if len(ports) == 0 {
			ports = []int32{443}
		}
		https := &ateapipb.HTTPSRule{
			Hostnames: rule.GetHosts(),
			Ports:     &ateapipb.Ports{Numbers: ports},
		}
		if creds := rule.GetCredentials(); len(creds) > 0 {
			https.Effects = &ateapipb.HttpRuleEffects{}
			for _, cred := range creds {
				https.Effects.ReplaceHeaders = append(https.Effects.ReplaceHeaders, &ateapipb.CredentialHeader{
					Header:        cred.GetHeader(),
					Prefix:        cred.GetPrefix(),
					CredentialUri: cred.GetCredentialUri(),
				})
			}
		}
		policy.Rules = append(policy.Rules, &ateapipb.EgressRule{Https: https})
	}
	return policy
}

// EnsureEgressPolicy creates the actor's egress policy from the task's egress
// rules. Tasks are immutable, so an existing policy is left as it is. A task
// without egress rules gets no policy, which Substrate treats as deny-all.
func (c *Client) EnsureEgressPolicy(ctx context.Context, atespace, actorName string, rules []*v1alpha1.EgressRule) error {
	if len(rules) == 0 {
		return nil
	}
	_, err := c.control.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{
		Actor:        &ateapipb.ObjectRef{Atespace: atespace, Name: actorName},
		EgressPolicy: BuildEgressPolicy(atespace, rules),
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("creating egress policy for actor %s/%s: %w", atespace, actorName, err)
	}
	return nil
}

// EnsureActor creates an Actor in the specified atespace deriving from an ActorTemplate.
func (c *Client) EnsureActor(ctx context.Context, atespace, actorName, templateAtespace, templateName string) (*ateapipb.Actor, error) {
	req := &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: atespace,
				Name:     actorName,
			},
			ActorTemplate: &ateapipb.ObjectRef{
				Atespace: templateAtespace,
				Name:     templateName,
			},
		},
	}
	actor, err := c.control.CreateActor(ctx, req)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			existing, getErr := c.GetActor(ctx, atespace, actorName)
			if getErr == nil && existing != nil {
				state := existing.GetStatus().GetState()
				if state == ateapipb.ActorState_ACTOR_STATE_CRASHED {
					// Revert to the last snapshot so the workspace survives the crash.
					reverted, revertErr := c.control.RevertActor(ctx, &ateapipb.RevertActorRequest{
						Actor: &ateapipb.ObjectRef{
							Atespace: atespace,
							Name:     actorName,
						},
					})
					if revertErr == nil {
						slog.Warn("existing actor is crashed, reverted to its last snapshot", "actor", actorName)
						return reverted.GetActor(), nil
					}
					slog.Warn("existing actor is crashed and could not be reverted, deleting and recreating", "actor", actorName, "error", revertErr)
					if err := c.DeleteActor(ctx, atespace, actorName); err != nil {
						return nil, err
					}
					actor, err = c.control.CreateActor(ctx, req)
					if err == nil {
						return actor, nil
					}
					return nil, fmt.Errorf("recreating actor %s/%s after crash: %w", atespace, actorName, err)
				}
				if state == ateapipb.ActorState_ACTOR_STATE_DELETING {
					// Wait briefly for previous actor deletion to finalize before recreating
					for i := 0; i < 20; i++ {
						select {
						case <-ctx.Done():
							return nil, ctx.Err()
						case <-time.After(500 * time.Millisecond):
						}
						actor, err = c.control.CreateActor(ctx, req)
						if err == nil {
							return actor, nil
						}
						if status.Code(err) != codes.AlreadyExists {
							break
						}
					}
					return nil, fmt.Errorf("actor %s/%s is still deleting; please retry", atespace, actorName)
				}
			}
			return existing, getErr
		}
		return nil, fmt.Errorf("creating actor %s/%s: %w", atespace, actorName, err)
	}
	return actor, nil
}

// ResumeActor resumes the specified actor onto a worker and returns the worker details.
func (c *Client) ResumeActor(ctx context.Context, atespace, actorName string) (*ateapipb.Actor, string, error) {
	req := &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
	}
	resp, err := c.control.ResumeActor(ctx, req)
	if err != nil {
		return nil, "", fmt.Errorf("resuming actor %s/%s: %w", atespace, actorName, err)
	}
	if resp.Actor == nil {
		return nil, "", fmt.Errorf("nil actor returned when resuming %s/%s", atespace, actorName)
	}
	return resp.Actor, WorkerIP(resp.Actor), nil
}

// WorkerIP returns the address of the worker pod hosting the actor, or ""
// when it has none. Agent Substrate v0.4 reports up to one IP per family,
// primary family first; the first non-empty one is used.
func WorkerIP(actor *ateapipb.Actor) string {
	for _, ip := range actor.GetStatus().GetWorkerAssignment().GetWorkerPodIps() {
		if ip != "" {
			return ip
		}
	}
	return ""
}

// SuspendActor suspends the specified actor, triggering state checkpointing.
func (c *Client) SuspendActor(ctx context.Context, atespace, actorName string) error {
	req := &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
	}
	_, err := c.control.SuspendActor(ctx, req)
	if err != nil {
		return fmt.Errorf("suspending actor %s/%s: %w", atespace, actorName, err)
	}
	return nil
}

// GetActor fetches an Actor by name.
func (c *Client) GetActor(ctx context.Context, atespace, actorName string) (*ateapipb.Actor, error) {
	req := &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
	}
	return c.control.GetActor(ctx, req)
}

// actorDeletionPollInterval is the interval between checks when waiting for an actor to be deleted.
var actorDeletionPollInterval = 200 * time.Millisecond

// DeleteActor deletes the specified actor from Substrate and blocks until the actor
// is fully deleted.
func (c *Client) DeleteActor(ctx context.Context, atespace, actorName string) error {
	req := &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
		AnyState: true,
	}
	_, err := c.control.DeleteActor(ctx, req)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("deleting actor %s/%s: %w", atespace, actorName, err)
	}

	getReq := &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
	}

	// Fast path: check if the actor was deleted synchronously.
	_, err = c.control.GetActor(ctx, getReq)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("waiting for actor %s/%s deletion: %w", atespace, actorName, ctx.Err())
		}
	}

	ticker := time.NewTicker(actorDeletionPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for actor %s/%s deletion: %w", atespace, actorName, ctx.Err())
		case <-ticker.C:
			_, err := c.control.GetActor(ctx, getReq)
			if err != nil {
				if status.Code(err) == codes.NotFound {
					return nil
				}
				if ctx.Err() != nil {
					return fmt.Errorf("waiting for actor %s/%s deletion: %w", atespace, actorName, ctx.Err())
				}
			}
		}
	}
}

// ListActorTemplates returns all ActorTemplates in the given atespace.
func (c *Client) ListActorTemplates(ctx context.Context, atespace string) ([]*ateapipb.ActorTemplate, error) {
	resp, err := c.control.ListActorTemplates(ctx, &ateapipb.ListActorTemplatesRequest{Atespace: atespace})
	if err != nil {
		return nil, fmt.Errorf("listing actor templates in %s: %w", atespace, err)
	}
	if resp.GetNextPageToken() != "" {
		slog.Warn("actor template listing was truncated", "atespace", atespace)
	}
	return resp.GetActorTemplates(), nil
}

// DeleteActorTemplate deletes the specified ActorTemplate. A missing template is not an error.
func (c *Client) DeleteActorTemplate(ctx context.Context, atespace, templateName string) error {
	req := &ateapipb.DeleteActorTemplateRequest{
		ActorTemplate: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     templateName,
		},
	}
	_, err := c.control.DeleteActorTemplate(ctx, req)
	if err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("deleting actor template %s/%s: %w", atespace, templateName, err)
	}
	return nil
}
