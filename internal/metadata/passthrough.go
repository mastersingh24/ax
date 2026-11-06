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
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// newPassThrough returns a reverse proxy to the task's server on a local port.
// Responses are flushed as they arrive so server-sent events and long-running
// requests stream, and connection upgrades (WebSockets) are forwarded.
//
// The request reaches the task's server with Host set to the loopback address
// it was sent to. The inbound Host is the router's name, which means nothing
// to a server on 127.0.0.1, and a loopback server that guards against DNS
// rebinding rightly refuses it (core-agent answers 421). The inbound host is
// kept in X-Forwarded-Host for servers that build absolute URLs.
func newPassThrough(port int) http.Handler {
	target := &url.URL{Scheme: "http", Host: fmt.Sprintf("127.0.0.1:%d", port)}
	proxy := httputil.NewSingleHostReverseProxy(target)
	director := proxy.Director
	proxy.Director = func(req *http.Request) {
		inbound := req.Host
		director(req)
		if inbound != "" {
			if prior := req.Header.Get("X-Forwarded-Host"); prior != "" {
				req.Header.Set("X-Forwarded-Host", prior+", "+inbound)
			} else {
				req.Header.Set("X-Forwarded-Host", inbound)
			}
		}
		req.Host = target.Host
	}
	proxy.FlushInterval = -1
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		slog.Warn("task server unavailable", "port", port, "path", r.URL.Path, "error", err)
		http.Error(w, fmt.Sprintf("task server on port %d is not available", port), http.StatusBadGateway)
	}
	return proxy
}
