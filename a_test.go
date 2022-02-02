/*
 *
 * Copyright 2022 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package grpc_test

import (
	"testing"

	"cloud.google.com/go/compute/metadata"
	"google.golang.org/grpc/internal/leakcheck"
	// _ "google.golang.org/grpc/balancer"
	// _ "google.golang.org/grpc/balancer/roundrobin"
	// _ "google.golang.org/grpc/codes"
	// _ "google.golang.org/grpc/internal"
	// _ "google.golang.org/grpc/internal/balancer/stub"
	// _ "google.golang.org/grpc/internal/grpctest"
	// _ "google.golang.org/grpc/internal/transport"
	// _ "google.golang.org/grpc/resolver"
	// _ "google.golang.org/grpc/resolver/manual"
	// _ "google.golang.org/grpc/serviceconfig"
	// _ "google.golang.org/grpc/status"
)

func TestML(t *testing.T) {
	defer leakcheck.Check(t)

	if metadata.OnGCE() {
		t.Logf("on gce")
	}
}
