/*
 *
 * Copyright 2020 gRPC authors.
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
 *
 */

package client

import (
	"fmt"
	"time"

	"google.golang.org/grpc/xds/internal"
)

// OverloadDropConfig contains the config to drop overloads.
type OverloadDropConfig struct {
	Category    string
	Numerator   uint32
	Denominator uint32
}

// EndpointHealthStatus represents the health status of an endpoint.
type EndpointHealthStatus int32

const (
	// EndpointHealthStatusUnknown represents HealthStatus UNKNOWN.
	EndpointHealthStatusUnknown EndpointHealthStatus = iota
	// EndpointHealthStatusHealthy represents HealthStatus HEALTHY.
	EndpointHealthStatusHealthy
	// EndpointHealthStatusUnhealthy represents HealthStatus UNHEALTHY.
	EndpointHealthStatusUnhealthy
	// EndpointHealthStatusDraining represents HealthStatus DRAINING.
	EndpointHealthStatusDraining
	// EndpointHealthStatusTimeout represents HealthStatus TIMEOUT.
	EndpointHealthStatusTimeout
	// EndpointHealthStatusDegraded represents HealthStatus DEGRADED.
	EndpointHealthStatusDegraded
)

// Endpoint contains information of an endpoint.
type Endpoint struct {
	Address      string
	HealthStatus EndpointHealthStatus
	Weight       uint32
}

// Locality contains information of a locality.
type Locality struct {
	Endpoints []Endpoint
	ID        internal.Locality
	Priority  uint32
	Weight    uint32
}

// EndpointsUpdate contains an EDS update.
type EndpointsUpdate struct {
	Drops      []OverloadDropConfig
	Localities []Locality
}

type edsCallbackFunc = func(EndpointsUpdate, error)

// WatchEndpoints uses EDS to discover endpoints in the provided clusterName.
//
// WatchEndpoints can be called multiple times, with same or different
// clusterNames. Each call will start an independent watcher for the resource.
//
// Note that during race, there's a small window where the callback can be
// called after the watcher is canceled. The caller needs to handle this case.
func (c *Client) WatchEndpoints(clusterName string, cb edsCallbackFunc) (cancel func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	wi := &watchInfo{
		typeURL:     edsURL,
		target:      clusterName,
		edsCallback: cb,
	}

	wi.expiryTimer = time.AfterFunc(defaultWatchExpiryTimeout, func() {
		c.scheduleCallback(wi, EndpointsUpdate{}, fmt.Errorf("xds: EDS target %s not found, watcher timeout", clusterName))
	})
	return c.watch(wi)
}
