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
)

// ClusterUpdate contains information from a received CDS response, which is of
// interest to the registered CDS watcher.
type ClusterUpdate struct {
	// ServiceName is the service name corresponding to the clusterName which
	// is being watched for through CDS.
	ServiceName string
	// EnableLRS indicates whether or not load should be reported through LRS.
	EnableLRS bool
}

type cdsCallbackFunc func(ClusterUpdate, error)

// WatchCluster uses CDS to discover information about the provided
// clusterName.
//
// WatchCluster can be called multiple times, with same or different
// clusterNames. Each call will start an independent watcher for the resource.
//
// Note that during race, there's a small window where the callback can be
// called after the watcher is canceled. The caller needs to handle this case.
func (c *Client) WatchCluster(clusterName string, cb cdsCallbackFunc) (cancel func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	wi := &watchInfo{
		typeURL:     cdsURL,
		target:      clusterName,
		cdsCallback: cb,
	}

	wi.expiryTimer = time.AfterFunc(defaultWatchExpiryTimeout, func() {
		c.scheduleCallback(wi, ClusterUpdate{}, fmt.Errorf("xds: CDS target %s not found, watcher timeout", clusterName))
	})
	return c.watch(wi)
}
