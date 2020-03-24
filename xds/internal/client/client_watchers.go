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
	"time"
)

// The value chosen here is based on the default value of the
// initial_fetch_timeout field in corepb.ConfigSource proto.
var defaultWatchExpiryTimeout = 15 * time.Second

const (
	ldsURL = "type.googleapis.com/envoy.api.v2.Listener"
	rdsURL = "type.googleapis.com/envoy.api.v2.RouteConfiguration"
	cdsURL = "type.googleapis.com/envoy.api.v2.Cluster"
	edsURL = "type.googleapis.com/envoy.api.v2.ClusterLoadAssignment"
)

// watchInfo holds all the information about a watch call.
type watchInfo struct {
	typeURL string
	target  string

	ldsCallback ldsCallbackFunc
	rdsCallback rdsCallbackFunc
	cdsCallback cdsCallbackFunc
	edsCallback edsCallbackFunc
	expiryTimer *time.Timer
}

func (c *Client) watch(wi *watchInfo) (cancel func()) {
	// If this is a new watcher, will ask lower level to send a new request with
	// the resource name. If this type+name is already being watched, will do
	// nothing.
	var watchers map[string]*watchInfoSet
	switch wi.typeURL {
	case ldsURL:
		watchers = c.ldsWatchers
	case rdsURL:
		watchers = c.rdsWatchers
	case cdsURL:
		watchers = c.cdsWatchers
	case edsURL:
		watchers = c.edsWatchers
	}

	resourceName := wi.target
	s, ok := watchers[wi.target]
	if !ok {
		s = newWatchInfoSet()
		watchers[resourceName] = s
		c.v2c.addWatch(wi.typeURL, resourceName)
	}
	s.add(wi)

	// If this clusterName is in cache, it will call the callback with the
	// value.
	switch wi.typeURL {
	case ldsURL:
		if v, ok := c.ldsCache[resourceName]; ok {
			c.scheduleCallback(wi, v, nil)
		}
	case rdsURL:
		if v, ok := c.rdsCache[resourceName]; ok {
			c.scheduleCallback(wi, v, nil)
		}
	case cdsURL:
		if v, ok := c.cdsCache[resourceName]; ok {
			c.scheduleCallback(wi, v, nil)
		}
	case edsURL:
		if v, ok := c.edsCache[resourceName]; ok {
			c.scheduleCallback(wi, v, nil)
		}
	}

	return func() {
		// Remove this watcher from cdsWatchers.
		c.mu.Lock()
		defer c.mu.Unlock()
		if s := watchers[resourceName]; s != nil {
			wi.expiryTimer.Stop()
			s.remove(wi)
			if s.len() == 0 {
				delete(watchers, resourceName)
				c.v2c.removeWatch(wi.typeURL, resourceName)
				// TODO: remove item from cache.
			}
		}
	}
}
