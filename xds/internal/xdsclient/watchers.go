/*
 *
 * Copyright 2021 gRPC authors.
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

package xdsclient

import (
	"google.golang.org/grpc/xds/internal/xdsclient/resource"
)

// WatchListener uses LDS to discover information about the provided listener.
//
// Note that during race (e.g. an xDS response is received while the user is
// calling cancel()), there's a small window where the callback can be called
// after the watcher is canceled. The caller needs to handle this case.
func (c *clientImpl) WatchListener(serviceName string, cb func(resource.ListenerUpdate, error)) (cancel func()) {
	first, cancelF := c.pubsub.WatchListener(serviceName, cb)
	if first {
		c.controller.AddWatch(resource.ListenerResource, serviceName)
	}
	return func() {
		if cancelF() {
			c.controller.RemoveWatch(resource.ListenerResource, serviceName)
		}
	}
}

// WatchRouteConfig starts a listener watcher for the service..
//
// Note that during race (e.g. an xDS response is received while the user is
// calling cancel()), there's a small window where the callback can be called
// after the watcher is canceled. The caller needs to handle this case.
func (c *clientImpl) WatchRouteConfig(routeName string, cb func(resource.RouteConfigUpdate, error)) (cancel func()) {
	first, cancelF := c.pubsub.WatchRouteConfig(routeName, cb)
	if first {
		c.controller.AddWatch(resource.RouteConfigResource, routeName)
	}
	return func() {
		if cancelF() {
			c.controller.RemoveWatch(resource.RouteConfigResource, routeName)
		}
	}
}

// WatchCluster uses CDS to discover information about the provided
// clusterName.
//
// WatchCluster can be called multiple times, with same or different
// clusterNames. Each call will start an independent watcher for the resource.
//
// Note that during race (e.g. an xDS response is received while the user is
// calling cancel()), there's a small window where the callback can be called
// after the watcher is canceled. The caller needs to handle this case.
func (c *clientImpl) WatchCluster(clusterName string, cb func(resource.ClusterUpdate, error)) (cancel func()) {
	first, cancelF := c.pubsub.WatchCluster(clusterName, cb)
	if first {
		c.controller.AddWatch(resource.ClusterResource, clusterName)
	}
	return func() {
		if cancelF() {
			c.controller.RemoveWatch(resource.ClusterResource, clusterName)
		}
	}
}

// WatchEndpoints uses EDS to discover endpoints in the provided clusterName.
//
// WatchEndpoints can be called multiple times, with same or different
// clusterNames. Each call will start an independent watcher for the resource.
//
// Note that during race (e.g. an xDS response is received while the user is
// calling cancel()), there's a small window where the callback can be called
// after the watcher is canceled. The caller needs to handle this case.
func (c *clientImpl) WatchEndpoints(clusterName string, cb func(resource.EndpointsUpdate, error)) (cancel func()) {
	first, cancelF := c.pubsub.WatchEndpoints(clusterName, cb)
	if first {
		c.controller.AddWatch(resource.EndpointsResource, clusterName)
	}
	return func() {
		if cancelF() {
			c.controller.RemoveWatch(resource.EndpointsResource, clusterName)
		}
	}
}
