/*
 *
 * Copyright 2019 gRPC authors.
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

package client

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/xds/internal/client/load"
	load2 "google.golang.org/grpc/xds/internal/client/load"
)

// NodeMetadataHostnameKey is the metadata key for specifying the target name in
// the node proto of an LRS request.
const NodeMetadataHostnameKey = "PROXYLESS_CLIENT_HOSTNAME"

// lrsClient maps to one lrsServer. It contains:
// - a ClientConn to this server (only if it's different from the xds server)
// - a load.Store that contains loads only for this server
type lrsClient struct {
	refCount int
	cancel   func()
	cc       *grpc.ClientConn // nil if the server is same as the xds server
	load     *load2.Store
}

// ReportLoad starts an load reporting stream to the given server. If the server
// is not an empty string, and is different from the xds server, a new
// ClientConn will be created.
//
// The same options used for creating the Client will be used (including
// NodeProto, and dial options if necessary).
//
// It returns a Store for the user to report loads, a function to cancel the
// load reporting stream.
func (c *Client) ReportLoad(server string) (*load.Store, func()) {
	c.lrsMu.Lock()
	defer c.lrsMu.Unlock()

	// If there's already a client to this server, use it.
	if c, ok := c.lrsClients[server]; ok {
		c.refCount++
		return c.load, c.cancel
	}

	// First reporting stream to this server.
	var (
		cc    *grpc.ClientConn
		newCC bool
	)
	c.logger.Infof("Starting load report to server: %s", server)
	if server == "" || server == c.opts.Config.BalancerName {
		cc = c.cc
	} else {
		c.logger.Infof("LRS server is different from xDS server, starting a new ClientConn")
		dopts := append([]grpc.DialOption{c.opts.Config.Creds}, c.opts.DialOpts...)
		ccNew, err := grpc.Dial(server, dopts...)
		if err != nil {
			// An error from a non-blocking dial indicates something serious.
			c.logger.Infof("xds: failed to dial load report server {%s}: %v", server, err)
			return nil, func() {}
		}
		cc = ccNew
		newCC = true
	}

	store := load.NewStore()
	ctx, cancel := context.WithCancel(context.Background())
	go c.apiClient.ReportLoad(ctx, c.cc, LoadReportingOptions{
		load: store,
	})

	lrsC := &lrsClient{
		refCount: 1,
		cancel:   cancel,
		load:     store,
	}
	if newCC {
		lrsC.cc = cc
	}
	c.lrsClients[server] = lrsC

	return store, func() {
		c.lrsMu.Lock()
		defer c.lrsMu.Unlock()
		lrsC.cancel()
		lrsC.refCount--
		if lrsC.refCount == 0 && lrsC.cc != nil {
			lrsC.cc.Close()
		}
	}
}
