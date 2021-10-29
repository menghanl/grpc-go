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

// Package controller contains implementation to connect to the control plane.
// Including starting the ClientConn, starting the xDS stream, and
// sending/receiving messages.
//
// All the messages are parsed by the resource package (e.g.
// UnmarshalListener()) and sent to the Pubsub watchers.
package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/internal/backoff"
	"google.golang.org/grpc/internal/buffer"
	"google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/xds/internal/xdsclient/bootstrap"
	"google.golang.org/grpc/xds/internal/xdsclient/controller/version"
	"google.golang.org/grpc/xds/internal/xdsclient/pubsub"
	"google.golang.org/grpc/xds/internal/xdsclient/xdsresource"
)

// Controller manages the connection and stream to the control plane.
type Controller struct {
	config *bootstrap.Config
	logger *grpclog.PrefixLogger

	cc              *grpc.ClientConn // Connection to the management server.
	vClient         version.VersionedClient
	updateValidator xdsresource.UpdateValidatorFunc
	updateHandler   pubsub.UpdateHandler

	stopRunGoroutine context.CancelFunc

	backoff  func(int) time.Duration
	streamCh chan grpc.ClientStream
	sendCh   *buffer.Unbounded

	// FIXME: re-order the fields.

	mu sync.Mutex
	// Message specific watch infos, protected by the above mutex. These are
	// written to, after successfully reading from the update channel, and are
	// read from when recovering from a broken stream to resend the xDS
	// messages. When the user of this client object cancels a watch call,
	// these are set to nil. All accesses to the map protected and any value
	// inside the map should be protected with the above mutex.
	watchMap map[xdsresource.ResourceType]map[string]bool
	// versionMap contains the version that was acked (the version in the ack
	// request that was sent on wire). The key is rType, the value is the
	// version string, becaues the versions for different resource types should
	// be independent.
	versionMap map[xdsresource.ResourceType]string
	// nonceMap contains the nonce from the most recent received response.
	nonceMap map[xdsresource.ResourceType]string

	// Changes to map lrsClients and the lrsClient inside the map need to be
	// protected by lrsMu.
	lrsMu      sync.Mutex
	lrsClients map[string]*lrsClient
}

// New creates a new controller.
func New(config *bootstrap.Config, updateHandler pubsub.UpdateHandler, validator xdsresource.UpdateValidatorFunc, logger *grpclog.PrefixLogger) (_ *Controller, retErr error) {
	switch {
	case config.BalancerName == "":
		return nil, errors.New("xds: no xds_server name provided in options")
	case config.Creds == nil:
		return nil, errors.New("xds: no credentials provided in options")
	case config.NodeProto == nil:
		return nil, errors.New("xds: no node_proto provided in options")
	}

	dopts := []grpc.DialOption{
		config.Creds,
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    5 * time.Minute,
			Timeout: 20 * time.Second,
		}),
	}

	ret := &Controller{
		config: config,

		backoff:    backoff.DefaultExponential.Backoff,
		streamCh:   make(chan grpc.ClientStream, 1),
		sendCh:     buffer.NewUnbounded(),
		watchMap:   make(map[xdsresource.ResourceType]map[string]bool),
		versionMap: make(map[xdsresource.ResourceType]string),
		nonceMap:   make(map[xdsresource.ResourceType]string),

		updateValidator: validator,
		updateHandler:   updateHandler,

		lrsClients: make(map[string]*lrsClient),
	}

	defer func() {
		if retErr != nil {
			if ret.cc != nil {
				ret.cc.Close()
			}
		}
	}()

	cc, err := grpc.Dial(config.BalancerName, dopts...)
	if err != nil {
		// An error from a non-blocking dial indicates something serious.
		return nil, fmt.Errorf("xds: failed to dial control plane {%s}: %v", config.BalancerName, err)
	}
	ret.cc = cc

	builder := version.GetAPIClientBuilder(config.TransportAPI)
	if builder == nil {
		return nil, fmt.Errorf("no client builder for xDS API version: %v", config.TransportAPI)
	}
	apiClient, err := builder(version.BuildOptions{NodeProto: config.NodeProto, Logger: logger})
	if err != nil {
		return nil, err
	}
	ret.vClient = apiClient

	ctx, cancel := context.WithCancel(context.Background())
	ret.stopRunGoroutine = cancel
	go ret.run(ctx)

	return ret, nil
}

// Close closes the controller.
func (t *Controller) Close() {
	t.stopRunGoroutine()
	t.cc.Close()
}
