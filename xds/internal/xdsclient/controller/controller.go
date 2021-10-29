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
	"runtime/debug"
	"sync"
	"time"

	v2corepb "github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	v3corepb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/internal/backoff"
	"google.golang.org/grpc/internal/buffer"
	"google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/xds/internal/version"
	"google.golang.org/grpc/xds/internal/xdsclient/bootstrap"
	"google.golang.org/grpc/xds/internal/xdsclient/resource"
)

// Function to be overridden in tests.
var newAPIClient = func(apiVersion version.TransportAPI, cc *grpc.ClientConn, opts BuildOptions) (VersionedClient, error) {
	cb := getAPIClientBuilder(apiVersion)
	if cb == nil {
		debug.PrintStack()
		return nil, fmt.Errorf("no client builder for xDS API version: %v", apiVersion)
	}
	return cb.Build(opts)
}

// Controller manages the connection and stream to the control plane.
type Controller struct {
	config *bootstrap.Config
	logger *grpclog.PrefixLogger

	cc      *grpc.ClientConn // Connection to the management server.
	vClient VersionedClient

	cancelCtx context.CancelFunc

	backoff  func(int) time.Duration
	streamCh chan grpc.ClientStream
	sendCh   *buffer.Unbounded

	mu sync.Mutex
	// Message specific watch infos, protected by the above mutex. These are
	// written to, after successfully reading from the update channel, and are
	// read from when recovering from a broken stream to resend the xDS
	// messages. When the user of this client object cancels a watch call,
	// these are set to nil. All accesses to the map protected and any value
	// inside the map should be protected with the above mutex.
	watchMap map[resource.Type]map[string]bool
	// versionMap contains the version that was acked (the version in the ack
	// request that was sent on wire). The key is rType, the value is the
	// version string, becaues the versions for different resource types should
	// be independent.
	versionMap map[resource.Type]string
	// nonceMap contains the nonce from the most recent received response.
	nonceMap map[resource.Type]string

	// Changes to map lrsClients and the lrsClient inside the map need to be
	// protected by lrsMu.
	lrsMu      sync.Mutex
	lrsClients map[string]*lrsClient
}

// FIXME: is this really necessary???
var grpcDial = grpc.Dial

// New creates a new controller.
func New(config *bootstrap.Config, pubsub UpdateHandler, validator resource.UpdateValidatorFunc, logger *grpclog.PrefixLogger) (*Controller, error) {
	switch {
	case config.BalancerName == "":
		return nil, errors.New("xds: no xds_server name provided in options")
	case config.Creds == nil:
		return nil, errors.New("xds: no credentials provided in options")
	case config.NodeProto == nil:
		return nil, errors.New("xds: no node_proto provided in options")
	}

	switch config.TransportAPI {
	case version.TransportV2:
		if _, ok := config.NodeProto.(*v2corepb.Node); !ok {
			return nil, fmt.Errorf("xds: Node proto type (%T) does not match API version: %v", config.NodeProto, config.TransportAPI)
		}
	case version.TransportV3:
		if _, ok := config.NodeProto.(*v3corepb.Node); !ok {
			return nil, fmt.Errorf("xds: Node proto type (%T) does not match API version: %v", config.NodeProto, config.TransportAPI)
		}
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
		watchMap:   make(map[resource.Type]map[string]bool),
		versionMap: make(map[resource.Type]string),
		nonceMap:   make(map[resource.Type]string),

		lrsClients: make(map[string]*lrsClient),
	}

	cc, err := grpcDial(config.BalancerName, dopts...)
	if err != nil {
		// An error from a non-blocking dial indicates something serious.
		return nil, fmt.Errorf("xds: failed to dial control plane {%s}: %v", config.BalancerName, err)
	}
	ret.cc = cc
	// FIXME: defer close on error

	apiClient, err := newAPIClient(config.TransportAPI, cc, BuildOptions{
		NodeProto: config.NodeProto,
		Logger:    logger,

		Pubsub:          pubsub, // FIXME: the name.
		UpdateValidator: validator,
	})
	if err != nil {
		return nil, err
	}
	ret.vClient = apiClient
	// FIXME: defer close on error

	ctx, cancel := context.WithCancel(context.Background())
	ret.cancelCtx = cancel

	go ret.run(ctx)

	return ret, nil
}

// Close closes the controller.
func (t *Controller) Close() {
	t.cancelCtx()
	// c.vClient.Close()
	t.cc.Close()
}
