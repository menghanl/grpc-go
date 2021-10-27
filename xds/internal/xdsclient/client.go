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
 *
 */

// Package xdsclient implements a full fledged gRPC client for the xDS API used
// by the xds resolver and balancer implementations.
package xdsclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	v2corepb "github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	v3corepb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/internal/backoff"
	"google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/internal/grpcsync"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/xds/internal/version"
	"google.golang.org/grpc/xds/internal/xdsclient/bootstrap"
	"google.golang.org/grpc/xds/internal/xdsclient/load"
	"google.golang.org/grpc/xds/internal/xdsclient/pubsub"
	"google.golang.org/grpc/xds/internal/xdsclient/resource"
)

var (
	m = make(map[version.TransportAPI]APIClientBuilder)
)

// RegisterAPIClientBuilder registers a client builder for xDS transport protocol
// version specified by b.Version().
//
// NOTE: this function must only be called during initialization time (i.e. in
// an init() function), and is not thread-safe. If multiple builders are
// registered for the same version, the one registered last will take effect.
func RegisterAPIClientBuilder(b APIClientBuilder) {
	m[b.Version()] = b
}

// getAPIClientBuilder returns the client builder registered for the provided
// xDS transport API version.
func getAPIClientBuilder(version version.TransportAPI) APIClientBuilder {
	if b, ok := m[version]; ok {
		return b
	}
	return nil
}

// BuildOptions contains options to be passed to client builders.
type BuildOptions struct {
	// Parent is a top-level xDS client which has the intelligence to take
	// appropriate action based on xDS responses received from the management
	// server.
	Parent UpdateHandler
	// Validator performs post unmarshal validation checks.
	Validator resource.UpdateValidatorFunc
	// NodeProto contains the Node proto to be used in xDS requests. The actual
	// type depends on the transport protocol version used.
	NodeProto proto.Message
	// Backoff returns the amount of time to backoff before retrying broken
	// streams.
	Backoff func(int) time.Duration
	// Logger provides enhanced logging capabilities.
	Logger *grpclog.PrefixLogger
}

// APIClientBuilder creates an xDS client for a specific xDS transport protocol
// version.
type APIClientBuilder interface {
	// Build builds a transport protocol specific implementation of the xDS
	// client based on the provided clientConn to the management server and the
	// provided options.
	Build(*grpc.ClientConn, BuildOptions) (APIClient, error)
	// Version returns the xDS transport protocol version used by clients build
	// using this builder.
	Version() version.TransportAPI
}

// APIClient represents the functionality provided by transport protocol
// version specific implementations of the xDS client.
//
// TODO: unexport this interface and all the methods after the PR to make
// xdsClient sharable by clients. AddWatch and RemoveWatch are exported for
// v2/v3 to override because they need to keep track of LDS name for RDS to use.
// After the share xdsClient change, that's no longer necessary. After that, we
// will still keep this interface for testing purposes.
type APIClient interface {
	// AddWatch adds a watch for an xDS resource given its type and name.
	AddWatch(resource.ResourceType, string)

	// RemoveWatch cancels an already registered watch for an xDS resource
	// given its type and name.
	RemoveWatch(resource.ResourceType, string)

	// reportLoad starts an LRS stream to periodically report load using the
	// provided ClientConn, which represent a connection to the management
	// server.
	reportLoad(ctx context.Context, cc *grpc.ClientConn, opts loadReportingOptions)

	// Close cleans up resources allocated by the API client.
	Close()
}

// loadReportingOptions contains configuration knobs for reporting load data.
type loadReportingOptions struct {
	loadStore *load.Store
}

// UpdateHandler receives and processes (by taking appropriate actions) xDS
// resource updates from an APIClient for a specific version.
type UpdateHandler interface {
	// NewListeners handles updates to xDS listener resources.
	NewListeners(map[string]resource.ListenerUpdateErrTuple, resource.UpdateMetadata)
	// NewRouteConfigs handles updates to xDS RouteConfiguration resources.
	NewRouteConfigs(map[string]resource.RouteConfigUpdateErrTuple, resource.UpdateMetadata)
	// NewClusters handles updates to xDS Cluster resources.
	NewClusters(map[string]resource.ClusterUpdateErrTuple, resource.UpdateMetadata)
	// NewEndpoints handles updates to xDS ClusterLoadAssignment (or tersely
	// referred to as Endpoints) resources.
	NewEndpoints(map[string]resource.EndpointsUpdateErrTuple, resource.UpdateMetadata)
	// NewConnectionError handles connection errors from the xDS stream. The
	// error will be reported to all the resource watchers.
	NewConnectionError(err error)
}

// Function to be overridden in tests.
var newAPIClient = func(apiVersion version.TransportAPI, cc *grpc.ClientConn, opts BuildOptions) (APIClient, error) {
	cb := getAPIClientBuilder(apiVersion)
	if cb == nil {
		return nil, fmt.Errorf("no client builder for xDS API version: %v", apiVersion)
	}
	return cb.Build(cc, opts)
}

// clientImpl is the real implementation of the xds client. The exported Client
// is a wrapper of this struct with a ref count.
//
// Implements UpdateHandler interface.
// TODO(easwars): Make a wrapper struct which implements this interface in the
// style of ccBalancerWrapper so that the Client type does not implement these
// exported methods.
type clientImpl struct {
	done      *grpcsync.Event
	config    *bootstrap.Config
	cc        *grpc.ClientConn // Connection to the management server.
	apiClient APIClient

	logger *grpclog.PrefixLogger
	pubsub *pubsub.Pubsub

	// Changes to map lrsClients and the lrsClient inside the map need to be
	// protected by lrsMu.
	lrsMu      sync.Mutex
	lrsClients map[string]*lrsClient
}

// newWithConfig returns a new xdsClient with the given config.
func newWithConfig(config *bootstrap.Config, watchExpiryTimeout time.Duration) (_ *clientImpl, retErr error) {
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

	c := &clientImpl{
		done:       grpcsync.NewEvent(),
		config:     config,
		lrsClients: make(map[string]*lrsClient),
	}

	defer func() {
		if retErr != nil {
			if c.cc != nil {
				c.cc.Close()
			}
			if c.pubsub != nil {
				c.pubsub.Close()
			}
			if c.apiClient != nil {
				c.apiClient.Close()
			}
		}
	}()

	cc, err := grpc.Dial(config.BalancerName, dopts...)
	if err != nil {
		// An error from a non-blocking dial indicates something serious.
		return nil, fmt.Errorf("xds: failed to dial balancer {%s}: %v", config.BalancerName, err)
	}
	c.cc = cc
	c.logger = prefixLogger(c)
	c.logger.Infof("Created ClientConn to xDS management server: %s", config.BalancerName)

	c.pubsub = pubsub.New(watchExpiryTimeout, c.logger)

	apiClient, err := newAPIClient(config.TransportAPI, cc, BuildOptions{
		Parent:    c,
		Validator: c.updateValidator,
		NodeProto: config.NodeProto,
		Backoff:   backoff.DefaultExponential.Backoff,
		Logger:    c.logger,
	})
	if err != nil {
		return nil, err
	}
	c.apiClient = apiClient
	c.logger.Infof("Created")
	return c, nil
}

// BootstrapConfig returns the configuration read from the bootstrap file.
// Callers must treat the return value as read-only.
func (c *clientRefCounted) BootstrapConfig() *bootstrap.Config {
	return c.config
}

// Close closes the gRPC connection to the management server.
func (c *clientImpl) Close() {
	if c.done.HasFired() {
		return
	}
	c.done.Fire()
	// TODO: Should we invoke the registered callbacks here with an error that
	// the client is closed?
	c.apiClient.Close()
	c.cc.Close()
	c.pubsub.Close()
	c.logger.Infof("Shutdown")
}

func (c *clientImpl) filterChainUpdateValidator(fc *resource.FilterChain) error {
	if fc == nil {
		return nil
	}
	return c.securityConfigUpdateValidator(fc.SecurityCfg)
}

func (c *clientImpl) securityConfigUpdateValidator(sc *resource.SecurityConfig) error {
	if sc == nil {
		return nil
	}
	if sc.IdentityInstanceName != "" {
		if _, ok := c.config.CertProviderConfigs[sc.IdentityInstanceName]; !ok {
			return fmt.Errorf("identitiy certificate provider instance name %q missing in bootstrap configuration", sc.IdentityInstanceName)
		}
	}
	if sc.RootInstanceName != "" {
		if _, ok := c.config.CertProviderConfigs[sc.RootInstanceName]; !ok {
			return fmt.Errorf("root certificate provider instance name %q missing in bootstrap configuration", sc.RootInstanceName)
		}
	}
	return nil
}

func (c *clientImpl) updateValidator(u interface{}) error {
	switch update := u.(type) {
	case resource.ListenerUpdate:
		if update.InboundListenerCfg == nil || update.InboundListenerCfg.FilterChains == nil {
			return nil
		}
		return update.InboundListenerCfg.FilterChains.Validate(c.filterChainUpdateValidator)
	case resource.ClusterUpdate:
		return c.securityConfigUpdateValidator(update.SecurityCfg)
	default:
		// We currently invoke this update validation function only for LDS and
		// CDS updates. In the future, if we wish to invoke it for other xDS
		// updates, corresponding plumbing needs to be added to those unmarshal
		// functions.
	}
	return nil
}
