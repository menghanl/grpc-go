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

// Package clusterresolver contains clusterresolver balancer implementation.
package clusterresolver

import (
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/internal/grpcsync"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"google.golang.org/grpc/xds/internal/balancer/clusterresolver/balancerconfigbuilder"
	"google.golang.org/grpc/xds/internal/balancer/priority"
	xdsclient "google.golang.org/grpc/xds/internal/client"
	"google.golang.org/grpc/xds/internal/client/load"
)

const clusterResolverName = "xds_cluster_resolver_experimental"

// xdsClientInterface contains only the xds_client methods needed by EDS
// balancer. It's defined so we can override xdsclient.New function in tests.
type xdsClientInterface interface {
	WatchEndpoints(clusterName string, edsCb func(xdsclient.EndpointsUpdate, error)) (cancel func())
	ReportLoad(server string) (loadStore *load.Store, cancel func())
	Close()
}

var (
	newXDSClient = func() (xdsClientInterface, error) { return xdsclient.New() }
)

func init() {
	balancer.Register(&clusterResolverBalancerBuilder{})
}

type clusterResolverBalancerBuilder struct{}

// Build helps implement the balancer.Builder interface.
func (b *clusterResolverBalancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	priorityBuilder := balancer.Get(priority.Name)
	if priorityBuilder == nil {
		logger.Errorf("priority balancer is needed but not registered")
		return nil
	}
	priorityConfigParser, ok := priorityBuilder.(balancer.ConfigParser)
	if !ok {
		logger.Errorf("priority balancer builder is not a config parser")
		return nil
	}

	crb := &clusterResolverBalancer{
		cc:         cc,
		closed:     grpcsync.NewEvent(),
		grpcUpdate: make(chan interface{}),
		// xdsClientUpdate:   make(chan *edsUpdate),
		// childPolicyUpdate: buffer.NewUnbounded(),
		child:                priorityBuilder.Build(cc, opts),
		priorityConfigParser: priorityConfigParser,
	}
	crb.logger = prefixLogger(crb)

	client, err := newXDSClient()
	if err != nil {
		crb.logger.Errorf("xds: failed to create xds-client: %v", err)
		return nil
	}

	crb.xdsClient = client
	crb.resolver = newResourceResolver(client)

	// crb.edsImpl = newEDSBalancer(crb.cc, opts, crb.enqueueChildBalancerState, crb.logger)
	crb.logger.Infof("Created")
	go crb.run()
	return crb
}

func (b *clusterResolverBalancerBuilder) Name() string {
	return clusterResolverName
}

func (b *clusterResolverBalancerBuilder) ParseConfig(c json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
	var cfg LBConfig
	if err := json.Unmarshal(c, &cfg); err != nil {
		return nil, fmt.Errorf("unable to unmarshal balancer config %s into clusterResolver balancer config, error: %v", string(c), err)
	}
	return &cfg, nil
}

// clusterResolverBalancer manages xdsClient and the actual EDS balancer implementation that
// does load balancing.
//
// It currently has only an clusterResolverBalancer. Later, we may add fallback.
type clusterResolverBalancer struct {
	cc     balancer.ClientConn
	closed *grpcsync.Event
	logger *grpclog.PrefixLogger

	// clusterResolverBalancer continuously monitors the channels below, and
	// will handle events from them in sync.
	grpcUpdate chan interface{}
	// xdsClientUpdate   chan *edsUpdate
	// childPolicyUpdate *buffer.Unbounded

	resolver *resourceResolver

	xdsClient            xdsClientInterface
	child                balancer.Balancer
	priorityConfigParser balancer.ConfigParser
}

// run gets executed in a goroutine once clusterResolverBalancer is created. It monitors
// updates from grpc, xdsClient and load balancer. It synchronizes the
// operations that happen inside clusterResolverBalancer. It exits when clusterResolverBalancer is
// closed.
func (crb *clusterResolverBalancer) run() {
	for {
		select {
		case update := <-crb.grpcUpdate:
			crb.handleGRPCUpdate(update)
		case priorities := <-crb.resolver.updateCh:
			cfgJSON, addrs := balancerconfigbuilder.BuildPriorityConfigMarshalled(priorities, nil)
			cfg, err := crb.priorityConfigParser.ParseConfig(cfgJSON)
			if err != nil {
				crb.logger.Warningf("failed to parse generated priority balancer config, this should never happen because the config is generated: %v", err)
			}
			crb.child.UpdateClientConnState(balancer.ClientConnState{
				ResolverState: resolver.State{
					Addresses: addrs,
					// ServiceConfig: nil,
					// Attributes:    nil,
				},
				BalancerConfig: cfg,
			})
		// case update := <-crb.xdsClientUpdate:
		// 	crb.handleXDSClientUpdate(update)
		// case update := <-crb.childPolicyUpdate.Get():
		// 	crb.childPolicyUpdate.Load()
		// 	u := update.(balancer.State)
		// 	crb.cc.UpdateState(u)
		case <-crb.closed.Done():
			// crb.xdsClient.Close()
			// crb.child.Close()
			return
		}
	}
}

/*
// handleErrorFromUpdate handles both the error from parent ClientConn (from CDS
// balancer) and the error from xds client (from the resolver). fromParent is
// true if error is from parent ClientConn.
//
// If the error is connection error, it should be handled for fallback purposes.
//
// If the error is resource-not-found:
// - If it's from CDS balancer (shows as a resolver error), it means LDS or CDS
// resources were removed. The EDS watch should be canceled.
// - If it's from xds client, it means EDS resource were removed. The EDS
// resolver should keep watching.
// In both cases, the sub-balancers will be closed, and the future picks will
// fail.
func (x *clusterResolverBalancer) handleErrorFromUpdate(err error, fromParent bool) {
	x.logger.Warningf("Received error: %v", err)
	// if xdsclient.ErrType(err) == xdsclient.ErrorTypeResourceNotFound {
	// 	if fromParent {
	// 		// This is an error from the parent ClientConn (can be the parent
	// 		// CDS balancer), and is a resource-not-found error. This means the
	// 		// resource (can be either LDS or CDS) was removed. Stop the EDS
	// 		// watch.
	// 		// x.cancelWatch()
	// 	}
	// 	// FIXME: resource was removed, need to delete addresses for one of the children.
	// 	// This can just be handled by regenerating the child config.
	// 	// x.child.UpdateClientConnState(regenerateConfig())
	// }
}
*/

func (crb *clusterResolverBalancer) handleGRPCUpdate(update interface{}) {
	switch u := update.(type) {
	case *subConnStateUpdate:
		crb.child.UpdateSubConnState(u.sc, u.state)
	case *balancer.ClientConnState:
		crb.logger.Infof("Receive update from resolver, balancer config: %+v", u.BalancerConfig)
		// FIXME: this should be LBConfig
		cfg, _ := u.BalancerConfig.(*LBConfig)
		if cfg == nil {
			// service config parsing failed. should never happen.
			return
		}
		if err := crb.handleServiceConfigUpdate(cfg, u.ResolverState.ServiceConfig); err != nil {
			crb.logger.Warningf("failed to update xDS client: %v", err)
		}
	case error:
		crb.child.ResolverError(u)
	default:
		// unreachable path
		crb.logger.Errorf("wrong update type: %T", update)
	}
}

// handleServiceConfigUpdate applies the service config update, watching a new
// EDS service name and restarting LRS stream, as required.
func (crb *clusterResolverBalancer) handleServiceConfigUpdate(config *LBConfig, _ *serviceconfig.ParseResult) error {
	crb.resolver.updateMechanisms(config.DiscoveryMechanisms)
	return nil
}

/*
func (x *clusterResolverBalancer) handleXDSClientUpdate(update *edsUpdate) {
	if err := update.err; err != nil {
		x.handleErrorFromUpdate(err, false)
		return
	}
	// x.child.UpdateClientConnState(regenerateConfig())
}
*/

type subConnStateUpdate struct {
	sc    balancer.SubConn
	state balancer.SubConnState
}

func (crb *clusterResolverBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	update := &subConnStateUpdate{
		sc:    sc,
		state: state,
	}
	select {
	case crb.grpcUpdate <- update:
	case <-crb.closed.Done():
	}
}

func (crb *clusterResolverBalancer) ResolverError(err error) {
	select {
	case crb.grpcUpdate <- err:
	case <-crb.closed.Done():
	}
}

func (crb *clusterResolverBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	select {
	case crb.grpcUpdate <- &s:
	case <-crb.closed.Done():
	}
	return nil
}

/*
type edsUpdate struct {
	resp xdsclient.EndpointsUpdate
	err  error
}

func (x *clusterResolverBalancer) handleEDSUpdate(resp xdsclient.EndpointsUpdate, err error) {
	select {
	case x.xdsClientUpdate <- &edsUpdate{resp: resp, err: err}:
	case <-x.closed.Done():
	}
}

// func (x *clusterResolverBalancer) enqueueChildBalancerState(s balancer.State) {
// 	x.childPolicyUpdate.Put(s)
// }

*/

func (crb *clusterResolverBalancer) Close() {
	// crb.closed.Fire()
	// crb.logger.Infof("Shutdown")
}

// equalStringPointers returns true if
// - a and b are both nil OR
// - *a == *b (and a and b are both non-nil)
func equalStringPointers(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
