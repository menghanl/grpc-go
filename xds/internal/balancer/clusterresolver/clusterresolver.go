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

// Package clusterresolver contains EDS balancer implementation.
package clusterresolver

import (
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/internal/buffer"
	"google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/internal/grpcsync"
	"google.golang.org/grpc/internal/pretty"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"google.golang.org/grpc/xds/internal/balancer/clusterresolver/balancerconfigbuilder"
	"google.golang.org/grpc/xds/internal/balancer/priority"
	xdsclient "google.golang.org/grpc/xds/internal/client"
)

// Name is the name of the cluster_resolver balancer.
const Name = "xds_cluster_resolver_experimental"

var (
	errBalancerClosed = errors.New("cdsBalancer is closed")
	newXDSClient      = func() (xdsClientInterface, error) { return xdsclient.New() }
	newChildBalancer  = func(bb balancer.Builder, cc balancer.ClientConn, o balancer.BuildOptions) balancer.Balancer {
		return bb.Build(cc, o)
	}
)

func init() {
	balancer.Register(&clusterResolverBalancerBuilder{})
}

type clusterResolverBalancerBuilder struct{}

// Build helps implement the balancer.Builder interface.
func (clusterResolverBalancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
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

	b := &clusterResolverBalancer{
		bOpts:    opts,
		updateCh: buffer.NewUnbounded(),
		closed:   grpcsync.NewEvent(),
		done:     grpcsync.NewEvent(),

		priorityBuilder:      priorityBuilder,
		priorityConfigParser: priorityConfigParser,
	}
	b.logger = prefixLogger(b)
	b.logger.Infof("Created")

	client, err := newXDSClient()
	if err != nil {
		b.logger.Errorf("xds: failed to create xds-client: %v", err)
		return nil
	}
	b.xdsClient = client
	b.resourceWatcher = newResourceResolver(client, b.logger)
	b.cc = &ccWrapper{
		ClientConn:      cc,
		resourceWatcher: b.resourceWatcher,
	}

	go b.run()
	return b
}

func (clusterResolverBalancerBuilder) Name() string {
	return Name
}

func (clusterResolverBalancerBuilder) ParseConfig(c json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
	var cfg LBConfig
	if err := json.Unmarshal(c, &cfg); err != nil {
		return nil, fmt.Errorf("unable to unmarshal balancer config %s into cluster-resolver config, error: %v", string(c), err)
	}
	return &cfg, nil
}

// xdsClientInterface contains only the xds_client methods needed by EDS
// balancer. It's defined so we can override xdsclient.New function in tests.
type xdsClientInterface interface {
	WatchEndpoints(clusterName string, edsCb func(xdsclient.EndpointsUpdate, error)) (cancel func())
	Close()
}

// ccUpdate wraps a clientConn update received from gRPC (pushed from the
// xdsResolver).
type ccUpdate struct {
	state balancer.ClientConnState
	err   error
}

// scUpdate wraps a subConn update received from gRPC. This is directly passed
// on to the child balancer.
type scUpdate struct {
	subConn balancer.SubConn
	state   balancer.SubConnState
}

// clusterResolverBalancer manages xdsClient and the actual EDS balancer implementation that
// does load balancing.
//
// It currently has only an clusterResolverBalancer. Later, we may add fallback.
type clusterResolverBalancer struct {
	cc              *ccWrapper            // ClientConn interface passed to child LB.
	bOpts           balancer.BuildOptions // BuildOptions passed to child LB.
	updateCh        *buffer.Unbounded     // Channel for gRPC and xdsClient updates.
	xdsClient       xdsClientInterface    // xDS client to watch EDS resource.
	resourceWatcher *resourceResolver     // Watcher to watch EDS and DNS.
	logger          *grpclog.PrefixLogger
	closed          *grpcsync.Event
	done            *grpcsync.Event

	priorityBuilder      balancer.Builder
	priorityConfigParser balancer.ConfigParser

	config    *LBConfig
	configRaw *serviceconfig.ParseResult

	child               balancer.Balancer
	priorities          []balancerconfigbuilder.PriorityConfig
	watchUpdateReceived bool
}

// handleClientConnUpdate handles a ClientConnUpdate received from gRPC. Good
// updates lead to registration of EDS and DNS watches. Updates with error lead
// to cancellation of existing watch and propagation of the same error to the
// child balancer.
func (eb *clusterResolverBalancer) handleClientConnUpdate(update *ccUpdate) {
	// We first handle errors, if any, and then proceed with handling the
	// update, only if the status quo has changed.
	if err := update.err; err != nil {
		eb.handleErrorFromUpdate(err, true)
		return
	}

	eb.logger.Infof("Receive update from resolver, balancer config: %v", pretty.ToJSON(update.state.BalancerConfig))
	cfg, _ := update.state.BalancerConfig.(*LBConfig)
	if cfg == nil {
		eb.logger.Warningf("xds: unexpected LoadBalancingConfig type: %T", update.state.BalancerConfig)
		// service config parsing failed. should never happen.
		return
	}

	eb.config = cfg
	eb.configRaw = update.state.ResolverState.ServiceConfig
	eb.resourceWatcher.updateMechanisms(cfg.DiscoveryMechanisms)

	if !eb.watchUpdateReceived {
		// If update was not received, wait for it.
		return
	}
	// If eds resp was received before this, the child policy was created. We
	// need to generate a new balancer config and send it to the child, because
	// certain fields (unrelated to EDS watch) might have changed.
	if err := eb.updateChildConfig(); err != nil {
		eb.logger.Warningf("failed to update child policy config: %v", err)
	}
}

// handleWatchUpdate handles a watch update from the xDS Client. Good updates
// lead to clientConn updates being invoked on the underlying child balancer.
func (eb *clusterResolverBalancer) handleWatchUpdate(update *resourceUpdate) {
	if err := update.err; err != nil {
		eb.logger.Warningf("Watch error from xds-client %p: %v", eb.xdsClient, err)
		eb.handleErrorFromUpdate(err, false)
		return
	}

	eb.logger.Infof("resource update: %+v", pretty.ToJSON(update.p))
	eb.watchUpdateReceived = true
	eb.priorities = update.p

	// A new EDS update triggers new child configs (e.g. different priorities
	// for the priority balancer), and new addresses (the endpoints come from
	// the EDS response).
	if err := eb.updateChildConfig(); err != nil {
		eb.logger.Warningf("failed to update child policy's balancer config: %v", err)
	}
}

// updateChildConfig builds a balancer config from eb's cached eds resp and
// service config, and sends that to the child balancer. Note that it also
// generates the addresses, because the endpoints come from the EDS resp.
//
// If child balancer doesn't already exist, one will be created.
func (eb *clusterResolverBalancer) updateChildConfig() error {
	// Child was build when the first EDS resp was received, so we just build
	// the config and addresses.
	if eb.child == nil {
		eb.child = newChildBalancer(eb.priorityBuilder, eb.cc, eb.bOpts)
	}

	childCfgBytes, addrs := balancerconfigbuilder.BuildPriorityConfigMarshalled(eb.priorities, eb.config.EndpointPickingPolicy)
	childCfg, err := eb.priorityConfigParser.ParseConfig(childCfgBytes)
	if err != nil {
		err = fmt.Errorf("failed to parse generated priority balancer config, this should never happen because the config is generated: %v", err)
		eb.logger.Warningf("%v", err)
		return err
	}
	eb.logger.Infof("build balancer config: %v", pretty.ToJSON(childCfg))
	return eb.child.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: resolver.State{
			Addresses:     addrs,
			ServiceConfig: eb.configRaw,
		},
		BalancerConfig: childCfg,
	})
}

// handleErrorFromUpdate handles both the error from parent ClientConn (from CDS
// balancer) and the error from xds client (from the watcher). fromParent is
// true if error is from parent ClientConn.
//
// If the error is connection error, it should be handled for fallback purposes.
//
// If the error is resource-not-found:
// - If it's from CDS balancer (shows as a resolver error), it means LDS or CDS
// resources were removed. The EDS watch should be canceled.
// - If it's from xds client, it means EDS resource were removed. The EDS
// watcher should keep watching.
// In both cases, the sub-balancers will be receive the error.
func (eb *clusterResolverBalancer) handleErrorFromUpdate(err error, fromParent bool) {
	eb.logger.Warningf("Received error: %v", err)
	if fromParent && xdsclient.ErrType(err) == xdsclient.ErrorTypeResourceNotFound {
		// This is an error from the parent ClientConn (can be the parent CDS
		// balancer), and is a resource-not-found error. This means the resource
		// (can be either LDS or CDS) was removed. Stop the EDS watch.
		eb.resourceWatcher.stop()
	}
	if eb.child != nil {
		eb.child.ResolverError(err)
	} else {
		// If eds balancer was never created, fail the RPCs with errors.
		eb.cc.UpdateState(balancer.State{
			ConnectivityState: connectivity.TransientFailure,
			Picker:            base.NewErrPicker(err),
		})
	}

}

// run is a long-running goroutine which handles all updates from gRPC and
// xdsClient. All methods which are invoked directly by gRPC or xdsClient simply
// push an update onto a channel which is read and acted upon right here.
func (eb *clusterResolverBalancer) run() {
	for {
		select {
		case u := <-eb.updateCh.Get():
			eb.updateCh.Load()
			switch update := u.(type) {
			case *ccUpdate:
				eb.handleClientConnUpdate(update)
			case *scUpdate:
				// SubConn updates are simply handed over to the underlying
				// child balancer.
				if eb.child == nil {
					eb.logger.Errorf("xds: received scUpdate {%+v} with no child balancer", update)
					break
				}
				eb.child.UpdateSubConnState(update.subConn, update.state)
			}
		case u := <-eb.resourceWatcher.updateChannel:
			eb.handleWatchUpdate(u)

		// Close results in cancellation of the CDS watch and closing of the
		// underlying clusterResolverBalancer and is the only way to exit this goroutine.
		case <-eb.closed.Done():
			eb.resourceWatcher.stop()

			if eb.child != nil {
				eb.child.Close()
				eb.child = nil
			}
			eb.xdsClient.Close()
			// This is the *ONLY* point of return from this function.
			eb.logger.Infof("Shutdown")
			eb.done.Fire()
			return
		}
	}
}

// Following are methods to implement the balancer interface.

// UpdateClientConnState receives the serviceConfig (which contains the
// clusterName to watch for in CDS) and the xdsClient object from the
// xdsResolver.
func (eb *clusterResolverBalancer) UpdateClientConnState(state balancer.ClientConnState) error {
	if eb.closed.HasFired() {
		eb.logger.Warningf("xds: received ClientConnState {%+v} after clusterResolverBalancer was closed", state)
		return errBalancerClosed
	}

	eb.updateCh.Put(&ccUpdate{state: state})
	return nil
}

// ResolverError handles errors reported by the xdsResolver.
func (eb *clusterResolverBalancer) ResolverError(err error) {
	if eb.closed.HasFired() {
		eb.logger.Warningf("xds: received resolver error {%v} after clusterResolverBalancer was closed", err)
		return
	}
	eb.updateCh.Put(&ccUpdate{err: err})
}

// UpdateSubConnState handles subConn updates from gRPC.
func (eb *clusterResolverBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	if eb.closed.HasFired() {
		eb.logger.Warningf("xds: received subConn update {%v, %v} after clusterResolverBalancer was closed", sc, state)
		return
	}
	eb.updateCh.Put(&scUpdate{subConn: sc, state: state})
}

// Close closes the cdsBalancer and the underlying child balancer.
func (eb *clusterResolverBalancer) Close() {
	eb.closed.Fire()
	<-eb.done.Done()
}

// ccWrapper overrides ResolveNow(), so that re-resolution from the child
// policies will trigger the DNS resolver in cluster_resolver balancer.
type ccWrapper struct {
	balancer.ClientConn
	resourceWatcher *resourceResolver
}

func (c *ccWrapper) ResolveNow(resolver.ResolveNowOptions) {
	c.resourceWatcher.resolveNow()
}
