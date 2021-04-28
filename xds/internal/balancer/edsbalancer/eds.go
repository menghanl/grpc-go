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

// Package edsbalancer contains EDS balancer implementation.
package edsbalancer

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/internal"
	"google.golang.org/grpc/internal/buffer"
	"google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/internal/grpcsync"
	"google.golang.org/grpc/internal/pretty"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"google.golang.org/grpc/xds/internal/balancer/priority"
	xdsclient "google.golang.org/grpc/xds/internal/client"
	"google.golang.org/grpc/xds/internal/client/load"
)

const edsName = "eds_experimental"

var (
	errBalancerClosed = errors.New("cdsBalancer is closed")
	newXDSClient      = func() (xdsClientInterface, error) { return xdsclient.New() }
	newChildBalancer  = func(bb balancer.Builder, cc balancer.ClientConn, o balancer.BuildOptions) balancer.Balancer {
		return bb.Build(cc, o)
	}
)

func init() {
	balancer.Register(&edsBalancerBuilder{})
}

type edsBalancerBuilder struct{}

// Build helps implement the balancer.Builder interface.
func (edsBalancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
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

	b := &edsBalancer{
		bOpts:       opts,
		updateCh:    buffer.NewUnbounded(),
		closed:      grpcsync.NewEvent(),
		cancelWatch: func() {}, // No-op at this point.

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

	b.ccw = &ccWrapper{
		ClientConn: cc,
		parent:     b,
	}
	go b.run()
	return b
}

func (edsBalancerBuilder) Name() string {
	return edsName
}

func (edsBalancerBuilder) ParseConfig(c json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
	var cfg EDSConfig
	if err := json.Unmarshal(c, &cfg); err != nil {
		return nil, fmt.Errorf("unable to unmarshal balancer config %s into EDSConfig, error: %v", string(c), err)
	}
	return &cfg, nil
}

// xdsClientInterface contains only the xds_client methods needed by EDS
// balancer. It's defined so we can override xdsclient.New function in tests.
type xdsClientInterface interface {
	WatchEndpoints(clusterName string, edsCb func(xdsclient.EndpointsUpdate, error)) (cancel func())
	ReportLoad(server string) (loadStore *load.Store, cancel func())
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

// watchUpdate wraps the information received from a registered EDS watcher. A
// non-nil error is propagated to the underlying child balancer. A valid update
// results in creating a new child balancer (priority balancer, if one doesn't
// already exist) and pushing the updated balancer config to it.
type watchUpdate struct {
	eds xdsclient.EndpointsUpdate
	err error
}

// edsBalancer manages xdsClient and the actual EDS balancer implementation that
// does load balancing.
//
// It currently has only an edsBalancer. Later, we may add fallback.
type edsBalancer struct {
	ccw       *ccWrapper            // ClientConn interface passed to child LB.
	bOpts     balancer.BuildOptions // BuildOptions passed to child LB.
	updateCh  *buffer.Unbounded     // Channel for gRPC and xdsClient updates.
	xdsClient xdsClientInterface    // xDS client to watch EDS resource.
	logger    *grpclog.PrefixLogger
	closed    *grpcsync.Event

	priorityBuilder      balancer.Builder
	priorityConfigParser balancer.ConfigParser

	config      *EDSConfig
	edsToWatch  string
	configRaw   *serviceconfig.ParseResult
	cancelWatch func() // EDS watch cancel func.

	child           balancer.Balancer
	edsResp         xdsclient.EndpointsUpdate
	edsRespReceived bool

	clusterNameMu sync.Mutex
	clusterName   string
}

// handleClientConnUpdate handles a ClientConnUpdate received from gRPC. Good
// updates lead to registration of an EDS watch. Updates with error lead to
// cancellation of existing watch and propagation of the same error to the
// child balancer.
func (eb *edsBalancer) handleClientConnUpdate(update *ccUpdate) {
	// We first handle errors, if any, and then proceed with handling the
	// update, only if the status quo has changed.
	if err := update.err; err != nil {
		eb.handleErrorFromUpdate(err, true)
	}

	eb.logger.Infof("Receive update from resolver, balancer config: %+v", update.state.BalancerConfig)
	cfg, _ := update.state.BalancerConfig.(*EDSConfig)
	if cfg == nil {
		eb.logger.Warningf("xds: unexpected LoadBalancingConfig type: %T", update.state.BalancerConfig)
		// service config parsing failed. should never happen.
		return
	}

	if err := eb.handleServiceConfigUpdate(cfg, update.state.ResolverState.ServiceConfig); err != nil {
		eb.logger.Warningf("failed to update xDS client: %v", err)
	}
}

// handleServiceConfigUpdate applies the service config update, watching a new
// EDS service name and restarting LRS stream, as required.
func (eb *edsBalancer) handleServiceConfigUpdate(config *EDSConfig, raw *serviceconfig.ParseResult) error {
	// Update the cached cluster name, which will be attached to the address
	// attributes when creating SubConns. Needs to hold the mutex because
	// NewSubConn and UpdateSubConn are not synchronized.
	eb.clusterNameMu.Lock()
	eb.clusterName = config.ClusterName
	eb.clusterNameMu.Unlock()

	eb.config = config
	eb.configRaw = raw

	// If EDSServiceName is set, use it to watch EDS. Otherwise, use the cluster
	// name.
	newEDSToWatch := config.EDSServiceName
	if newEDSToWatch == "" {
		newEDSToWatch = config.ClusterName
	}
	var restartEDSWatch bool
	if eb.edsToWatch != newEDSToWatch {
		restartEDSWatch = true
		eb.edsToWatch = newEDSToWatch
	}

	// Restart EDS watch when the eds name has changed.
	if restartEDSWatch {
		eb.startEndpointsWatch()
	}

	if !eb.edsRespReceived {
		// If eds resp was not received, wait for it.
		return nil
	}
	// If eds resp was received, we need to generate a new balancer config and
	// send it to the child, because certain fields (unrelated to EDS watch)
	// might have changed.
	return eb.updateChildConfig()
}

// updateChildConfig builds a balancer config from eb's cached eds resp and
// service config, and sends that to the child balancer.
//
// If child balancer doesn't already exist, one will be created.
func (eb *edsBalancer) updateChildConfig() error {
	// Child was build when the first EDS resp was received, so we just build
	// the config and addresses.
	if eb.child == nil {
		eb.child = newChildBalancer(eb.priorityBuilder, eb.ccw, eb.bOpts)
	}

	childCfgBytes, addrs := buildPriorityConfigMarshalled(eb.edsResp, eb.config)
	childCfg, err := eb.priorityConfigParser.ParseConfig(childCfgBytes)
	if err != nil {
		eb.logger.Warningf("failed to parse generated priority balancer config, this should never happen because the config is generated: %v", err)
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

// startEndpointsWatch starts the EDS watch.
//
// This usually means load report needs to be restarted, but this function does
// NOT do that. Caller needs to call startLoadReport separately.
func (eb *edsBalancer) startEndpointsWatch() {
	eb.cancelWatch()
	edsToWatch := eb.edsToWatch
	cancelEDSWatch := eb.xdsClient.WatchEndpoints(edsToWatch, func(update xdsclient.EndpointsUpdate, err error) {
		eb.updateCh.Put(&watchUpdate{eds: update, err: err})
	})
	eb.logger.Infof("Watch started on resource name %v with xds-client %p", edsToWatch, eb.xdsClient)
	eb.cancelWatch = func() {
		cancelEDSWatch()
		eb.logger.Infof("Watch cancelled on resource name %v with xds-client %p", edsToWatch, eb.xdsClient)
	}
}

// handleWatchUpdate handles a watch update from the xDS Client. Good updates
// lead to clientConn updates being invoked on the underlying child balancer.
func (eb *edsBalancer) handleWatchUpdate(update *watchUpdate) {
	if err := update.err; err != nil {
		eb.logger.Warningf("Watch error from xds-client %p: %v", eb.xdsClient, err)
		eb.handleErrorFromUpdate(err, false)
		return
	}

	eb.logger.Infof("Watch update from xds-client %p, content: %+v", eb.xdsClient, pretty.ToJSON(update.eds))
	eb.edsRespReceived = true
	eb.edsResp = update.eds

	if err := eb.updateChildConfig(); err != nil {
		eb.logger.Warningf("failed to update child policy's balancer config: %v", err)
	}
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
func (eb *edsBalancer) handleErrorFromUpdate(err error, fromParent bool) {
	eb.logger.Warningf("Received error: %v", err)
	if fromParent && xdsclient.ErrType(err) == xdsclient.ErrorTypeResourceNotFound {
		// This is an error from the parent ClientConn (can be the parent CDS
		// balancer), and is a resource-not-found error. This means the resource
		// (can be either LDS or CDS) was removed. Stop the EDS watch.
		eb.cancelWatch()
		eb.edsToWatch = ""
		eb.cancelWatch = func() {}
	}
	if eb.child != nil {
		eb.child.ResolverError(err)
	} else {
		// If eds balancer was never created, fail the RPCs with
		// errors.
		eb.ccw.UpdateState(balancer.State{
			ConnectivityState: connectivity.TransientFailure,
			Picker:            base.NewErrPicker(err),
		})
	}

}

// run is a long-running goroutine which handles all updates from gRPC and
// xdsClient. All methods which are invoked directly by gRPC or xdsClient simply
// push an update onto a channel which is read and acted upon right here.
func (eb *edsBalancer) run() {
	for {
		select {
		case u := <-eb.updateCh.Get():
			eb.updateCh.Load()
			switch update := u.(type) {
			case *ccUpdate:
				eb.handleClientConnUpdate(update)
			case *scUpdate:
				// SubConn updates are passthrough and are simply handed over to
				// the underlying child balancer.
				if eb.child == nil {
					eb.logger.Errorf("xds: received scUpdate {%+v} with no child balancer", update)
					break
				}
				eb.child.UpdateSubConnState(update.subConn, update.state)
			case *watchUpdate:
				eb.handleWatchUpdate(update)
			}

		// Close results in cancellation of the CDS watch and closing of the
		// underlying edsBalancer and is the only way to exit this goroutine.
		case <-eb.closed.Done():
			eb.cancelWatch()
			eb.edsToWatch = ""
			eb.cancelWatch = func() {}

			if eb.child != nil {
				eb.child.Close()
				eb.child = nil
			}
			eb.xdsClient.Close()
			// This is the *ONLY* point of return from this function.
			eb.logger.Infof("Shutdown")
			return
		}
	}
}

// UpdateClientConnState receives the serviceConfig (which contains the
// clusterName to watch for in CDS) and the xdsClient object from the
// xdsResolver.
func (eb *edsBalancer) UpdateClientConnState(state balancer.ClientConnState) error {
	if eb.closed.HasFired() {
		eb.logger.Warningf("xds: received ClientConnState {%+v} after edsBalancer was closed", state)
		return errBalancerClosed
	}

	eb.updateCh.Put(&ccUpdate{state: state})
	return nil
}

// ResolverError handles errors reported by the xdsResolver.
func (eb *edsBalancer) ResolverError(err error) {
	if eb.closed.HasFired() {
		eb.logger.Warningf("xds: received resolver error {%v} after edsBalancer was closed", err)
		return
	}
	eb.updateCh.Put(&ccUpdate{err: err})
}

// UpdateSubConnState handles subConn updates from gRPC.
func (eb *edsBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	if eb.closed.HasFired() {
		eb.logger.Warningf("xds: received subConn update {%v, %v} after edsBalancer was closed", sc, state)
		return
	}
	eb.updateCh.Put(&scUpdate{subConn: sc, state: state})
}

// Close closes the cdsBalancer and the underlying child balancer.
func (eb *edsBalancer) Close() {
	eb.closed.Fire()
}

// --------------------------------------

func (eb *edsBalancer) getClusterName() string {
	eb.clusterNameMu.Lock()
	defer eb.clusterNameMu.Unlock()
	return eb.config.ClusterName
}

// ccWrapper wraps the balancer.ClientConn passed to the CDS balancer at
// creation and intercepts the NewSubConn() and UpdateAddresses() call from the
// child policy to add security configuration required by xDS credentials.
//
// Other methods of the balancer.ClientConn interface are not overridden and
// hence get the original implementation.
type ccWrapper struct {
	balancer.ClientConn
	parent *edsBalancer
}

// NewSubConn intercepts NewSubConn() calls from the child policy and adds an
// address attribute which provides all information required by the xdsCreds
// handshaker to perform the TLS handshake.
func (ccw *ccWrapper) NewSubConn(addrs []resolver.Address, opts balancer.NewSubConnOptions) (balancer.SubConn, error) {
	clusterName := ccw.parent.getClusterName()
	newAddrs := make([]resolver.Address, len(addrs))
	for i, addr := range addrs {
		newAddrs[i] = internal.SetXDSHandshakeClusterName(addr, clusterName)
	}
	return ccw.ClientConn.NewSubConn(newAddrs, opts)
}

func (ccw *ccWrapper) UpdateAddresses(sc balancer.SubConn, addrs []resolver.Address) {
	clusterName := ccw.parent.getClusterName()
	newAddrs := make([]resolver.Address, len(addrs))
	for i, addr := range addrs {
		newAddrs[i] = internal.SetXDSHandshakeClusterName(addr, clusterName)
	}
	ccw.ClientConn.UpdateAddresses(sc, newAddrs)
}
