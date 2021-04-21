/*
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

package edsbalancer

import (
	"sync"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/internal"
	"google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"google.golang.org/grpc/xds/internal/balancer/priority"
	xdsclient "google.golang.org/grpc/xds/internal/client"
)

// TODO: make this a environment variable?
var defaultPriorityInitTimeout = 10 * time.Second

// edsBalancerImpl does load balancing based on the EDS responses. Note that it
// doesn't implement the balancer interface. It's intended to be used by a high
// level balancer implementation.
//
// The localities are picked as weighted round robin. A configurable child
// policy is used to manage endpoints in each locality.
type edsBalancerImpl struct {
	cc                   balancer.ClientConn
	buildOpts            balancer.BuildOptions
	logger               *grpclog.PrefixLogger
	priorityBuilder      balancer.Builder
	priorityConfigParser balancer.ConfigParser

	enqueueChildBalancerStateUpdate func(balancer.State)

	mu               sync.Mutex
	config           *EDSConfig
	serviceConfigRaw *serviceconfig.ParseResult
	edsRespReceived  bool
	edsResp          xdsclient.EndpointsUpdate
	child            balancer.Balancer
}

// newEDSBalancerImpl create a new edsBalancerImpl.
func newEDSBalancerImpl(cc balancer.ClientConn, bOpts balancer.BuildOptions, enqueueState func(balancer.State), logger *grpclog.PrefixLogger) *edsBalancerImpl {
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

	edsImpl := &edsBalancerImpl{
		cc:                   cc,
		buildOpts:            bOpts,
		logger:               logger,
		priorityBuilder:      priorityBuilder,
		priorityConfigParser: priorityConfigParser,

		enqueueChildBalancerStateUpdate: enqueueState,
	}
	// Don't start sub-balancer here. Start it when handling the first EDS
	// response.
	return edsImpl
}

func (edsImpl *edsBalancerImpl) handleServiceConfig(cfg *EDSConfig, raw *serviceconfig.ParseResult) {
	edsImpl.mu.Lock()
	defer edsImpl.mu.Unlock()
	edsImpl.config = cfg
	edsImpl.serviceConfigRaw = raw

	if !edsImpl.edsRespReceived {
		return
	}

	// Child was build when the first EDS resp was received, so we just build
	// the config and addresses.

	childCfgBytes, addrs := buildPriorityConfigMarshalled(edsImpl.edsResp, edsImpl.config)
	childCfg, err := edsImpl.priorityConfigParser.ParseConfig(childCfgBytes)
	if err != nil {
		edsImpl.logger.Warningf("failed to parse generated priority balancer config, this should never happen because the config is generated: %v", err)
	}
	// FIXME: handle error
	_ = edsImpl.child.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: resolver.State{
			Addresses:     addrs,
			ServiceConfig: edsImpl.serviceConfigRaw,
		},
		BalancerConfig: childCfg,
	})
}

func (edsImpl *edsBalancerImpl) getClusterName() string {
	edsImpl.mu.Lock()
	defer edsImpl.mu.Unlock()
	return edsImpl.config.ClusterName
}

// handleEDSResponse handles the EDS response and creates/deletes localities and
// SubConns. It also handles drops.
//
// HandleChildPolicy and HandleEDSResponse must be called by the same goroutine.
func (edsImpl *edsBalancerImpl) handleEDSResponse(edsResp xdsclient.EndpointsUpdate) {
	edsImpl.mu.Lock()
	defer edsImpl.mu.Unlock()

	if !edsImpl.edsRespReceived {
		edsImpl.edsRespReceived = true
		edsImpl.child = edsImpl.priorityBuilder.Build(edsImpl.cc, edsImpl.buildOpts)
	}
	edsImpl.edsResp = edsResp

	childCfgBytes, addrs := buildPriorityConfigMarshalled(edsImpl.edsResp, edsImpl.config)
	childCfg, err := edsImpl.priorityConfigParser.ParseConfig(childCfgBytes)
	if err != nil {
		edsImpl.logger.Warningf("failed to parse generated priority balancer config, this should never happen because the config is generated: %v", err)
		return // FIXME: handle the error here.
	}
	// FIXME: handle error
	_ = edsImpl.child.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: resolver.State{
			Addresses:     addrs,
			ServiceConfig: edsImpl.serviceConfigRaw,
		},
		BalancerConfig: childCfg,
	})
}

// handleSubConnStateChange handles the state change and update pickers accordingly.
func (edsImpl *edsBalancerImpl) handleSubConnStateChange(sc balancer.SubConn, s balancer.SubConnState) {
	// If there's an subconn update, it's guaranteed child was built. No need to
	// check for nil.
	edsImpl.child.UpdateSubConnState(sc, s)
}

func (edsImpl *edsBalancerImpl) close() {
	edsImpl.mu.Lock()
	defer edsImpl.mu.Unlock()
	if edsImpl.child != nil {
		edsImpl.child.Close()
	}
}

// edsBalancerWrapperCC implements the balancer.ClientConn API and get passed to
// each balancer group. It contains the locality priority.
type edsBalancerWrapperCC struct {
	balancer.ClientConn
	parent *edsBalancerImpl
}

func (ebwcc *edsBalancerWrapperCC) NewSubConn(addrs []resolver.Address, opts balancer.NewSubConnOptions) (balancer.SubConn, error) {
	clusterName := ebwcc.parent.getClusterName()
	newAddrs := make([]resolver.Address, len(addrs))
	for i, addr := range addrs {
		newAddrs[i] = internal.SetXDSHandshakeClusterName(addr, clusterName)
	}
	return ebwcc.ClientConn.NewSubConn(newAddrs, opts)
}

func (ebwcc *edsBalancerWrapperCC) UpdateAddresses(sc balancer.SubConn, addrs []resolver.Address) {
	clusterName := ebwcc.parent.getClusterName()
	newAddrs := make([]resolver.Address, len(addrs))
	for i, addr := range addrs {
		newAddrs[i] = internal.SetXDSHandshakeClusterName(addr, clusterName)
	}
	ebwcc.ClientConn.UpdateAddresses(sc, newAddrs)
}
