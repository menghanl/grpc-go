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
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"google.golang.org/grpc/xds/internal/balancer/lrs"
	xdsclient "google.golang.org/grpc/xds/internal/client"
)

const (
	edsName = "eds_experimental"
)

var (
	newEDSBalancer = func(cc balancer.ClientConn, loadStore lrs.Store) edsBalancerImplInterface {
		return newEDSBalancerImpl(cc, loadStore)
	}
)

func init() {
	balancer.Register(&edsBalancerBuilder{})
}

type edsBalancerBuilder struct{}

// Build helps implement the balancer.Builder interface.
func (b *edsBalancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	ctx, cancel := context.WithCancel(context.Background())
	x := &edsBalancer{
		ctx:             ctx,
		cancel:          cancel,
		cc:              cc,
		buildOpts:       opts,
		grpcUpdate:      make(chan interface{}),
		xdsClientUpdate: make(chan *edsUpdate),
	}
	loadStore := lrs.NewStore()
	x.edsImpl = newEDSBalancer(x.cc, loadStore)
	x.client = newXDSClientWrapper(x.handleEDSUpdate, x.buildOpts, loadStore)
	go x.run()
	return x
}

func (b *edsBalancerBuilder) Name() string {
	return edsName
}

func (b *edsBalancerBuilder) ParseConfig(c json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
	var cfg EDSConfig
	if err := json.Unmarshal(c, &cfg); err != nil {
		return nil, fmt.Errorf("unable to unmarshal balancer config %s into EDSConfig, error: %v", string(c), err)
	}
	return &cfg, nil
}

// edsBalancerImplInterface defines the interface that edsBalancerImpl must
// implement to communicate with edsBalancer.
//
// It's implemented by the real eds balancer and a fake testing eds balancer.
type edsBalancerImplInterface interface {
	// HandleEDSResponse passes the received EDS message from traffic director to eds balancer.
	HandleEDSResponse(edsResp *xdsclient.EDSUpdate)
	// HandleChildPolicy updates the eds balancer the intra-cluster load balancing policy to use.
	HandleChildPolicy(name string, config json.RawMessage)
	// HandleSubConnStateChange handles state change for SubConn.
	HandleSubConnStateChange(sc balancer.SubConn, state connectivity.State)
	// Close closes the eds balancer.
	Close()
}

var _ balancer.V2Balancer = (*edsBalancer)(nil) // Assert that we implement V2Balancer

// edsBalancer manages xdsClient and the actual EDS balancer implementation that
// does load balancing.
//
// It currently has only an edsBalancer. Later, we may add fallback.
type edsBalancer struct {
	cc        balancer.ClientConn // *xdsClientConn
	buildOpts balancer.BuildOptions
	ctx       context.Context
	cancel    context.CancelFunc

	// edsBalancer continuously monitor the channels below, and will handle events from them in sync.
	grpcUpdate      chan interface{}
	xdsClientUpdate chan *edsUpdate

	client  *xdsclientWrapper // may change when passed a different service config
	config  *EDSConfig        // may change when passed a different service config
	edsImpl edsBalancerImplInterface
}

// run gets executed in a goroutine once edsBalancer is created. It monitors updates from grpc,
// xdsClient and load balancer. It synchronizes the operations that happen inside edsBalancer. It
// exits when edsBalancer is closed.
func (x *edsBalancer) run() {
	for {
		select {
		case update := <-x.grpcUpdate:
			x.handleGRPCUpdate(update)
		case update := <-x.xdsClientUpdate:
			x.handleXDSClientUpdate(update)
		case <-x.ctx.Done():
			if x.client != nil {
				x.client.close()
				x.client = nil
			}
			x.edsImpl.Close()
			return
		}
	}
}

// handleErrorFromUpdate handles both the error from ClientComm (from CDS
// balancer) and the error from xds client (from the watcher).
//
// If the error is connection error, it should be handled for fallback purposes.
//
// If the error is resource-not-found:
// - If it's from CDS balancer, it means CDS resources were removed. The EDS
// watch should be canceled (and is already canceled by the caller of this
// function).
// - If it's from xds client, it means EDS resource were removed. The EDS
// watcher should keep watching.
// In both cases, the sub-balancers will be closed, and the future picks will
// fail.
func (x *edsBalancer) handleErrorFromUpdate(err error) {
	// TODO: Need to distinguish between connection errors and resource removed
	// errors. For the former, we will need to handle it later on for fallback.
	//
	// For the latter, closing sub-balancers and fail the pickers.

	// If it's a resource-not-found error
	// - it could come from CDS balancer, so the CDS resources were removed
	// - it could come from xds client, so the EDS resources were removed
	// In both cases, the sub-balancers should be closed, and the picks should
	// all fail.
	if xdsclient.TypeOfError(err) == xdsclient.ErrorTypeResourceNotFound {
		// FIXME: does an empty update closes all sub-balancers?
		//
		// FIXME: what picker do we get after closing all sub-balancers? It
		// should update picker with an error picker. Needs tests to verify.
		x.edsImpl.HandleEDSResponse(&xdsclient.EDSUpdate{})
	}

	// FIXME: do we need to manually update the picker to an error picker with
	// the err passed in? If we do, we need to make sure the picker won't be
	// replaced by a later picker update.

	// if b.edsLB != nil {
	// 	b.edsLB.ResolverError(err)
	// } else {
	// 	// If eds balancer was never created, fail the RPCs with
	// 	// errors.
	// 	b.cc.UpdateState(balancer.State{
	// 		ConnectivityState: connectivity.TransientFailure,
	// 		Picker:            base.NewErrPickerV2(err),
	// 	})
	// }

	panic("www")
}

func (x *edsBalancer) handleGRPCUpdate(update interface{}) {
	switch u := update.(type) {
	case *subConnStateUpdate:
		x.edsImpl.HandleSubConnStateChange(u.sc, u.state.ConnectivityState)
	case *balancer.ClientConnState:
		cfg, _ := u.BalancerConfig.(*EDSConfig)
		if cfg == nil {
			// service config parsing failed. should never happen.
			return
		}

		x.client.handleUpdate(cfg, u.ResolverState.Attributes)

		if x.config == nil {
			x.config = cfg
			return
		}

		// We will update the edsImpl with the new child policy, if we got a
		// different one.
		if !reflect.DeepEqual(cfg.ChildPolicy, x.config.ChildPolicy) {
			if cfg.ChildPolicy != nil {
				x.edsImpl.HandleChildPolicy(cfg.ChildPolicy.Name, cfg.ChildPolicy.Config)
			} else {
				x.edsImpl.HandleChildPolicy(roundrobin.Name, nil)
			}
		}

		x.config = cfg
	case error:
		// This is an error from the resolver (can be the parent CDS balancer).
		if xdsclient.TypeOfError(u) == xdsclient.ErrorTypeResourceNotFound {
			// A resource-not-found error, means the CDS resource was removed.
			// Stop the EDS watch.
			x.client.cancelWatch()
		}
		x.handleErrorFromUpdate(u)
	default:
		// unreachable path
		panic("wrong update type")
	}
}

func (x *edsBalancer) handleXDSClientUpdate(update *edsUpdate) {
	if err := update.err; err != nil {
		x.handleErrorFromUpdate(err)
		return
	}
	x.edsImpl.HandleEDSResponse(update.resp)
}

type subConnStateUpdate struct {
	sc    balancer.SubConn
	state balancer.SubConnState
}

func (x *edsBalancer) HandleSubConnStateChange(sc balancer.SubConn, state connectivity.State) {
	grpclog.Error("UpdateSubConnState should be called instead of HandleSubConnStateChange")
}

func (x *edsBalancer) HandleResolvedAddrs(addrs []resolver.Address, err error) {
	grpclog.Error("UpdateResolverState should be called instead of HandleResolvedAddrs")
}

func (x *edsBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	update := &subConnStateUpdate{
		sc:    sc,
		state: state,
	}
	select {
	case x.grpcUpdate <- update:
	case <-x.ctx.Done():
	}
}

func (x *edsBalancer) ResolverError(err error) {
	select {
	case x.grpcUpdate <- err:
	case <-x.ctx.Done():
	}
}

func (x *edsBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	select {
	case x.grpcUpdate <- &s:
	case <-x.ctx.Done():
	}
	return nil
}

type edsUpdate struct {
	resp *xdsclient.EDSUpdate
	err  error
}

func (x *edsBalancer) handleEDSUpdate(resp *xdsclient.EDSUpdate, err error) error {
	// TODO: this function should take (resp, error), and send them together on
	// the channel. There doesn't need to be a separate `loseContact` function.
	select {
	case x.xdsClientUpdate <- &edsUpdate{resp: resp, err: err}:
	case <-x.ctx.Done():
	}

	return nil
}

func (x *edsBalancer) Close() {
	x.cancel()
}
