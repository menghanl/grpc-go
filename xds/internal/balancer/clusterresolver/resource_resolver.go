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
 *
 */

package clusterresolver

import (
	"sync"

	"google.golang.org/grpc/xds/internal/balancer/clusterresolver/balancerconfigbuilder"
	"google.golang.org/grpc/xds/internal/client"
)

type resolverType interface {
	hasUpdate() bool
	resolveNow()
	stop()
}

type resourceResolver struct {
	xdsclient xdsClientInterface

	updateCh chan []balancerconfigbuilder.PriorityConfig

	// mu protects the slice and map, and content of the resolvers in the slice.
	mu           sync.Mutex
	resolvers    []resolverType // resolvers sorted by priority.
	resolversMap map[balancerconfigbuilder.DiscoveryMechanism]resolverType
}

func newResourceResolver(xdsclient xdsClientInterface) *resourceResolver {
	return &resourceResolver{
		xdsclient: xdsclient,
		updateCh:  make(chan []balancerconfigbuilder.PriorityConfig, 1),
	}
}

func (rr *resourceResolver) updateMechanisms(mechanisms []balancerconfigbuilder.DiscoveryMechanism) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	newDMs := make(map[balancerconfigbuilder.DiscoveryMechanism]bool)
	newResolvers := make([]resolverType, len(mechanisms))
	// Start one watch for new discover mechanisms.
	for i, dm := range mechanisms {
		newDMs[dm] = true
		if r, ok := rr.resolversMap[dm]; ok {
			newResolvers[i] = r
			continue
		}
		switch dm.Type {
		case balancerconfigbuilder.DiscoveryMechanismTypeEDS:
			r := newEDSResolver(dm, rr.xdsclient, rr)
			newResolvers[i] = r
			rr.resolversMap[dm] = r
		case balancerconfigbuilder.DiscoveryMechanismTypeLogicalDNS:
			// watchDNS
		}
	}
	for dm, r := range rr.resolversMap {
		if !newDMs[dm] {
			delete(rr.resolversMap, dm)
			r.stop()
		}
	}
	rr.resolvers = newResolvers
	// Regenerate in case priority order changed.
	rr.generate()
}

// caller must hold rr.mu.
func (rr *resourceResolver) generate() {
	var ret []balancerconfigbuilder.PriorityConfig
	for _, r := range rr.resolvers {
		if !r.hasUpdate() {
			// Don't send updates to parent until all resolvers have update to
			// send.
			return
		}
		switch rt := r.(type) {
		case *edsResolver:
			ret = append(ret, balancerconfigbuilder.PriorityConfig{
				Mechanism: rt.mechanism,
				EDSResp:   rt.update,
			})
		case *dnsResolver:
			panic("dns not supported")
		}
	}
	select {
	case <-rr.updateCh:
	default:
	}
	rr.updateCh <- ret
}

type edsResolver struct {
	mechanism balancerconfigbuilder.DiscoveryMechanism
	cancel    func()

	update         client.EndpointsUpdate
	updateReceived bool

	topLevelResolver *resourceResolver
}

func (er *edsResolver) hasUpdate() bool {
	return er.updateReceived
}

func (er *edsResolver) resolveNow() {
}

func (er *edsResolver) stop() {
	er.cancel()
}

func newEDSResolver(mechanism balancerconfigbuilder.DiscoveryMechanism, xdsclient xdsClientInterface, topLevelResolver *resourceResolver) *edsResolver {
	ret := &edsResolver{
		mechanism: mechanism,
	}
	// If EDSServiceName is not set, use the cluster name as EDS service name to
	// watch.
	nameToWatch := mechanism.EDSServiceName
	if nameToWatch == "" {
		nameToWatch = mechanism.Cluster
	}
	ret.cancel = xdsclient.WatchEndpoints(nameToWatch, func(update client.EndpointsUpdate, err error) {
		topLevelResolver.mu.Lock()
		defer topLevelResolver.mu.Unlock()
		// FIXME: if err != nil
		ret.update = update
		ret.updateReceived = true
		topLevelResolver.generate()
	})
	return ret
}

type dnsResolver struct {
	target string
}

func newDNSResolver(target string, _ *resourceResolver) *dnsResolver {
	return &dnsResolver{
		target: target,
	}
}

// FIXME: dnsResolver needs to implement resolver.ClientConn, to work with the DNS resolver.

func (er *dnsResolver) hasUpdate() bool {
	panic("implement me")
}

func (dr *dnsResolver) resolveNow() {
	panic("implement me")
}

func (dr *dnsResolver) stop() {
	panic("implement me")
}
