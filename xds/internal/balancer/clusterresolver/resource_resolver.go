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
	"reflect"
	"sync"

	"google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/xds/internal/balancer/clusterresolver/balancerconfigbuilder"
	"google.golang.org/grpc/xds/internal/client"
)

type resourceUpdate struct {
	p   []balancerconfigbuilder.PriorityConfig
	err error
}

type resolverInterface interface {
	lastUpdate() (interface{}, bool)
	resolveNow()
	stop()
}

type discoveryMechanismKey struct {
	typ  balancerconfigbuilder.DiscoveryMechanismType
	name string
}

type resolverMechanismTuple struct {
	dm    balancerconfigbuilder.DiscoveryMechanism
	dmKey discoveryMechanismKey
	r     resolverInterface
}

type resourceResolver struct {
	xdsclient xdsClientInterface
	logger    *grpclog.PrefixLogger

	updateChannel chan *resourceUpdate

	// mu protects the slice and map, and content of the resolvers in the slice.
	mu           sync.Mutex
	mechanisms   []balancerconfigbuilder.DiscoveryMechanism
	resolvers    []resolverMechanismTuple
	resolversMap map[discoveryMechanismKey]resolverInterface
}

func newResourceResolver(xdsclient xdsClientInterface, logger *grpclog.PrefixLogger) *resourceResolver {
	return &resourceResolver{
		xdsclient:     xdsclient,
		logger:        logger,
		updateChannel: make(chan *resourceUpdate, 1),
		resolversMap:  make(map[discoveryMechanismKey]resolverInterface),
	}
}

func (rr *resourceResolver) updateMechanisms(mechanisms []balancerconfigbuilder.DiscoveryMechanism) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	if reflect.DeepEqual(rr.mechanisms, mechanisms) {
		return
	}
	rr.mechanisms = mechanisms
	rr.resolvers = make([]resolverMechanismTuple, len(mechanisms))
	newDMs := make(map[discoveryMechanismKey]bool)

	// Start one watch for new discover mechanisms.
	for i, dm := range mechanisms {
		switch dm.Type {
		case balancerconfigbuilder.DiscoveryMechanismTypeEDS:
			// If EDSServiceName is not set, use the cluster name as EDS service
			// name to watch.
			nameToWatch := dm.EDSServiceName
			if nameToWatch == "" {
				nameToWatch = dm.Cluster
			}
			dmKey := discoveryMechanismKey{typ: dm.Type, name: nameToWatch}
			newDMs[dmKey] = true

			r := rr.resolversMap[dmKey]
			if r == nil {
				r = newEDSResolver(nameToWatch, rr.xdsclient, rr)
				rr.resolversMap[dmKey] = r
			}
			rr.resolvers[i] = resolverMechanismTuple{dm: dm, dmKey: dmKey, r: r}
		case balancerconfigbuilder.DiscoveryMechanismTypeLogicalDNS:
			// watchDNS
			dmKey := discoveryMechanismKey{typ: dm.Type, name: dm.DNSHostname}
			newDMs[dmKey] = true

			r := rr.resolversMap[dmKey]
			if r == nil {
				r = newDNSResolver(dm.DNSHostname, rr)
				rr.resolversMap[dmKey] = r
			}
			rr.resolvers[i] = resolverMechanismTuple{dm: dm, dmKey: dmKey, r: r}
		}
	}
	for dm, r := range rr.resolversMap {
		if !newDMs[dm] {
			delete(rr.resolversMap, dm)
			r.stop()
		}
	}
	// Regenerate in case priority order changed.
	rr.generate()
}

func (rr *resourceResolver) resolveNow() {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	for _, r := range rr.resolversMap {
		r.resolveNow()
	}
}

func (rr *resourceResolver) stop() {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	for dm, r := range rr.resolversMap {
		delete(rr.resolversMap, dm)
		r.stop()
	}
	rr.mechanisms = nil
	rr.resolvers = nil
}

// caller must hold rr.mu.
func (rr *resourceResolver) generate() {
	var ret []balancerconfigbuilder.PriorityConfig
	for _, rDM := range rr.resolvers {
		r, ok := rr.resolversMap[rDM.dmKey]
		if !ok {
			rr.logger.Infof("resolver for %+v not found, should never happen", rDM.dmKey)
			continue
		}

		u, ok := r.lastUpdate()
		if !ok {
			// Don't send updates to parent until all resolvers have update to
			// send.
			return
		}
		switch uu := u.(type) {
		case client.EndpointsUpdate:
			ret = append(ret, balancerconfigbuilder.PriorityConfig{
				Mechanism: rDM.dm,
				EDSResp:   uu,
			})
		case []string:
			ret = append(ret, balancerconfigbuilder.PriorityConfig{
				Mechanism: rDM.dm,
				Addresses: uu,
			})
		}
	}
	select {
	case <-rr.updateChannel:
	default:
	}
	rr.updateChannel <- &resourceUpdate{p: ret}
}

type edsResolver struct {
	cancel func()

	update         client.EndpointsUpdate
	updateReceived bool
}

func (er *edsResolver) lastUpdate() (interface{}, bool) {
	if !er.updateReceived {
		return nil, false
	}
	return er.update, true
}

func (er *edsResolver) resolveNow() {
}

func (er *edsResolver) stop() {
	er.cancel()
}

func newEDSResolver(nameToWatch string, xdsclient xdsClientInterface, topLevelResolver *resourceResolver) *edsResolver {
	ret := &edsResolver{}
	topLevelResolver.logger.Infof("EDS watch started on %v", nameToWatch)
	cancel := xdsclient.WatchEndpoints(nameToWatch, func(update client.EndpointsUpdate, err error) {
		topLevelResolver.mu.Lock()
		defer topLevelResolver.mu.Unlock()
		if err != nil {
			select {
			case <-topLevelResolver.updateChannel:
			default:
			}
			topLevelResolver.updateChannel <- &resourceUpdate{err: err}
			return
		}
		ret.update = update
		ret.updateReceived = true
		topLevelResolver.generate()
	})
	ret.cancel = func() {
		topLevelResolver.logger.Infof("EDS watch canceled on %v", nameToWatch)
		cancel()
	}
	return ret
}
