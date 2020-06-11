/*
 *
 * Copyright 2020 gRPC authors.
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

package xdsrouting

import (
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/internal/hierarchy"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"google.golang.org/grpc/xds/internal"
	"google.golang.org/grpc/xds/internal/balancer/balancergroup"
)

const xdsRoutingName = "xds_routing_experimental"

func init() {
	balancer.Register(&routingBB{})
}

type routingBB struct{}

func (rbb *routingBB) Build(cc balancer.ClientConn, _ balancer.BuildOptions) balancer.Balancer {
	b := &routingBalancer{}
	b.logger = grpclog.NewPrefixLogger(loggingPrefix(b))
	b.stateAggregator = newRoutingBalancerStateAggregator(cc, b.logger)
	b.stateAggregator.start()
	b.bg = balancergroup.New(cc, b.stateAggregator, nil, b.logger)
	b.bg.Start()
	b.logger.Infof("Created")
	return b
}

func (rbb *routingBB) Name() string {
	return xdsRoutingName
}

func (rbb *routingBB) ParseConfig(c json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
	return parseConfig(c)
}

type actionAndMatcher struct {
	action string
	m      matcher
}

type routingBalancer struct {
	logger *grpclog.PrefixLogger

	// TODO: make this package not dependent on xds specific code. Same as for
	// weighted target balancer.
	bg              *balancergroup.BalancerGroup
	stateAggregator *routingBalancerStateAggregator

	actions map[string]action
	routes  []actionAndMatcher
	// key in matchers is route with action field set to empty string. So that
	// matchers can be reused by routes if the action is updated.
	matchers map[string]matcher
}

// TODO: remove this and use strings directly as keys for balancer group.
func makeLocalityFromName(name string) internal.LocalityID {
	return internal.LocalityID{Region: name}
}

// TODO: remove this and use strings directly as keys for balancer group.
func getNameFromLocality(id internal.LocalityID) string {
	return id.Region
}

func (rb *routingBalancer) updateActions(s balancer.ClientConnState, newConfig *lbConfig) (needRebuild bool) {
	addressesSplit := hierarchy.Group(s.ResolverState.Addresses)
	var rebuildStateAndPicker bool

	// Remove sub-pickers and sub-balancers that are not in the new action list.
	for name := range rb.actions {
		if _, ok := newConfig.actions[name]; !ok {
			l := makeLocalityFromName(name)
			rb.stateAggregator.remove(l)
			rb.bg.Remove(l)
			// Trigger a state/picker update, because we don't want `ClientConn`
			// to pick this sub-balancer anymore.
			rebuildStateAndPicker = true
		}
	}

	// For sub-balancers in the new action list,
	// - add to balancer group if it's new,
	// - forward the address/balancer config update.
	for name, newT := range newConfig.actions {
		l := makeLocalityFromName(name)

		_, ok := rb.actions[name]
		if !ok {
			// If this is a new sub-balancer, add weights to the picker map.
			rb.stateAggregator.add(l)
			// Then add to the balancer group.
			rb.bg.Add(l, balancer.Get(newT.ChildPolicy.Name))
			// Not trigger a state/picker update. Wait for the new sub-balancer
			// to send its updates.
		}

		// Forwards all the update:
		// - Addresses are from the map after splitting with hierarchy path,
		// - Top level service config and attributes are the same,
		// - Balancer config comes from the targets map.
		//
		// TODO: handle error? How to aggregate errors and return?
		_ = rb.bg.UpdateClientConnState(l, balancer.ClientConnState{
			ResolverState: resolver.State{
				Addresses:     addressesSplit[name],
				ServiceConfig: s.ResolverState.ServiceConfig,
				Attributes:    s.ResolverState.Attributes,
			},
			BalancerConfig: newT.ChildPolicy.Config,
		})
	}

	rb.actions = newConfig.actions

	return rebuildStateAndPicker
}

func routeToMatcher(r route) matcher {
	var matchers []matcher
	switch {
	case r.regex != "":
		matchers = append(matchers, newPathRegexMatcher(r.regex))
	case r.path != "":
		matchers = append(matchers, newPathExactMatcher(r.path))
	default:
		matchers = append(matchers, newPathPrefixMatcher(r.prefix))
	}
	for _, h := range r.headers {
		var matcherT matcher
		switch {
		case h.exactMatch != "":
			matcherT = newHeaderExactMatcher(h.name, h.exactMatch)
		case h.regexMatch != "":
			matcherT = newHeaderRegexMatcher(h.name, h.regexMatch)
		case h.prefixMatch != "":
			matcherT = newHeaderPrefixMatcher(h.name, h.prefixMatch)
		case h.suffixMatch != "":
			matcherT = newHeaderSuffixMatcher(h.name, h.suffixMatch)
		case h.rangeMatch != nil:
			matcherT = newHeaderRangeMatcher(h.name, h.rangeMatch.start, h.rangeMatch.end)
		default:
			matcherT = newHeaderPresentMatcher(h.name, h.presentMatch)
		}
		if h.invertMatch {
			matcherT = newInvertMatcher(matcherT)
		}
		matchers = append(matchers, matcherT)
	}
	if r.fraction != nil {
		matchers = append(matchers, newFractionMatcher(*r.fraction))
	}
	if len(matchers) == 1 {
		return matchers[0]
	}
	return newAndMatcher(matchers)
}

func routesEqual(a, b []actionAndMatcher) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		aa := a[i]
		bb := b[i]
		if aa.action != bb.action {
			return false
		}
		if !aa.m.Equal(bb.m) {
			return false
		}
	}
	return true
}

func (rb *routingBalancer) updateRoutes(newConfig *lbConfig) (needRebuild bool) {
	var newRoutes []actionAndMatcher
	newMatchers := make(map[string]matcher)
	for _, rt := range newConfig.routes {
		matcherTemp := routeToMatcher(rt)
		matcherStr := matcherTemp.String()
		if m, ok := rb.matchers[matcherStr]; ok {
			newMatchers[matcherStr] = m
			newRoutes = append(newRoutes, actionAndMatcher{action: rt.action, m: m})
			continue
		}
		// Build a new matcher if the matcher doesn't already exist.
		newM := routeToMatcher(rt)
		newMatchers[matcherStr] = newM
		newRoutes = append(newRoutes, actionAndMatcher{action: rt.action, m: newM})
	}

	rebuildStateAndPicker := !routesEqual(newRoutes, rb.routes)

	rb.routes = newRoutes
	rb.matchers = newMatchers

	if rebuildStateAndPicker {
		var rpr []routingPickerRoute
		for _, rtAndM := range rb.routes {
			rpr = append(rpr, routingPickerRoute{
				m:  rtAndM.m,
				id: rtAndM.action,
			})
		}
		rb.stateAggregator.updateRoutes(rpr)
	}
	return rebuildStateAndPicker
}

func (rb *routingBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	newConfig, ok := s.BalancerConfig.(*lbConfig)
	if !ok {
		return fmt.Errorf("unexpected balancer config with type: %T", s.BalancerConfig)
	}
	rb.logger.Infof("update with config %+v, resolver state %+v", s.BalancerConfig, s.ResolverState)
	var rebuildStateAndPicker bool
	if rb.updateActions(s, newConfig) {
		rebuildStateAndPicker = true
	}
	if rb.updateRoutes(newConfig) {
		rebuildStateAndPicker = true
	}
	if rebuildStateAndPicker {
		rb.stateAggregator.buildAndUpdate()
	}
	return nil
}

func (rb *routingBalancer) ResolverError(err error) {
	rb.bg.ResolverError(err)
}

func (rb *routingBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	rb.bg.UpdateSubConnState(sc, state)
}

func (rb *routingBalancer) Close() {
	rb.stateAggregator.close()
	rb.bg.Close()
}
