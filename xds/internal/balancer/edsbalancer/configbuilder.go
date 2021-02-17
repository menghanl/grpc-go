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

package edsbalancer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/balancer/weightedroundrobin"
	"google.golang.org/grpc/internal/hierarchy"
	internalserviceconfig "google.golang.org/grpc/internal/serviceconfig"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/xds/internal/balancer/clusterimpl"
	"google.golang.org/grpc/xds/internal/balancer/lrs"
	"google.golang.org/grpc/xds/internal/balancer/priority"
	"google.golang.org/grpc/xds/internal/balancer/weightedtarget"
	xdsclient "google.golang.org/grpc/xds/internal/client"
)

const million = 1000000

// type edsChildConfig struct {
// 	edsResp xdsclient.EndpointsUpdate
//
// 	childPolicy string
// 	// childConfig json.RawMessage
//
// 	cluster, edsService   string
// 	maxConcurrentRequests *uint32
// 	lrsServer             *string
// }

func build(edsResp xdsclient.EndpointsUpdate, cfg *EDSConfig) ([]byte, []resolver.Address) {
	// spew.Dump(edsResp)
	// spew.Dump(cfg)
	pc, addrs := buildPriorityConfig(edsResp, cfg)
	ret, err := json.Marshal(pc)
	if err != nil {
		panic(err.Error())
	}

	var prettyGot bytes.Buffer
	if err := json.Indent(&prettyGot, ret, ">>> ", "  "); err != nil {
		panic(err.Error())
	}
	// fmt.Println("\n\n", prettyGot.String())
	// spew.Dump(addrs)

	return ret, addrs
}

func buildPriorityConfig(edsResp xdsclient.EndpointsUpdate, c *EDSConfig) (*priority.LBConfig, []resolver.Address) {
	var retAddrs []resolver.Address

	// childPolicy := roundrobin.Name
	// cluster := "cluster-name-for-watch"
	// edsService := "service-name-from-parent"
	// *lrsServer = "lrs-addr-from-config"

	var (
		childPolicy = roundrobin.Name
		cluster     string
		edsService  string
		lrsServer   *string
		maxRequest  *uint32
	)
	if c != nil {
		childPolicy = c.ChildPolicy.Name // FIXME: handle nil
		cluster = c.ClusterName
		edsService = c.EDSServiceName
		lrsServer = c.LrsLoadReportingServerName
		maxRequest = c.MaxConcurrentRequests
	}

	drops := make([]clusterimpl.DropCategory, 0, len(edsResp.Drops))
	for _, d := range edsResp.Drops {
		drops = append(drops, clusterimpl.DropCategory{
			Category:           d.Category,
			RequestsPerMillion: d.Numerator * million / d.Denominator,
		})
	}
	priorityChildNames, priorities := groupLocalitiesByPriority(edsResp.Localities)

	children := make(map[string]*priority.Child)
	for priorityName, priorityLocalities := range priorities {
		wtConfig, addrs := localitiesToWeightedTarget(priorityLocalities, priorityName, childPolicy, lrsServer, cluster, edsService)
		children[priorityName] = &priority.Child{
			Config: &internalserviceconfig.BalancerConfig{
				Name: clusterimpl.Name,
				Config: &clusterimpl.LBConfig{
					Cluster:        cluster,
					EDSServiceName: edsService,
					ChildPolicy: &internalserviceconfig.BalancerConfig{
						Name:   weightedtarget.Name,
						Config: wtConfig,
					},
					LRSLoadReportingServerName: lrsServer,
					MaxConcurrentRequests:      maxRequest,
					DropCategories:             drops,
				},
			},
		}
		retAddrs = append(retAddrs, addrs...)
	}

	return &priority.LBConfig{
		Children:   children,
		Priorities: priorityChildNames,
	}, retAddrs
}

// groupLocalitiesByPriority returns the localities grouped by locality.
//
// It also returns a list of strings where each string represents a priority,
// and the list is sorted from higher priority to lower priority.
func groupLocalitiesByPriority(localities []xdsclient.Locality) ([]string, map[string][]xdsclient.Locality) {
	var priorityIntSlice []int
	priorities := make(map[string][]xdsclient.Locality)
	for _, locality := range localities {
		if locality.Weight == 0 {
			continue
		}
		priorityName := fmt.Sprintf("priority-%v", locality.Priority)
		priorities[priorityName] = append(priorities[priorityName], locality)
		priorityIntSlice = append(priorityIntSlice, int(locality.Priority))
	}
	// Sort the priorities based on the int value, deduplicate, and then turn
	// the sorted list into a string list. This will be child names, in priority
	// order.
	sort.Ints(priorityIntSlice)
	priorityIntSliceDeduped := dedupSortedIntSlice(priorityIntSlice)
	priorityNameSlice := make([]string, 0, len(priorityIntSliceDeduped))
	for _, p := range priorityIntSliceDeduped {
		priorityNameSlice = append(priorityNameSlice, fmt.Sprintf("priority-%v", p))
	}
	return priorityNameSlice, priorities
}

func dedupSortedIntSlice(a []int) []int {
	if len(a) == 0 {
		return a
	}
	i, j := 0, 1
	for ; j < len(a); j++ {
		if a[i] == a[j] {
			continue
		}
		i++
		if i != j {
			a[i] = a[j]
		}
	}
	return a[:i+1]
}

// localitiesToWeightedTarget takes a list of localities (with the same
// priority), and generates a weighted target config, and list of addresses.
//
// The addresses have path hierarchy set to [p, locality-name], so priority and
// weighted target know which child policy they are for.
func localitiesToWeightedTarget(localities []xdsclient.Locality, priorityName, childPolicy string, lrsServer *string, cluster, edsService string) (*weightedtarget.LBConfig, []resolver.Address) {
	weightedTargets := make(map[string]weightedtarget.Target)
	var addrs []resolver.Address
	for _, locality := range localities {
		localityStr, err := locality.ID.ToString()
		if err != nil {
			localityStr = fmt.Sprintf("%+v", locality.ID)
		}

		var child *internalserviceconfig.BalancerConfig
		if lrsServer != nil {
			localityID := locality.ID
			child = &internalserviceconfig.BalancerConfig{
				Name: lrs.Name,
				Config: &lrs.LBConfig{
					ClusterName:    cluster,
					EdsServiceName: edsService,
					ChildPolicy: &internalserviceconfig.BalancerConfig{
						Name: childPolicy,
					},
					LrsLoadReportingServerName: *lrsServer,
					Locality:                   &localityID,
				},
			}
		} else {
			// If lrsServer is not set, skip LRS policy.
			child = &internalserviceconfig.BalancerConfig{
				Name: childPolicy,
			}
		}
		weightedTargets[localityStr] = weightedtarget.Target{
			Weight:      locality.Weight,
			ChildPolicy: child,
		}

		for _, endpoint := range locality.Endpoints {
			// Filter out all "unhealthy" endpoints (unknown and
			// healthy are both considered to be healthy:
			// https://www.envoyproxy.io/docs/envoy/latest/api-v2/api/v2/core/health_check.proto#envoy-api-enum-core-healthstatus).
			if endpoint.HealthStatus != xdsclient.EndpointHealthStatusHealthy &&
				endpoint.HealthStatus != xdsclient.EndpointHealthStatusUnknown {
				continue
			}

			addr := resolver.Address{
				Addr: endpoint.Address,
			}
			if childPolicy == weightedroundrobin.Name && endpoint.Weight != 0 {
				ai := weightedroundrobin.AddrInfo{Weight: endpoint.Weight}
				addr = weightedroundrobin.SetAddrInfo(addr, ai)
			}
			addr = hierarchy.Set(addr, []string{priorityName, localityStr})
			addrs = append(addrs, addr)
		}
	}
	return &weightedtarget.LBConfig{
		Targets: weightedTargets,
	}, addrs
}
