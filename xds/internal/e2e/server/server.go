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

package main

import (
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/xds/internal/client"
	"google.golang.org/grpc/xds/internal/testutils/fakeserver"
	"google.golang.org/protobuf/types/known/wrapperspb"

	xdspb "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	basepb "github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	corepb "github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	routepb "github.com/envoyproxy/go-control-plane/envoy/api/v2/route"
	httppb "github.com/envoyproxy/go-control-plane/envoy/config/filter/network/http_connection_manager/v2"
	listenerpb "github.com/envoyproxy/go-control-plane/envoy/config/listener/v2"
	anypb "github.com/golang/protobuf/ptypes/any"
)

const (
	httpConnManagerURL = "type.googleapis.com/envoy.config.filter.network.http_connection_manager.v2.HttpConnectionManager"
	ldsURL             = "type.googleapis.com/envoy.api.v2.Listener"
	rdsURL             = "type.googleapis.com/envoy.api.v2.RouteConfiguration"
	cdsURL             = "type.googleapis.com/envoy.api.v2.Cluster"
	edsURL             = "type.googleapis.com/envoy.api.v2.ClusterLoadAssignment"
)

func makeXDSListener(ldsName, rdsName string) *xdspb.Listener {
	marshaledConnMgr1, _ := proto.Marshal(&httppb.HttpConnectionManager{
		RouteSpecifier: &httppb.HttpConnectionManager_Rds{
			Rds: &httppb.Rds{
				ConfigSource: &basepb.ConfigSource{
					ConfigSourceSpecifier: &basepb.ConfigSource_Ads{Ads: &basepb.AggregatedConfigSource{}},
				},
				RouteConfigName: rdsName,
			},
		},
	})
	return &xdspb.Listener{
		Name: ldsName,
		ApiListener: &listenerpb.ApiListener{
			ApiListener: &anypb.Any{
				TypeUrl: httpConnManagerURL,
				Value:   marshaledConnMgr1,
			},
		},
	}
}

func makeRouteConfig(ldsName, rdsName string, clusterWeights map[string]uint32) *xdspb.RouteConfiguration {
	var routeAction *routepb.RouteAction
	if len(clusterWeights) == 0 {
		panic("")
	} else {
		var (
			clusters []*routepb.WeightedCluster_ClusterWeight
			totalW   uint32
		)
		for c, w := range clusterWeights {
			clusters = append(clusters, &routepb.WeightedCluster_ClusterWeight{
				Name:   c,
				Weight: &wrapperspb.UInt32Value{Value: w},
			})
			totalW += w
		}
		routeAction = &routepb.RouteAction{
			ClusterSpecifier: &routepb.RouteAction_WeightedClusters{
				WeightedClusters: &routepb.WeightedCluster{
					Clusters:    clusters,
					TotalWeight: &wrapperspb.UInt32Value{Value: totalW},
				},
			},
		}
	}

	return &xdspb.RouteConfiguration{
		Name: rdsName,
		VirtualHosts: []*routepb.VirtualHost{{
			Domains: []string{ldsName},
			Routes: []*routepb.Route{
				{
					Match: &routepb.RouteMatch{PathSpecifier: &routepb.RouteMatch_Prefix{Prefix: ""}},
					Action: &routepb.Route_Route{
						Route: routeAction,
					},
				},
			},
		}},
	}
}

func makeCluster(cdsName, edsName string) *xdspb.Cluster {
	return &xdspb.Cluster{
		Name:                 cdsName,
		ClusterDiscoveryType: &xdspb.Cluster_Type{Type: xdspb.Cluster_EDS},
		EdsClusterConfig: &xdspb.Cluster_EdsClusterConfig{
			EdsConfig: &corepb.ConfigSource{
				ConfigSourceSpecifier: &corepb.ConfigSource_Ads{
					Ads: &corepb.AggregatedConfigSource{},
				},
			},
			ServiceName: edsName,
		},
		LbPolicy: xdspb.Cluster_ROUND_ROBIN,
	}
}

func makeClusterLoadAssignment(edsName string, localityName, backendAddr string /*this should ba a map[map[]]*/) *xdspb.ClusterLoadAssignment {
	clab0 := client.NewClusterLoadAssignmentBuilder(edsName, nil)
	clab0.AddLocality(localityName, 1, 0, []string{backendAddr}, nil)
	return clab0.Build()
}

var (
	goodListener1 = makeXDSListener("lds.name", "route.name")

	goodRouteConfig1 = makeRouteConfig("lds.name", "route.name",
		map[string]uint32{
			"cluster-1.name": 10,
			"cluster-2.name": 90,
		},
	)

	clusters = map[string]*xdspb.Cluster{
		"cluster-1.name": makeCluster("cluster-1.name", "service-1.name"),
		"cluster-2.name": makeCluster("cluster-2.name", "service-2.name"),
	}

	// goodCluster1 = makeCluster("cluster-1.name", "service-1.name")
	// goodCluster2     = makeCluster("cluster-2.name", "service-2.name")
	// goodCDSResponse2 = makeXDSResp(cdsURL, goodCluster2)

	assignments = map[string]*xdspb.ClusterLoadAssignment{
		"service-1.name": makeClusterLoadAssignment("service-1.name", "locality-1", "localhost:19527"),
		"service-2.name": makeClusterLoadAssignment("service-2.name", "locality-2", "localhost:19528"),
	}
)

func makeXDSResp(typ string, ms ...proto.Message) *xdspb.DiscoveryResponse {
	var resources []*anypb.Any
	for _, m := range ms {
		if m == nil {
			panic("nil")
		}
		marshaledListener1, _ := proto.Marshal(m)
		resources = append(resources, &anypb.Any{
			TypeUrl: typ,
			Value:   marshaledListener1,
		})
	}

	return &xdspb.DiscoveryResponse{
		Resources: resources,
		TypeUrl:   typ,
	}
}

func main() {
	s, cleanup, err := fakeserver.StartServer()
	if err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
	defer cleanup()

	fmt.Printf("xds server serving on %s\n", s.Address)

	fmt.Println()
	fmt.Println(" ----- beginning ----- ")

	var (
		ldsReq []string
		rdsReq []string
		cdsReq []string
		edsReq []string
	)

	for {
		req, err := s.XDSRequestChan.TimedReceive(100 * time.Second)
		if err != nil {
			log.Fatalf("no req after 100 seconds")
		}

		xdsReq := req.(*fakeserver.Request).Req.(*xdspb.DiscoveryRequest)
		fmt.Printf("req for %v: %v, version %v\n", xdsReq.TypeUrl, xdsReq.ResourceNames, xdsReq.VersionInfo)
		switch xdsReq.TypeUrl {
		case ldsURL:
			if cmp.Equal(ldsReq, xdsReq.ResourceNames) {
				fmt.Println("same resource names for LDS, ignore")
			} else {
				ldsReq = xdsReq.ResourceNames
				s.XDSResponseChan <- &fakeserver.Response{
					Resp: makeXDSResp(ldsURL, goodListener1),
				}
			}
		case rdsURL:
			if cmp.Equal(rdsReq, xdsReq.ResourceNames) {
				fmt.Println("same resource names for RDS, ignore")
			} else {
				rdsReq = xdsReq.ResourceNames
				s.XDSResponseChan <- &fakeserver.Response{
					Resp: makeXDSResp(rdsURL, goodRouteConfig1),
				}
			}
		case cdsURL:
			if cmp.Equal(cdsReq, xdsReq.ResourceNames, cmp.Transformer("Sort", func(in []string) []string {
				out := append([]string(nil), in...)
				sort.Strings(out)
				return out
			})) {
				fmt.Println("same resource names for CDS, ignore")
			} else {
				cdsReq = xdsReq.ResourceNames
				var ms []proto.Message
				for _, rn := range cdsReq {
					t, ok := clusters[rn]
					if ok {
						ms = append(ms, t)
					} else {
						fmt.Printf("unknown CDS name: %v\n", rn)
					}
				}
				s.XDSResponseChan <- &fakeserver.Response{
					Resp: makeXDSResp(cdsURL, ms...),
				}
			}
		case edsURL:
			if cmp.Equal(edsReq, xdsReq.ResourceNames, cmp.Transformer("Sort", func(in []string) []string {
				out := append([]string(nil), in...)
				sort.Strings(out)
				return out
			})) {
				fmt.Println("same resource names for EDS, ignore")
			} else {
				edsReq = xdsReq.ResourceNames
				var ms []proto.Message
				for _, rn := range edsReq {
					t, ok := assignments[rn]
					if ok {
						ms = append(ms, t)
					} else {
						fmt.Printf("unknown EDS name: %v\n", rn)
					}
				}
				s.XDSResponseChan <- &fakeserver.Response{
					Resp: makeXDSResp(edsURL, ms...),
				}
			}
		}

		fmt.Println()
	}
}
