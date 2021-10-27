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

package v2

import (
	"context"
	"testing"
	"time"

	xdspb "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/xds/internal/testutils/fakeserver"
	"google.golang.org/grpc/xds/internal/xdsclient"
	"google.golang.org/grpc/xds/internal/xdsclient/resource"
)

// doLDS makes a LDS watch, and waits for the response and ack to finish.
//
// This is called by RDS tests to start LDS first, because LDS is a
// pre-requirement for RDS, and RDS handle would fail without an existing LDS
// watch.
func doLDS(ctx context.Context, t *testing.T, v2c xdsclient.APIClient, fakeServer *fakeserver.Server) {
	v2c.AddWatch(resource.ListenerResource, goodLDSTarget1)
	if _, err := fakeServer.XDSRequestChan.Receive(ctx); err != nil {
		t.Fatalf("Timeout waiting for LDS request: %v", err)
	}
}

// TestRDSHandleResponseWithRouting starts a fake xDS server, makes a ClientConn
// to it, and creates a v2Client using it. Then, it registers an LDS and RDS
// watcher and tests different RDS responses.
func (s) TestRDSHandleResponseWithRouting(t *testing.T) {
	tests := []struct {
		name          string
		rdsResponse   *xdspb.DiscoveryResponse
		wantErr       bool
		wantUpdate    map[string]resource.RouteConfigUpdateErrTuple
		wantUpdateMD  resource.UpdateMetadata
		wantUpdateErr bool
	}{
		// Badly marshaled RDS response.
		{
			name:        "badly-marshaled-response",
			rdsResponse: badlyMarshaledRDSResponse,
			wantErr:     true,
			wantUpdate:  nil,
			wantUpdateMD: resource.UpdateMetadata{
				Status: resource.ServiceStatusNACKed,
				ErrState: &resource.UpdateErrorMetadata{
					Err: cmpopts.AnyError,
				},
			},
			wantUpdateErr: false,
		},
		// Response does not contain RouteConfiguration proto.
		{
			name:        "no-route-config-in-response",
			rdsResponse: badResourceTypeInRDSResponse,
			wantErr:     true,
			wantUpdate:  nil,
			wantUpdateMD: resource.UpdateMetadata{
				Status: resource.ServiceStatusNACKed,
				ErrState: &resource.UpdateErrorMetadata{
					Err: cmpopts.AnyError,
				},
			},
			wantUpdateErr: false,
		},
		// No virtualHosts in the response. Just one test case here for a bad
		// RouteConfiguration, since the others are covered in
		// TestGetClusterFromRouteConfiguration.
		{
			name:        "no-virtual-hosts-in-response",
			rdsResponse: noVirtualHostsInRDSResponse,
			wantErr:     false,
			wantUpdate: map[string]resource.RouteConfigUpdateErrTuple{
				goodRouteName1: {Update: resource.RouteConfigUpdate{
					VirtualHosts: nil,
					Raw:          marshaledNoVirtualHostsRouteConfig,
				}},
			},
			wantUpdateMD: resource.UpdateMetadata{
				Status: resource.ServiceStatusACKed,
			},
			wantUpdateErr: false,
		},
		// Response contains one good RouteConfiguration, uninteresting though.
		{
			name:        "one-uninteresting-route-config",
			rdsResponse: goodRDSResponse2,
			wantErr:     false,
			wantUpdate: map[string]resource.RouteConfigUpdateErrTuple{
				goodRouteName2: {Update: resource.RouteConfigUpdate{
					VirtualHosts: []*resource.VirtualHost{
						{
							Domains: []string{uninterestingDomain},
							Routes: []*resource.Route{{Prefix: newStringP(""),
								WeightedClusters: map[string]resource.WeightedCluster{uninterestingClusterName: {Weight: 1}},
								RouteAction:      resource.RouteActionRoute}},
						},
						{
							Domains: []string{goodLDSTarget1},
							Routes: []*resource.Route{{
								Prefix:           newStringP(""),
								WeightedClusters: map[string]resource.WeightedCluster{goodClusterName2: {Weight: 1}},
								RouteAction:      resource.RouteActionRoute}},
						},
					},
					Raw: marshaledGoodRouteConfig2,
				}},
			},
			wantUpdateMD: resource.UpdateMetadata{
				Status: resource.ServiceStatusACKed,
			},
			wantUpdateErr: false,
		},
		// Response contains one good interesting RouteConfiguration.
		{
			name:        "one-good-route-config",
			rdsResponse: goodRDSResponse1,
			wantErr:     false,
			wantUpdate: map[string]resource.RouteConfigUpdateErrTuple{
				goodRouteName1: {Update: resource.RouteConfigUpdate{
					VirtualHosts: []*resource.VirtualHost{
						{
							Domains: []string{uninterestingDomain},
							Routes: []*resource.Route{{
								Prefix:           newStringP(""),
								WeightedClusters: map[string]resource.WeightedCluster{uninterestingClusterName: {Weight: 1}},
								RouteAction:      resource.RouteActionRoute}},
						},
						{
							Domains: []string{goodLDSTarget1},
							Routes: []*resource.Route{{Prefix: newStringP(""),
								WeightedClusters: map[string]resource.WeightedCluster{goodClusterName1: {Weight: 1}},
								RouteAction:      resource.RouteActionRoute}},
						},
					},
					Raw: marshaledGoodRouteConfig1,
				}},
			},
			wantUpdateMD: resource.UpdateMetadata{
				Status: resource.ServiceStatusACKed,
			},
			wantUpdateErr: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testWatchHandle(t, &watchHandleTestcase{
				rType:            resource.RouteConfigResource,
				resourceName:     goodRouteName1,
				responseToHandle: test.rdsResponse,
				wantHandleErr:    test.wantErr,
				wantUpdate:       test.wantUpdate,
				wantUpdateMD:     test.wantUpdateMD,
				wantUpdateErr:    test.wantUpdateErr,
			})
		})
	}
}

// TestRDSHandleResponseWithoutRDSWatch tests the case where the v2Client
// receives an RDS response without a registered RDS watcher.
func (s) TestRDSHandleResponseWithoutRDSWatch(t *testing.T) {
	fakeServer, cc, cleanup := startServerAndGetCC(t)
	defer cleanup()

	v2c, err := newV2Client(&testUpdateReceiver{
		f: func(resource.ResourceType, map[string]interface{}, resource.UpdateMetadata) {},
	}, cc, goodNodeProto, func(int) time.Duration { return 0 }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer v2c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	doLDS(ctx, t, v2c, fakeServer)

	if v2c.handleRDSResponse(badResourceTypeInRDSResponse) == nil {
		t.Fatal("v2c.handleRDSResponse() succeeded, should have failed")
	}

	if v2c.handleRDSResponse(goodRDSResponse1) != nil {
		t.Fatal("v2c.handleRDSResponse() succeeded, should have failed")
	}
}
