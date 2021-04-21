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
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	internalserviceconfig "google.golang.org/grpc/internal/serviceconfig"
	"google.golang.org/grpc/xds/internal/balancer/clusterresolver/balancerconfigbuilder"
)

const (
	testJSONConfig1 = `{
  "discoveryMechanisms": [{
    "cluster": "test-cluster-name",
    "lrsLoadReportingServerName": "test-lrs-server",
    "maxConcurrentRequests": 314,
    "type": "EDS",
    "edsServiceName": "test-eds-service-name"
  }]
}`
	testJSONConfig2 = `{
  "discoveryMechanisms": [{
    "cluster": "test-cluster-name",
    "lrsLoadReportingServerName": "test-lrs-server",
    "maxConcurrentRequests": 314,
    "type": "EDS",
    "edsServiceName": "test-eds-service-name"
  },{
    "type": "LOGICAL_DNS"
  }]
}`
	testJSONConfig3 = `{
  "discoveryMechanisms": [{
    "cluster": "test-cluster-name",
    "lrsLoadReportingServerName": "test-lrs-server",
    "maxConcurrentRequests": 314,
    "type": "EDS",
    "edsServiceName": "test-eds-service-name"
  }],
  "localityPickingPolicy":[{"pick_first":{}}],
  "endpointPickingPolicy":[{"pick_first":{}}]
}`

	testClusterName = "test-cluster-name"
	testLRSServer   = "test-lrs-server"
	testMaxRequests = 314
	testEDSServcie  = "test-eds-service-name"
)

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name    string
		js      string
		want    *LBConfig
		wantErr bool
	}{
		{
			name:    "empty json",
			js:      "",
			want:    nil,
			wantErr: true,
		},
		{
			name: "OK with one discovery mechanism",
			js:   testJSONConfig1,
			want: &LBConfig{
				DiscoveryMechanisms: []balancerconfigbuilder.DiscoveryMechanism{
					{
						Cluster:                 testClusterName,
						LoadReportingServerName: newString(testLRSServer),
						MaxConcurrentRequests:   newUint32(testMaxRequests),
						Type:                    balancerconfigbuilder.DiscoveryMechanismTypeEDS,
						EDSServiceName:          testEDSServcie,
					},
				},
				LocalityPickingPolicy: nil,
				EndpointPickingPolicy: nil,
			},
			wantErr: false,
		},
		{
			name: "OK with multiple discovery mechanisms",
			js:   testJSONConfig2,
			want: &LBConfig{
				DiscoveryMechanisms: []balancerconfigbuilder.DiscoveryMechanism{
					{
						Cluster:                 testClusterName,
						LoadReportingServerName: newString(testLRSServer),
						MaxConcurrentRequests:   newUint32(testMaxRequests),
						Type:                    balancerconfigbuilder.DiscoveryMechanismTypeEDS,
						EDSServiceName:          testEDSServcie,
					},
					{
						Type: balancerconfigbuilder.DiscoveryMechanismTypeLogicalDNS,
					},
				},
				LocalityPickingPolicy: nil,
				EndpointPickingPolicy: nil,
			},
			wantErr: false,
		},
		{
			name: "OK with picking policy override",
			js:   testJSONConfig3,
			want: &LBConfig{
				DiscoveryMechanisms: []balancerconfigbuilder.DiscoveryMechanism{
					{
						Cluster:                 testClusterName,
						LoadReportingServerName: newString(testLRSServer),
						MaxConcurrentRequests:   newUint32(testMaxRequests),
						Type:                    balancerconfigbuilder.DiscoveryMechanismTypeEDS,
						EDSServiceName:          testEDSServcie,
					},
				},
				LocalityPickingPolicy: &internalserviceconfig.BalancerConfig{
					Name:   "pick_first",
					Config: nil,
				},
				EndpointPickingPolicy: &internalserviceconfig.BalancerConfig{
					Name:   "pick_first",
					Config: nil,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseConfig([]byte(tt.js))
			if (err != nil) != tt.wantErr {
				t.Errorf("parseConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("parseConfig() got unexpected output, diff (-got +want): %v", diff)
			}
		})
	}
}

func TestDiscoveryMechanismTypeMarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		typ  balancerconfigbuilder.DiscoveryMechanismType
		want string
	}{
		{
			name: "eds",
			typ:  balancerconfigbuilder.DiscoveryMechanismTypeEDS,
			want: `"EDS"`,
		},
		{
			name: "dns",
			typ:  balancerconfigbuilder.DiscoveryMechanismTypeLogicalDNS,
			want: `"LOGICAL_DNS"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.typ)
			if err != nil {
				t.Errorf("DiscoveryMechanismTypeEDS.MarshalJSON() error = %v, wantErr %v", err, false)
				return
			}
			if gotS := string(got); gotS != tt.want {
				t.Errorf("DiscoveryMechanismTypeEDS.MarshalJSON() got %q, want %q", gotS, tt.want)
			}
		})
	}
}
func TestDiscoveryMechanismTypeUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		js      string
		want    balancerconfigbuilder.DiscoveryMechanismType
		wantErr bool
	}{
		{
			name: "eds",
			js:   `"EDS"`,
			want: balancerconfigbuilder.DiscoveryMechanismTypeEDS,
		},
		{
			name: "dns",
			js:   `"LOGICAL_DNS"`,
			want: balancerconfigbuilder.DiscoveryMechanismTypeLogicalDNS,
		},
		{
			name:    "error",
			js:      `"1234"`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got balancerconfigbuilder.DiscoveryMechanismType
			err := json.Unmarshal([]byte(tt.js), &got)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("parseConfig() got unexpected output, diff (-got +want): %v", diff)
			}
		})
	}
}

func newString(s string) *string {
	return &s
}

func newUint32(i uint32) *uint32 {
	return &i
}
