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
	"errors"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc/xds/internal/client/load"

	v2corepb "github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	v2endpointpb "github.com/envoyproxy/go-control-plane/envoy/api/v2/endpoint"
	lrsgrpc "github.com/envoyproxy/go-control-plane/envoy/service/load_stats/v2"
	lrspb "github.com/envoyproxy/go-control-plane/envoy/service/load_stats/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/xds/internal"
)

type lrsStream lrsgrpc.LoadReportingService_StreamLoadStatsClient

func (v2c *client) NewLoadStatsStream(ctx context.Context, cc *grpc.ClientConn) (grpc.ClientStream, error) {
	c := lrsgrpc.NewLoadReportingServiceClient(cc)
	return c.StreamLoadStats(ctx)
}

func (v2c *client) SendFirstLoadStatsRequest(s grpc.ClientStream) error {
	stream, ok := s.(lrsStream)
	if !ok {
		return fmt.Errorf("lrs: Attempt to send request on unsupported stream type: %T", s)
	}
	node := proto.Clone(v2c.nodeProto).(*v2corepb.Node)
	if node == nil {
		node = &v2corepb.Node{}
	}

	req := &lrspb.LoadStatsRequest{Node: node}
	v2c.logger.Infof("lrs: sending init LoadStatsRequest: %v", req)
	return stream.Send(req)
}

func (v2c *client) HandleLoadStatsResponse(s grpc.ClientStream) ([]string, time.Duration, error) {
	stream, ok := s.(lrsStream)
	if !ok {
		return nil, 0, fmt.Errorf("lrs: Attempt to receive response on unsupported stream type: %T", s)
	}

	resp, err := stream.Recv()
	if err != nil {
		return nil, 0, fmt.Errorf("lrs: failed to receive first response: %v", err)
	}
	v2c.logger.Infof("lrs: received first LoadStatsResponse: %+v", resp)

	interval, err := ptypes.Duration(resp.GetLoadReportingInterval())
	if err != nil {
		return nil, 0, fmt.Errorf("lrs: failed to convert report interval: %v", err)
	}

	if resp.ReportEndpointGranularity {
		// TODO: fixme to support per endpoint loads.
		return nil, 0, errors.New("lrs: endpoint loads requested, but not supported by current implementation")
	}

	clusters := resp.Clusters
	if resp.SendAllClusters {
		clusters = nil
	}

	return clusters, interval, nil
}

func (v2c *client) SendLoadStatsRequest(s grpc.ClientStream, loadStore *load.Store, clusterNames []string) error {
	stream, ok := s.(lrsStream)
	if !ok {
		return fmt.Errorf("lrs: Attempt to send request on unsupported stream type: %T", s)
	}

	var clusterStats []*v2endpointpb.ClusterStats
	sds := loadStore.Stats(clusterNames)
	for _, sd := range sds {
		var (
			droppedReqs   []*v2endpointpb.ClusterStats_DroppedRequests
			localityStats []*v2endpointpb.UpstreamLocalityStats
		)
		for category, count := range sd.Drops {
			droppedReqs = append(droppedReqs, &v2endpointpb.ClusterStats_DroppedRequests{
				Category:     category,
				DroppedCount: count,
			})
		}
		for l, localityData := range sd.LocalityStats {
			lid, err := internal.LocalityIDFromString(l)
			if err != nil {
				return err
			}
			var loadMetricStats []*v2endpointpb.EndpointLoadMetricStats
			for name, loadData := range localityData.LoadStats {
				loadMetricStats = append(loadMetricStats, &v2endpointpb.EndpointLoadMetricStats{
					MetricName:                    name,
					NumRequestsFinishedWithMetric: loadData.Count,
					TotalMetricValue:              loadData.Sum,
				})
			}
			localityStats = append(localityStats, &v2endpointpb.UpstreamLocalityStats{
				Locality: &v2corepb.Locality{
					Region:  lid.Region,
					Zone:    lid.Zone,
					SubZone: lid.SubZone,
				},
				TotalSuccessfulRequests: localityData.RequestStats.Succeeded,
				TotalRequestsInProgress: localityData.RequestStats.InProgress,
				TotalErrorRequests:      localityData.RequestStats.Errored,
				LoadMetricStats:         loadMetricStats,
				UpstreamEndpointStats:   nil, // TODO: populate for per endpoint loads.
			})
		}

		clusterStats = append(clusterStats, &v2endpointpb.ClusterStats{
			ClusterName:           sd.Cluster,
			ClusterServiceName:    sd.Service,
			UpstreamLocalityStats: localityStats,
			TotalDroppedRequests:  sd.TotalDrops,
			DroppedRequests:       droppedReqs,
			LoadReportInterval:    ptypes.DurationProto(sd.ReportInterval),
		})

	}

	req := &lrspb.LoadStatsRequest{ClusterStats: clusterStats}
	v2c.logger.Infof("lrs: sending LRS loads: %+v", req)
	return stream.Send(req)
}
