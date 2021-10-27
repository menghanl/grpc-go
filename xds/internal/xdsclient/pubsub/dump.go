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
 */

package pubsub

import (
	anypb "github.com/golang/protobuf/ptypes/any"
	"google.golang.org/grpc/xds/internal/xdsclient/resource"
)

func rawFromCache(s string, cache interface{}) *anypb.Any {
	switch c := cache.(type) {
	case map[string]resource.ListenerUpdate:
		v, ok := c[s]
		if !ok {
			return nil
		}
		return v.Raw
	case map[string]resource.RouteConfigUpdate:
		v, ok := c[s]
		if !ok {
			return nil
		}
		return v.Raw
	case map[string]resource.ClusterUpdate:
		v, ok := c[s]
		if !ok {
			return nil
		}
		return v.Raw
	case map[string]resource.EndpointsUpdate:
		v, ok := c[s]
		if !ok {
			return nil
		}
		return v.Raw
	default:
		return nil
	}
}

func (pb *Pubsub) Dump(t resource.ResourceType) (string, map[string]resource.UpdateWithMD) {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	var (
		version string
		md      map[string]resource.UpdateMetadata
		cache   interface{}
	)
	switch t {
	case resource.ListenerResource:
		version = pb.ldsVersion
		md = pb.ldsMD
		cache = pb.ldsCache
	case resource.RouteConfigResource:
		version = pb.rdsVersion
		md = pb.rdsMD
		cache = pb.rdsCache
	case resource.ClusterResource:
		version = pb.cdsVersion
		md = pb.cdsMD
		cache = pb.cdsCache
	case resource.EndpointsResource:
		version = pb.edsVersion
		md = pb.edsMD
		cache = pb.edsCache
	default:
		pb.logger.Errorf("dumping resource of unknown type: %v", t)
		return "", nil
	}

	ret := make(map[string]resource.UpdateWithMD, len(md))
	for s, md := range md {
		ret[s] = resource.UpdateWithMD{
			MD:  md,
			Raw: rawFromCache(s, cache),
		}
	}
	return version, ret
}
