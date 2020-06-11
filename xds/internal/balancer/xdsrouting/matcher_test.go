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
	"context"
	"testing"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/metadata"
)

func TestAndMatcher(t *testing.T) {
	tests := []struct {
		name     string
		matchers []matcher
		info     balancer.PickInfo
		want     bool
	}{
		{
			name: "both match",
			matchers: []matcher{
				newPathExactMatcher("/a/b"),
				newHeaderExactMatcher("th", "tv"),
			},
			info: balancer.PickInfo{
				FullMethodName: "/a/b",
				Ctx:            metadata.NewOutgoingContext(context.Background(), metadata.Pairs("th", "tv")),
			},
			want: true,
		},
		{
			name: "only one match",
			matchers: []matcher{
				newPathExactMatcher("/a/b"),
				newHeaderExactMatcher("th", "tv"),
			},
			info: balancer.PickInfo{
				FullMethodName: "/z/y",
				Ctx:            metadata.NewOutgoingContext(context.Background(), metadata.Pairs("th", "tv")),
			},
			want: false,
		},
		{
			name: "both not match",
			matchers: []matcher{
				newPathExactMatcher("/z/y"),
				newHeaderExactMatcher("th", "abc"),
			},
			info: balancer.PickInfo{
				FullMethodName: "/a/b",
				Ctx:            metadata.NewOutgoingContext(context.Background(), metadata.Pairs("th", "tv")),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newAndMatcher(tt.matchers)
			if got := a.match(tt.info); got != tt.want {
				t.Errorf("match() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_invertMatcher_match(t *testing.T) {
	tests := []struct {
		name string
		m    matcher
		info balancer.PickInfo
	}{
		{
			name: "true->false",
			m:    newPathExactMatcher("/a/b"),
			info: balancer.PickInfo{FullMethodName: "/a/b"},
		},
		{
			name: "false->true",
			m:    newPathExactMatcher("/z/y"),
			info: balancer.PickInfo{FullMethodName: "/a/b"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newInvertMatcher(tt.m).match(tt.info)
			want := !tt.m.match(tt.info)
			if got != want {
				t.Errorf("match() = %v, want %v", got, want)
			}
		})
	}
}
