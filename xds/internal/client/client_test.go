/*
 *
 * Copyright 2019 gRPC authors.
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

package client

import (
	"testing"
	"time"

	corepb "github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	"google.golang.org/grpc"
	"google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/xds/internal/client/bootstrap"
	"google.golang.org/grpc/xds/internal/testutils"
)

const (
	testXDSServer   = "xds-server"
	chanRecvTimeout = 100 * time.Millisecond

	testLDSName = "test-lds"
	testRDSName = "test-rds"
	testCDSName = "test-cds"
	testEDSName = "test-eds"
)

type testXDSV2Client struct {
	r updateReceiver

	addWatches    map[string]chan string
	removeWatches map[string]chan string
}

func overrideNewXDSV2Client() (<-chan *testXDSV2Client, func()) {
	oldNewXDSV2Client := newXDSV2Client
	ch := make(chan *testXDSV2Client, 1)
	newXDSV2Client = func(parent *Client, cc *grpc.ClientConn, nodeProto *corepb.Node, backoff func(int) time.Duration, logger *grpclog.PrefixLogger) xdsv2Client {
		ret := newTestXDSV2Client(parent)
		ch <- ret
		return ret
	}
	return ch, func() { newXDSV2Client = oldNewXDSV2Client }
}

func newTestXDSV2Client(r updateReceiver) *testXDSV2Client {
	addWatches := make(map[string]chan string)
	addWatches[ldsURL] = make(chan string, 10)
	addWatches[rdsURL] = make(chan string, 10)
	addWatches[cdsURL] = make(chan string, 10)
	addWatches[edsURL] = make(chan string, 10)
	removeWatches := make(map[string]chan string)
	removeWatches[ldsURL] = make(chan string, 10)
	removeWatches[rdsURL] = make(chan string, 10)
	removeWatches[cdsURL] = make(chan string, 10)
	removeWatches[edsURL] = make(chan string, 10)
	return &testXDSV2Client{
		r:             r,
		addWatches:    addWatches,
		removeWatches: removeWatches,
	}
}

func (c *testXDSV2Client) addWatch(resourceType string, resourceName string) {
	c.addWatches[resourceType] <- resourceName
}

func (c *testXDSV2Client) removeWatch(resourceType string, resourceName string) {
	c.removeWatches[resourceType] <- resourceName
}

func (c *testXDSV2Client) close() {}

func clientOpts(balancerName string) Options {
	return Options{
		Config: bootstrap.Config{
			BalancerName: balancerName,
			Creds:        grpc.WithInsecure(),
			NodeProto:    &corepb.Node{},
		},
		// // WithTimeout is deprecated. But we are OK to call it here from the
		// // test, so we clearly know that the dial failed.
		// DialOpts: []grpc.DialOption{grpc.WithTimeout(5 * time.Second), grpc.WithBlock()},
	}
}

// TestWatchCallAnotherWatch covers the case where watch() is called inline by a
// callback. It makes sure it doesn't cause a deadlock.
func TestWatchCallAnotherWatch(t *testing.T) {
	v2ClientCh, cleanup := overrideNewXDSV2Client()
	defer cleanup()

	c, err := New(clientOpts(testXDSServer))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer c.Close()

	v2Client := <-v2ClientCh

	clusterUpdateCh := testutils.NewChannel()
	clusterErrCh := testutils.NewChannel()
	c.WatchCluster(testCDSName, func(update ClusterUpdate, err error) {
		clusterUpdateCh.Send(update)
		clusterErrCh.Send(err)
		// Calls another watch inline, to ensure there's deadlock.
		c.WatchCluster("another-random-name", func(ClusterUpdate, error) {})
	})

	wantUpdate := ClusterUpdate{ServiceName: testEDSName}
	v2Client.r.newUpdate(cdsURL, map[string]interface{}{
		testCDSName: wantUpdate,
	})

	if u, err := clusterUpdateCh.Receive(); err != nil || u != wantUpdate {
		t.Errorf("unexpected clusterUpdate: %v, error receiving from channel: %v", u, err)
	}
	if e, err := clusterErrCh.Receive(); err != nil || e != nil {
		t.Errorf("unexpected clusterError: %v, error receiving from channel: %v", e, err)
	}

	wantUpdate2 := ClusterUpdate{ServiceName: testEDSName + "2"}
	v2Client.r.newUpdate(cdsURL, map[string]interface{}{
		testCDSName: wantUpdate2,
	})

	if u, err := clusterUpdateCh.Receive(); err != nil || u != wantUpdate2 {
		t.Errorf("unexpected clusterUpdate: %v, error receiving from channel: %v", u, err)
	}
	if e, err := clusterErrCh.Receive(); err != nil || e != nil {
		t.Errorf("unexpected clusterError: %v, error receiving from channel: %v", e, err)
	}
}
