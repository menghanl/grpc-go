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

package client

import (
	"testing"

	"google.golang.org/grpc/xds/internal/testutils"
)

// TestServiceWatch covers the case where an update is received after a watch.
func (s) TestServiceWatch(t *testing.T) {
	v2ClientCh, cleanup := overrideNewXDSV2Client()
	defer cleanup()

	c, err := New(clientOpts(testXDSServer))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer c.Close()

	v2Client := <-v2ClientCh

	serviceUpdateCh := testutils.NewChannel()
	serviceErrCh := testutils.NewChannel()
	c.WatchService(testLDSName, func(update ServiceUpdate, err error) {
		serviceUpdateCh.Send(update)
		serviceErrCh.Send(err)
	})

	wantUpdate := ServiceUpdate{Cluster: testCDSName}

	<-v2Client.addWatches[ldsURL]
	v2Client.r.newUpdate(ldsURL, map[string]interface{}{
		testLDSName: ldsUpdate{routeName: testRDSName},
	})
	<-v2Client.addWatches[rdsURL]
	v2Client.r.newUpdate(rdsURL, map[string]interface{}{
		testRDSName: rdsUpdate{clusterName: testCDSName},
	})

	if u, err := serviceUpdateCh.Receive(); err != nil || u != wantUpdate {
		t.Errorf("unexpected serviceUpdate: %v, error receiving from channel: %v", u, err)
	}
	if e, err := serviceErrCh.Receive(); err != nil || e != nil {
		t.Errorf("unexpected serviceError: %v, error receiving from channel: %v", e, err)
	}
}

// TestServiceWatchLDSUpdate covers the case that after first LDS and first RDS
// response, the second LDS response trigger an new RDS watch, and an update of
// the old RDS watch doesn't trigger update to service callback.
func (s) TestServiceWatchLDSUpdate(t *testing.T) {
	v2ClientCh, cleanup := overrideNewXDSV2Client()
	defer cleanup()

	c, err := New(clientOpts(testXDSServer))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer c.Close()

	v2Client := <-v2ClientCh

	serviceUpdateCh := testutils.NewChannel()
	serviceErrCh := testutils.NewChannel()
	c.WatchService(testLDSName, func(update ServiceUpdate, err error) {
		serviceUpdateCh.Send(update)
		serviceErrCh.Send(err)
	})

	wantUpdate := ServiceUpdate{Cluster: testCDSName}

	<-v2Client.addWatches[ldsURL]
	v2Client.r.newUpdate(ldsURL, map[string]interface{}{
		testLDSName: ldsUpdate{routeName: testRDSName},
	})
	<-v2Client.addWatches[rdsURL]
	v2Client.r.newUpdate(rdsURL, map[string]interface{}{
		testRDSName: rdsUpdate{clusterName: testCDSName},
	})

	if u, err := serviceUpdateCh.Receive(); err != nil || u != wantUpdate {
		t.Errorf("unexpected serviceUpdate: %v, error receiving from channel: %v", u, err)
	}
	if e, err := serviceErrCh.Receive(); err != nil || e != nil {
		t.Errorf("unexpected serviceError: %v, error receiving from channel: %v", e, err)
	}

	// Another LDS update with a different RDS_name.
	v2Client.r.newUpdate(ldsURL, map[string]interface{}{
		testLDSName: ldsUpdate{routeName: testRDSName + "2"},
	})
	<-v2Client.addWatches[rdsURL]

	// Another update for the old name.
	v2Client.r.newUpdate(rdsURL, map[string]interface{}{
		testRDSName: rdsUpdate{clusterName: testCDSName},
	})

	if u, err := serviceUpdateCh.Receive(); err == nil {
		t.Errorf("unexpected serviceUpdate: %v, %v, want channel recv timeout", u, err)
	}
	if e, err := serviceErrCh.Receive(); err == nil {
		t.Errorf("unexpected serviceError: %v, %v, want channel recv timeout", e, err)
	}

	wantUpdate2 := ServiceUpdate{Cluster: testCDSName + "2"}
	// RDS update for the new name.
	v2Client.r.newUpdate(rdsURL, map[string]interface{}{
		testRDSName + "2": rdsUpdate{clusterName: testCDSName + "2"},
	})

	if u, err := serviceUpdateCh.Receive(); err != nil || u != wantUpdate2 {
		t.Errorf("unexpected serviceUpdate: %v, error receiving from channel: %v", u, err)
	}
	if e, err := serviceErrCh.Receive(); err != nil || e != nil {
		t.Errorf("unexpected serviceError: %v, error receiving from channel: %v", e, err)
	}
}

// TestServiceWatchSecond covers the case where a second WatchService() gets an
// error (because only one is allowed). But the first watch still receives
// updates.
func (s) TestServiceWatchSecond(t *testing.T) {
	v2ClientCh, cleanup := overrideNewXDSV2Client()
	defer cleanup()

	c, err := New(clientOpts(testXDSServer))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer c.Close()

	v2Client := <-v2ClientCh

	serviceUpdateCh := testutils.NewChannel()
	serviceErrCh := testutils.NewChannel()
	c.WatchService(testLDSName, func(update ServiceUpdate, err error) {
		serviceUpdateCh.Send(update)
		serviceErrCh.Send(err)
	})

	wantUpdate := ServiceUpdate{Cluster: testCDSName}

	<-v2Client.addWatches[ldsURL]
	v2Client.r.newUpdate(ldsURL, map[string]interface{}{
		testLDSName: ldsUpdate{routeName: testRDSName},
	})
	<-v2Client.addWatches[rdsURL]
	v2Client.r.newUpdate(rdsURL, map[string]interface{}{
		testRDSName: rdsUpdate{clusterName: testCDSName},
	})

	if u, err := serviceUpdateCh.Receive(); err != nil || u != wantUpdate {
		t.Errorf("unexpected serviceUpdate: %v, error receiving from channel: %v", u, err)
	}
	if e, err := serviceErrCh.Receive(); err != nil || e != nil {
		t.Errorf("unexpected serviceError: %v, error receiving from channel: %v", e, err)
	}

	serviceUpdateCh2 := testutils.NewChannel()
	serviceErrCh2 := testutils.NewChannel()
	// Call WatchService() again, with the same or different name.
	c.WatchService(testLDSName, func(update ServiceUpdate, err error) {
		serviceUpdateCh2.Send(update)
		serviceErrCh2.Send(err)
	})

	if u, err := serviceUpdateCh2.Receive(); err != nil || (u != ServiceUpdate{}) {
		t.Errorf("unexpected serviceUpdate: %v, %v, want an empty update", u, err)
	}
	if e, err := serviceErrCh2.Receive(); err != nil || e == nil {
		t.Errorf("unexpected serviceError: %v, %v, want non-nil error", e, err)
	}

	// Send update again, first callback should be called, second should
	// timeout.
	v2Client.r.newUpdate(ldsURL, map[string]interface{}{
		testLDSName: ldsUpdate{routeName: testRDSName},
	})
	v2Client.r.newUpdate(rdsURL, map[string]interface{}{
		testRDSName: rdsUpdate{clusterName: testCDSName},
	})

	if u, err := serviceUpdateCh.Receive(); err != nil || u != wantUpdate {
		t.Errorf("unexpected serviceUpdate: %v, error receiving from channel: %v", u, err)
	}
	if e, err := serviceErrCh.Receive(); err != nil || e != nil {
		t.Errorf("unexpected serviceError: %v, error receiving from channel: %v", e, err)
	}

	if u, err := serviceUpdateCh2.Receive(); err == nil {
		t.Errorf("unexpected serviceUpdate: %v, %v, want channel recv timeout", u, err)
	}
	if e, err := serviceErrCh2.Receive(); err == nil {
		t.Errorf("unexpected serviceError: %v, %v, want channel recv timeout", e, err)
	}
}
