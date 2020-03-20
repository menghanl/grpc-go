package client

import (
	"testing"

	"google.golang.org/grpc/xds/internal/testutils"
)

// TestServiceWatch covers the case where an update is received after a watch.
func TestServiceWatch(t *testing.T) {
	v2ClientCh, cleanup := overrideNewV2Client()
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

// TestServiceWatchSecond covers the case where a second WatchService() gets an
// error (because only one is allowed). But the first watch still receives
// updates.

// TestServiceWatchLDSUpdate covers the case that after first LDS and first RDS
// response, the second LDS response trigger an new RDS watch, and an update of
// the old RDS watch doesn't trigger update to service callback.
