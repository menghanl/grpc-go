package client

import (
	"testing"
	"time"

	"google.golang.org/grpc/xds/internal/testutils"
)

const (
	testXDSServer   = "xds-server"
	chanRecvTimeout = 100 * time.Millisecond

	testClusterName = "test-cluster"
	testServiceName = "test-service"
)

// TestCDSWatch covers the case where an update is received after a watch().
func TestCDSWatch(t *testing.T) {
	v2ClientCh, cleanup := overrideNewV2Client()
	defer cleanup()

	c, err := New(clientOpts(testXDSServer))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer c.Close()

	v2Client := <-v2ClientCh

	cdsUpdateCh := testutils.NewChannel()
	cdsErrCh := testutils.NewChannel()
	cancelWatch := c.WatchCluster(testClusterName, func(update CDSUpdate, err error) {
		cdsUpdateCh.Send(update)
		cdsErrCh.Send(err)
	})

	wantUpdate := CDSUpdate{ServiceName: testServiceName}
	v2Client.r.newUpdate(cdsURL, map[string]interface{}{
		testClusterName: wantUpdate,
	})

	if u, err := cdsUpdateCh.Receive(); err != nil || u != wantUpdate {
		t.Errorf("unexpected cdsUpdate: %v, error receiving from channel: %v", u, err)
	}
	if e, err := cdsErrCh.Receive(); err != nil || e != nil {
		t.Errorf("unexpected cdsError: %v, error receiving from channel: %v", e, err)
	}

	// Another update for a different resource name.
	v2Client.r.newUpdate(cdsURL, map[string]interface{}{
		"randomName": CDSUpdate{},
	})

	if u, err := cdsUpdateCh.TimedReceive(chanRecvTimeout); err != testutils.ErrRecvTimeout {
		t.Errorf("unexpected cdsUpdate: %v, %v, want channel recv timeout", u, err)
	}
	if e, err := cdsErrCh.TimedReceive(chanRecvTimeout); err != testutils.ErrRecvTimeout {
		t.Errorf("unexpected cdsError: %v, %v, want channel recv timeout", e, err)
	}

	// Cancel watch, and send update again.
	cancelWatch()
	v2Client.r.newUpdate(cdsURL, map[string]interface{}{
		testClusterName: wantUpdate,
	})

	if u, err := cdsUpdateCh.TimedReceive(chanRecvTimeout); err != testutils.ErrRecvTimeout {
		t.Errorf("unexpected cdsUpdate: %v, %v, want channel recv timeout", u, err)
	}
	if e, err := cdsErrCh.TimedReceive(chanRecvTimeout); err != testutils.ErrRecvTimeout {
		t.Errorf("unexpected cdsError: %v, %v, want channel recv timeout", e, err)
	}
}

// TestCDSTwoWatchSameResourceName covers the case where an update is received
// after two watch() for the same resource name.
func TestCDSTwoWatchSameResourceName(t *testing.T) {
	v2ClientCh, cleanup := overrideNewV2Client()
	defer cleanup()

	c, err := New(clientOpts(testXDSServer))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer c.Close()

	v2Client := <-v2ClientCh

	var cdsUpdateChs, cdsErrChs []*testutils.Channel
	const count = 2

	var cancelLastWatch func()

	for i := 0; i < count; i++ {
		cdsUpdateCh := testutils.NewChannel()
		cdsUpdateChs = append(cdsUpdateChs, cdsUpdateCh)
		cdsErrCh := testutils.NewChannel()
		cdsErrChs = append(cdsErrChs, cdsErrCh)
		cancelLastWatch = c.WatchCluster(testClusterName, func(update CDSUpdate, err error) {
			cdsUpdateCh.Send(update)
			cdsErrCh.Send(err)
		})
	}

	wantUpdate := CDSUpdate{ServiceName: testServiceName}
	v2Client.r.newUpdate(cdsURL, map[string]interface{}{
		testClusterName: wantUpdate,
	})

	for i := 0; i < count; i++ {
		if u, err := cdsUpdateChs[i].Receive(); err != nil || u != wantUpdate {
			t.Errorf("i=%v, unexpected cdsUpdate: %v, error receiving from channel: %v", i, u, err)
		}
		if e, err := cdsErrChs[i].Receive(); err != nil || e != nil {
			t.Errorf("i=%v, unexpected cdsError: %v, error receiving from channel: %v", i, e, err)
		}
	}

	// Cancel the last watch, and send update again.
	cancelLastWatch()
	v2Client.r.newUpdate(cdsURL, map[string]interface{}{
		testClusterName: wantUpdate,
	})

	for i := 0; i < count-1; i++ {
		if u, err := cdsUpdateChs[i].Receive(); err != nil || u != wantUpdate {
			t.Errorf("i=%v, unexpected cdsUpdate: %v, error receiving from channel: %v", i, u, err)
		}
		if e, err := cdsErrChs[i].Receive(); err != nil || e != nil {
			t.Errorf("i=%v, unexpected cdsError: %v, error receiving from channel: %v", i, e, err)
		}
	}

	if u, err := cdsUpdateChs[count-1].TimedReceive(chanRecvTimeout); err != testutils.ErrRecvTimeout {
		t.Errorf("unexpected cdsUpdate: %v, %v, want channel recv timeout", u, err)
	}
	if e, err := cdsErrChs[count-1].TimedReceive(chanRecvTimeout); err != testutils.ErrRecvTimeout {
		t.Errorf("unexpected cdsError: %v, %v, want channel recv timeout", e, err)
	}
}

// TestCDSThreeWatchDifferentResourceName covers the case where an update is
// received after three watch() for different resource names.
func TestCDSThreeWatchDifferentResourceName(t *testing.T) {
	v2ClientCh, cleanup := overrideNewV2Client()
	defer cleanup()

	c, err := New(clientOpts(testXDSServer))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer c.Close()

	v2Client := <-v2ClientCh

	var cdsUpdateChs, cdsErrChs []*testutils.Channel
	const count = 2

	// Two watches for the same name.
	for i := 0; i < count; i++ {
		cdsUpdateCh := testutils.NewChannel()
		cdsUpdateChs = append(cdsUpdateChs, cdsUpdateCh)
		cdsErrCh := testutils.NewChannel()
		cdsErrChs = append(cdsErrChs, cdsErrCh)
		c.WatchCluster(testClusterName+"1", func(update CDSUpdate, err error) {
			cdsUpdateCh.Send(update)
			cdsErrCh.Send(err)
		})
	}

	// Third watch for a different name.
	cdsUpdateCh2 := testutils.NewChannel()
	cdsErrCh2 := testutils.NewChannel()
	c.WatchCluster(testClusterName+"2", func(update CDSUpdate, err error) {
		cdsUpdateCh2.Send(update)
		cdsErrCh2.Send(err)
	})

	wantUpdate1 := CDSUpdate{ServiceName: testServiceName + "1"}
	wantUpdate2 := CDSUpdate{ServiceName: testServiceName + "2"}
	v2Client.r.newUpdate(cdsURL, map[string]interface{}{
		testClusterName + "1": wantUpdate1,
		testClusterName + "2": wantUpdate2,
	})

	for i := 0; i < count; i++ {
		if u, err := cdsUpdateChs[i].Receive(); err != nil || u != wantUpdate1 {
			t.Errorf("i=%v, unexpected cdsUpdate: %v, error receiving from channel: %v", i, u, err)
		}
		if e, err := cdsErrChs[i].Receive(); err != nil || e != nil {
			t.Errorf("i=%v, unexpected cdsError: %v, error receiving from channel: %v", i, e, err)
		}
	}

	if u, err := cdsUpdateCh2.Receive(); err != nil || u != wantUpdate2 {
		t.Errorf("unexpected cdsUpdate: %v, error receiving from channel: %v", u, err)
	}
	if e, err := cdsErrCh2.Receive(); err != nil || e != nil {
		t.Errorf("unexpected cdsError: %v, error receiving from channel: %v", e, err)
	}
}

// TestCDSWatchAfterCache covers the case where watch is called after the update
// is in cache.
func TestCDSWatchAfterCache(t *testing.T) {
	v2ClientCh, cleanup := overrideNewV2Client()
	defer cleanup()

	c, err := New(clientOpts(testXDSServer))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer c.Close()

	v2Client := <-v2ClientCh

	cdsUpdateCh := testutils.NewChannel()
	cdsErrCh := testutils.NewChannel()
	c.WatchCluster(testClusterName, func(update CDSUpdate, err error) {
		cdsUpdateCh.Send(update)
		cdsErrCh.Send(err)
	})

	wantUpdate := CDSUpdate{ServiceName: testServiceName}
	v2Client.r.newUpdate(cdsURL, map[string]interface{}{
		testClusterName: wantUpdate,
	})

	if u, err := cdsUpdateCh.Receive(); err != nil || u != wantUpdate {
		t.Errorf("unexpected cdsUpdate: %v, error receiving from channel: %v", u, err)
	}
	if e, err := cdsErrCh.Receive(); err != nil || e != nil {
		t.Errorf("unexpected cdsError: %v, error receiving from channel: %v", e, err)
	}

	// Another watch for the resource in cache.
	cdsUpdateCh2 := testutils.NewChannel()
	cdsErrCh2 := testutils.NewChannel()
	c.WatchCluster(testClusterName, func(update CDSUpdate, err error) {
		cdsUpdateCh2.Send(update)
		cdsErrCh2.Send(err)
	})

	// New watch should receives the update.
	if u, err := cdsUpdateCh2.Receive(); err != nil || u != wantUpdate {
		t.Errorf("unexpected cdsUpdate: %v, error receiving from channel: %v", u, err)
	}
	if e, err := cdsErrCh2.Receive(); err != nil || e != nil {
		t.Errorf("unexpected cdsError: %v, error receiving from channel: %v", e, err)
	}

	// Old watch should see nothing.
	if u, err := cdsUpdateCh.TimedReceive(chanRecvTimeout); err != testutils.ErrRecvTimeout {
		t.Errorf("unexpected cdsUpdate: %v, %v, want channel recv timeout", u, err)
	}
	if e, err := cdsErrCh.TimedReceive(chanRecvTimeout); err != testutils.ErrRecvTimeout {
		t.Errorf("unexpected cdsError: %v, %v, want channel recv timeout", e, err)
	}
}

// Watch with cache inline call watch() back
func TestWatchCallAnotherWatch(t *testing.T) {
	v2ClientCh, cleanup := overrideNewV2Client()
	defer cleanup()

	c, err := New(clientOpts(testXDSServer))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer c.Close()

	v2Client := <-v2ClientCh

	cdsUpdateCh := testutils.NewChannel()
	cdsErrCh := testutils.NewChannel()
	c.WatchCluster(testClusterName, func(update CDSUpdate, err error) {
		cdsUpdateCh.Send(update)
		cdsErrCh.Send(err)
		// Calls another watch inline, to ensure there's deadlock.
		c.WatchCluster("another-random-name", func(CDSUpdate, error) {})
	})

	wantUpdate := CDSUpdate{ServiceName: testServiceName}
	v2Client.r.newUpdate(cdsURL, map[string]interface{}{
		testClusterName: wantUpdate,
	})

	if u, err := cdsUpdateCh.Receive(); err != nil || u != wantUpdate {
		t.Errorf("unexpected cdsUpdate: %v, error receiving from channel: %v", u, err)
	}
	if e, err := cdsErrCh.Receive(); err != nil || e != nil {
		t.Errorf("unexpected cdsError: %v, error receiving from channel: %v", e, err)
	}

	wantUpdate2 := CDSUpdate{ServiceName: testServiceName + "2"}
	v2Client.r.newUpdate(cdsURL, map[string]interface{}{
		testClusterName: wantUpdate2,
	})

	if u, err := cdsUpdateCh.Receive(); err != nil || u != wantUpdate2 {
		t.Errorf("unexpected cdsUpdate: %v, error receiving from channel: %v", u, err)
	}
	if e, err := cdsErrCh.Receive(); err != nil || e != nil {
		t.Errorf("unexpected cdsError: %v, error receiving from channel: %v", e, err)
	}
}
