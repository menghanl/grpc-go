package client

// // TestCDSWatchExpiryTimer tests the case where the client does not receive an
// // CDS response for the request that it sends out. We want the watch callback
// // to be invoked with an error once the watchExpiryTimer fires.
// func (s) TestCDSWatchExpiryTimer(t *testing.T) {
// 	oldWatchExpiryTimeout := defaultWatchExpiryTimeout
// 	defaultWatchExpiryTimeout = 500 * time.Millisecond
// 	defer func() {
// 		defaultWatchExpiryTimeout = oldWatchExpiryTimeout
// 	}()
//
// 	fakeServer, cc, cleanup := startServerAndGetCC(t)
// 	defer cleanup()
//
// 	v2c := newV2Client(cc, goodNodeProto, func(int) time.Duration { return 0 }, nil)
// 	defer v2c.close()
// 	t.Log("Started xds v2Client...")
//
// 	callbackCh := testutils.NewChannel()
// 	v2c.watchCDS(clusterName1, func(u ClusterUpdate, err error) {
// 		t.Logf("Received callback with ClusterUpdate {%+v} and error {%v}", u, err)
// 		if u.ServiceName != "" {
// 			callbackCh.Send(fmt.Errorf("received serviceName %v in cdsCallback, wanted empty string", u.ServiceName))
// 		}
// 		if err == nil {
// 			callbackCh.Send(errors.New("received nil error in cdsCallback"))
// 		}
// 		callbackCh.Send(nil)
// 	})
//
// 	// Wait till the request makes it to the fakeServer. This ensures that
// 	// the watch request has been processed by the v2Client.
// 	if _, err := fakeServer.XDSRequestChan.Receive(); err != nil {
// 		t.Fatalf("Timeout expired when expecting an CDS request")
// 	}
// 	waitForNilErr(t, callbackCh)
// }
