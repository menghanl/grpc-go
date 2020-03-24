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

// // TestLDSWatchExpiryTimer tests the case where the client does not receive an
// // LDS response for the request that it sends out. We want the watch callback
// // to be invoked with an error once the watchExpiryTimer fires.
// func (s) TestLDSWatchExpiryTimer(t *testing.T) {
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
//
// 	callbackCh := testutils.NewChannel()
// 	v2c.watchLDS(goodLDSTarget1, func(u ldsUpdate, err error) {
// 		t.Logf("in v2c.watchLDS callback, ldsUpdate: %+v, err: %v", u, err)
// 		if u.routeName != "" {
// 			callbackCh.Send(fmt.Errorf("received routeName %v in ldsCallback, wanted empty string", u.routeName))
// 		}
// 		if err == nil {
// 			callbackCh.Send(errors.New("received nil error in ldsCallback"))
// 		}
// 		callbackCh.Send(nil)
// 	})
//
// 	// Wait till the request makes it to the fakeServer. This ensures that
// 	// the watch request has been processed by the v2Client.
// 	if _, err := fakeServer.XDSRequestChan.Receive(); err != nil {
// 		t.Fatalf("Timeout expired when expecting an LDS request")
// 	}
// // 	waitForNilErr(t, callbackCh)
// // }
//
// // TestRDSWatchExpiryTimer tests the case where the client does not receive an
// // RDS response for the request that it sends out. We want the watch callback
// // to be invoked with an error once the watchExpiryTimer fires.
// func (s) TestRDSWatchExpiryTimer(t *testing.T) {
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
// 	doLDS(t, v2c)
//
// 	callbackCh := testutils.NewChannel()
// 	v2c.watchRDS(goodRouteName1, func(u rdsUpdate, err error) {
// 		t.Logf("Received callback with rdsUpdate {%+v} and error {%v}", u, err)
// 		if u.clusterName != "" {
// 			callbackCh.Send(fmt.Errorf("received clusterName %v in rdsCallback, wanted empty string", u.clusterName))
// 		}
// 		if err == nil {
// 			callbackCh.Send(errors.New("received nil error in rdsCallback"))
// 		}
// 		callbackCh.Send(nil)
// 	})
//
// 	// Wait till the request makes it to the fakeServer. This ensures that
// 	// the watch request has been processed by the v2Client.
// 	if _, err := fakeServer.XDSRequestChan.Receive(); err != nil {
// 		t.Fatalf("Timeout expired when expecting an RDS request")
// 	}
// 	waitForNilErr(t, callbackCh)
// }

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
//
// func (s) TestEDSWatchExpiryTimer(t *testing.T) {
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
// 	v2c.watchEDS(goodRouteName1, func(u *EndpointsUpdate, err error) {
// 		t.Logf("Received callback with edsUpdate {%+v} and error {%v}", u, err)
// 		if u != nil {
// 			callbackCh.Send(fmt.Errorf("received EndpointsUpdate %v in edsCallback, wanted nil", u))
// 		}
// 		if err == nil {
// 			callbackCh.Send(errors.New("received nil error in edsCallback"))
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
