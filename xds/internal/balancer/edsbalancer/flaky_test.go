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

package edsbalancer

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/xds/internal/testutils"
	"google.golang.org/grpc/xds/internal/testutils/fakeclient"
	"google.golang.org/grpc/xds/internal/xdsclient"
)

func (s) TestCancelThenReadd(t *testing.T) {
	xdsC := fakeclient.NewClientWithName(testBalancerNameFooBar)
	cc := testutils.NewTestClientConn(t)

	builder := balancer.Get(edsName)
	edsB := builder.Build(cc, balancer.BuildOptions{Target: resolver.Target{Endpoint: testServiceName}})
	if edsB == nil {
		t.Fatalf("builder.Build(%s) failed and returned nil", edsName)
	}
	defer edsB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	if err := edsB.UpdateClientConnState(balancer.ClientConnState{
		ResolverState:  xdsclient.SetClient(resolver.State{}, xdsC),
		BalancerConfig: &EDSConfig{EDSServiceName: testServiceName},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := xdsC.WaitForWatchEDS(ctx); err != nil {
		t.Fatalf("xdsClient.WatchEndpoints failed with error: %v", err)
	}

	// One locality with one backend.
	clab1 := testutils.NewClusterLoadAssignmentBuilder(testClusterNames[0], nil)
	clab1.AddLocality(testSubZones[0], 1, 0, testEndpointAddrs[:1], nil)
	xdsC.InvokeWatchEDSCallback(parseEDSRespProtoForTesting(clab1.Build()), nil)

	sc1 := <-cc.NewSubConnCh
	edsB.UpdateSubConnState(sc1, balancer.SubConnState{ConnectivityState: connectivity.Connecting})
	edsB.UpdateSubConnState(sc1, balancer.SubConnState{ConnectivityState: connectivity.Ready})

	// Pick with only the first backend.
	if err := testRoundRobinPickerFromCh(cc.NewPickerCh, []balancer.SubConn{sc1}); err != nil {
		t.Fatal(err)
	}

	resourceErr := xdsclient.NewErrorf(xdsclient.ErrorTypeResourceNotFound, "edsBalancer resource not found error")
	edsB.ResolverError(resourceErr)
	if err := xdsC.WaitForCancelEDSWatch(ctx); err != nil {
		t.Fatalf("want watch to be canceled, waitForCancel failed: %v", err)
	}
	if err := testErrPickerFromCh(cc.NewPickerCh, errAllPrioritiesRemoved); err != nil {
		t.Fatal(err)
	}

	// An update with the same service name should trigger a new watch, because
	// the previous watch was canceled.
	if err := edsB.UpdateClientConnState(balancer.ClientConnState{
		ResolverState:  xdsclient.SetClient(resolver.State{}, xdsC),
		BalancerConfig: &EDSConfig{EDSServiceName: testServiceName},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := xdsC.WaitForWatchEDS(ctx); err != nil {
		t.Fatalf("xdsClient.WatchEndpoints failed with error: %v", err)
	}

	// Send the same EDS update again, should connect.
	xdsC.InvokeWatchEDSCallback(parseEDSRespProtoForTesting(clab1.Build()), nil)

	sc2 := <-cc.NewSubConnCh
	edsB.UpdateSubConnState(sc2, balancer.SubConnState{ConnectivityState: connectivity.Connecting})
	edsB.UpdateSubConnState(sc2, balancer.SubConnState{ConnectivityState: connectivity.Ready})

	// Pick with only the first backend.
	if err := testRoundRobinPickerFromCh(cc.NewPickerCh, []balancer.SubConn{sc2}); err != nil {
		t.Fatal(err)
	}
}

// testPickerFromCh receives pickers from the channel, and check if their
// behaviors are as expected (that the given function returns nil err).
//
// It returns nil if one picker has the correct behavior.
//
// It returns error when there's no picker from channel after 1 second timeout,
// and the error returned is the mismatch error from the previous picker.
func testPickerFromCh(ch chan balancer.Picker, f func(balancer.Picker) error) error {
	var (
		p   balancer.Picker
		err error
	)
	for {
		select {
		case p = <-ch:
		case <-time.After(defaultTestTimeout):
			return fmt.Errorf("timeout waiting for picker with expected behavior, error from previous picker: %v", err)
		}

		err = f(p)
		if err == nil {
			return nil
		}
	}
}

// testRoundRobinPickerFromCh receives pickers from the channel, and check if
// their behaviors are round-robin of want.
//
// It returns nil if one picker has the correct behavior.
//
// It returns error when there's no picker from channel after 1 second timeout,
// and the error returned is the mismatch error from the previous picker.
func testRoundRobinPickerFromCh(ch chan balancer.Picker, want []balancer.SubConn) error {
	return testPickerFromCh(ch, func(p balancer.Picker) error {
		return testutils.IsRoundRobin(want, subConnFromPicker(p))
	})
}

// testErrPickerFromCh receives pickers from the channel, and check if they
// return the wanted error.
//
// It returns nil if one picker has the correct behavior.
//
// It returns error when there's no picker from channel after 1 second timeout,
// and the error returned is the mismatch error from the previous picker.
func testErrPickerFromCh(ch chan balancer.Picker, want error) error {
	return testPickerFromCh(ch, func(p balancer.Picker) error {
		for i := 0; i < 5; i++ {
			_, err := p.Pick(balancer.PickInfo{})
			if !reflect.DeepEqual(err, want) {
				return fmt.Errorf("picker.Pick, got err %q, want err %q", err, want)
			}
		}
		return nil
	})
}
