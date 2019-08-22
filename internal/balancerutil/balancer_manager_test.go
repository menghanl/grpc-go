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
 */

package balancerutil

import (
	"errors"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

func curMethodName() string {
	pc := make([]uintptr, 10)
	runtime.Callers(2, pc)
	ss := strings.Split(runtime.FuncForPC(pc[0]).Name(), ".")
	return ss[len(ss)-1]
}

func invoke(it interface{}, name string) {
	if name == "" {
		return
	}
	method := reflect.ValueOf(it).MethodByName(name)
	methodType := method.Type()
	var in []reflect.Value
	if numIn := methodType.NumIn(); numIn > 0 {
		in = make([]reflect.Value, 0, numIn)
		for i := 0; i < numIn; i++ {
			in = append(in, reflect.New(methodType.In(i)).Elem())
		}
	}
	method.Call(in)
}

type testBalancerBuilder struct {
	cc balancer.ClientConn

	buildDirectCallMethodName        string
	subConnStateDirectCallMethodName string
	addrsDirectCallMethodName        string

	// This is to test no updates to balancer after balancer.Close(). An empty
	// struct is send to ch every time balancer gets an update. ch will be
	// closed in Close(). Further send to ch will cause panic.
	ch chan struct{}
}

func (b *testBalancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	b.cc = cc
	invoke(cc, b.buildDirectCallMethodName)
	return b
}

func (*testBalancerBuilder) Name() string { return "" }

func (b *testBalancerBuilder) HandleSubConnStateChange(sc balancer.SubConn, state connectivity.State) {
	invoke(b.cc, b.subConnStateDirectCallMethodName)
	select {
	case b.ch <- struct{}{}:
	default:
	}
}

func (b *testBalancerBuilder) HandleResolvedAddrs([]resolver.Address, error) {
	invoke(b.cc, b.addrsDirectCallMethodName)
	select {
	case b.ch <- struct{}{}:
	default:
	}
}

func (b *testBalancerBuilder) Close() {
	close(b.ch)
}

type testClientConn struct {
	mu         sync.Mutex
	totalCount int
	callCount  map[string]int
}

func newTestClientConn() *testClientConn {
	return &testClientConn{
		callCount: make(map[string]int),
	}
}

func (cc *testClientConn) NewSubConn([]resolver.Address, balancer.NewSubConnOptions) (balancer.SubConn, error) {
	cc.mu.Lock()
	cc.totalCount++
	cc.callCount[curMethodName()]++
	cc.mu.Unlock()
	return nil, errors.New("")
}
func (cc *testClientConn) RemoveSubConn(balancer.SubConn) {
	cc.mu.Lock()
	cc.totalCount++
	cc.callCount[curMethodName()]++
	cc.mu.Unlock()
}
func (cc *testClientConn) UpdateBalancerState(s connectivity.State, p balancer.Picker) {
	cc.mu.Lock()
	cc.totalCount++
	cc.callCount[curMethodName()]++
	cc.mu.Unlock()
}
func (cc *testClientConn) ResolveNow(resolver.ResolveNowOption) {
	cc.mu.Lock()
	cc.totalCount++
	cc.callCount[curMethodName()]++
	cc.mu.Unlock()
}
func (cc *testClientConn) Target() string {
	cc.mu.Lock()
	cc.totalCount++
	cc.callCount[curMethodName()]++
	cc.mu.Unlock()
	return ""
}

// Tests with different balancers/builders where a method calls back into
// ClientConn inline, to make sure there's no deadlock.
//
// Balancer/Builder is configured to call a ClientConn method (by name using
// reflect) inline in it's methods. The test will call balancer manager to
// eventually call balancer methods. Because the balancer methods call back to
// ClientConn inline, if a mutex is held that blocks the callback, the program
// will deadlock, the counting at the end of the test will error.

func getMethodNames(it interface{}) []string {
	tp := reflect.TypeOf(it)
	names := make([]string, 0, tp.NumMethod())
	for i := 0; i < tp.NumMethod(); i++ {
		names = append(names, tp.Method(i).Name)
	}
	return names
}

func Test_Build_CallsClientConn(t *testing.T) {
	cc := newTestClientConn()
	tests := getMethodNames(cc)

	for _, tt := range tests {
		m := NewBalancerManager(
			cc,
			&testBalancerBuilder{
				buildDirectCallMethodName: tt,
				ch:                        make(chan struct{}),
			},
			balancer.BuildOptions{},
		)
		defer m.Close()
	}

	for _, n := range tests {
		if got := cc.callCount[n]; got != 1 {
			t.Errorf("%s is called %d times, want 1", n, got)
		}
	}
}

func Test_HandleSCStateChange_CallsClientConn(t *testing.T) {
	cc := newTestClientConn()
	tests := getMethodNames(cc)

	for _, tt := range tests {
		m := NewBalancerManager(
			cc,
			&testBalancerBuilder{
				subConnStateDirectCallMethodName: tt,
				ch:                               make(chan struct{}),
			},
			balancer.BuildOptions{},
		)
		defer m.Close()
		m.HandleSubConnStateChange(nil, 0)
	}

	for i := 0; i < 1000; i++ {
		cc.mu.Lock()
		if cc.totalCount >= len(tests) {
			for _, n := range tests {
				if got := cc.callCount[n]; got != 1 {
					t.Errorf("%s is called %d times, want 1", n, got)
				}
			}
			return
		}
		cc.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
}

func Test_HandleAddress_CallsClientConn(t *testing.T) {
	cc := newTestClientConn()
	tests := getMethodNames(cc)

	for _, tt := range tests {
		m := NewBalancerManager(
			cc,
			&testBalancerBuilder{
				addrsDirectCallMethodName: tt,
				ch:                        make(chan struct{}),
			},
			balancer.BuildOptions{},
		)
		defer m.Close()
		m.UpdateClientConnState(&balancer.ClientConnState{})
	}

	for i := 0; i < 1000; i++ {
		cc.mu.Lock()
		if cc.totalCount >= len(tests) {
			for _, n := range tests {
				if got := cc.callCount[n]; got != 1 {
					t.Errorf("%s is called %d times, want 1", n, got)
				}
			}
			return
		}
		cc.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
}

// No update will be sent to balancer after balancer.Close()
func Test_NoOutgoingUpdatesToBalancerAfterClose(t *testing.T) {
	tbb := &testBalancerBuilder{
		ch: make(chan struct{}),
	}
	m := NewBalancerManager(
		newTestClientConn(),
		tbb,
		balancer.BuildOptions{},
	)

	for i := 0; i < 100; i++ {
		go func() {
			m.UpdateClientConnState(&balancer.ClientConnState{})
		}()
	}
	m.Close()
	for i := 0; i < 100; i++ {
		go func() {
			m.HandleSubConnStateChange(nil, 0)
		}()
	}

	for {
		if _, ok := <-tbb.ch; !ok {
			// Wait for 1 second to make sure no update is sent to balancer
			// after Close().
			time.Sleep(time.Second)
			return
		}
	}
}

// No update will be received from balancer after balancerManager.Close()
func Test_NoIncomingUpdatesFromBalancerAfterClose(t *testing.T) {
	cc := newTestClientConn()
	m := NewBalancerManager(
		cc,
		&testBalancerBuilder{
			ch: make(chan struct{}),
		},
		balancer.BuildOptions{},
	)

	for i := 0; i < 100; i++ {
		go func() {
			m.UpdateBalancerState(0, nil)
			m.RemoveSubConn(nil)
			m.NewSubConn(nil, balancer.NewSubConnOptions{})
		}()
	}
	cc.mu.Lock()
	want := cc.totalCount
	m.Close()
	cc.mu.Unlock()
	for i := 0; i < 100; i++ {
		go func() {
			m.UpdateBalancerState(0, nil)
			m.RemoveSubConn(nil)
			m.NewSubConn(nil, balancer.NewSubConnOptions{})
		}()
	}

	time.Sleep(time.Second)
	cc.mu.Lock()
	got := cc.totalCount
	cc.mu.Unlock()

	if got != want {
		t.Fatalf("ClientConn was called %d times before Close, %d times after Close, want 0", got, got-want)
	}
}
