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
	"fmt"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

// scStateUpdate contains the subConn and the new state it changed to.
type scStateUpdate struct {
	sc    balancer.SubConn
	state connectivity.State
}

// scStateUpdateBuffer is an unbounded channel for scStateChangeTuple.
// TODO make a general purpose buffer that uses interface{}.
type scStateUpdateBuffer struct {
	c       chan *scStateUpdate
	mu      sync.Mutex
	backlog []*scStateUpdate
}

func newSCStateUpdateBuffer() *scStateUpdateBuffer {
	return &scStateUpdateBuffer{
		c: make(chan *scStateUpdate, 1),
	}
}

func (b *scStateUpdateBuffer) put(t *scStateUpdate) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.backlog) == 0 {
		select {
		case b.c <- t:
			return
		default:
		}
	}
	b.backlog = append(b.backlog, t)
}

func (b *scStateUpdateBuffer) load() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.backlog) > 0 {
		select {
		case b.c <- b.backlog[0]:
			b.backlog[0] = nil
			b.backlog = b.backlog[1:]
		default:
		}
	}
}

// get returns the channel that the scStateUpdate will be sent to.
//
// Upon receiving, the caller should call load to send another
// scStateChangeTuple onto the channel if there is any.
func (b *scStateUpdateBuffer) get() <-chan *scStateUpdate {
	return b.c
}

// BalancerManager manages balancers. It can be used by ClientConn, or any
// parent balancer that needs sub-balancers.
//
// The purpose of balancer manager is a util for using balancer. It guarantees
// that after closing, no updates from balancers will be processed, and no more
// updates will be sent to the balancer.
//
// The way to do is to run all balancer methods in a goroutine. As opposite to
// holding a mutex, which could cause deadlock.
type BalancerManager struct {
	cc               balancer.ClientConn
	balancer         balancer.Balancer
	stateChangeQueue *scStateUpdateBuffer
	ccUpdateCh       chan *balancer.ClientConnState
	done             chan struct{}

	incomingMu          sync.Mutex // Mutex for all incoming calls from the balancer.
	subConns            map[balancer.SubConn]struct{}
	subConnsToBeRemoved map[balancer.SubConn]struct{}
}

// NewBalancerManager creates a new BalancerManager.
func NewBalancerManager(cc balancer.ClientConn, b balancer.Builder, bopts balancer.BuildOptions) *BalancerManager {
	ccb := &BalancerManager{
		cc:               cc,
		stateChangeQueue: newSCStateUpdateBuffer(),
		ccUpdateCh:       make(chan *balancer.ClientConnState, 1),
		done:             make(chan struct{}),
		subConns:         make(map[balancer.SubConn]struct{}),
	}
	go ccb.watcher()
	ccb.balancer = b.Build(ccb, bopts)
	return ccb
}

// watcher calls balancer functions sequentially, so the balancer can be
// implemented lock-free.
//
// It also helps to make sure no updates will be sent to the balancer after
// Close().
func (ccb *BalancerManager) watcher() {
	for {
		select {
		case t := <-ccb.stateChangeQueue.get():
			ccb.stateChangeQueue.load()
			select {
			case <-ccb.done:
				ccb.balancer.Close()
				return
			default:
			}
			if ub, ok := ccb.balancer.(balancer.V2Balancer); ok {
				ub.UpdateSubConnState(t.sc, balancer.SubConnState{ConnectivityState: t.state})
			} else {
				ccb.balancer.HandleSubConnStateChange(t.sc, t.state)
			}
		case s := <-ccb.ccUpdateCh:
			select {
			case <-ccb.done:
				ccb.balancer.Close()
				return
			default:
			}
			if ub, ok := ccb.balancer.(balancer.V2Balancer); ok {
				ub.UpdateClientConnState(*s)
			} else {
				ccb.balancer.HandleResolvedAddrs(s.ResolverState.Addresses, nil)
			}
		case <-ccb.done:
		}

		select {
		case <-ccb.done:
			ccb.balancer.Close()
			ccb.incomingMu.Lock()
			scs := ccb.subConnsToBeRemoved
			ccb.subConnsToBeRemoved = nil
			ccb.incomingMu.Unlock()
			for sc := range scs {
				ccb.cc.RemoveSubConn(sc)
			}
			return
		default:
		}
	}
}

// Close closes this balancer manager and the underlying balancer. Note that
// it's done asynchronously.
//
// It's guaranteed that after Close() takes effects, the balancer won't get any
// updates from ClientConn, and ClientConn also won't updates from the balancer.
func (ccb *BalancerManager) Close() {
	close(ccb.done)
	ccb.incomingMu.Lock()
	// We don't remove the SubConns immediately when closing. They are kept in a
	// set, and will be removed after balancer is closed.
	ccb.subConnsToBeRemoved = ccb.subConns
	ccb.subConns = nil
	ccb.incomingMu.Unlock()
}

// Outgoing to balancers.

// HandleSubConnStateChange forwards sc's state change to balancer.
func (ccb *BalancerManager) HandleSubConnStateChange(sc balancer.SubConn, s connectivity.State) {
	ccb.stateChangeQueue.put(&scStateUpdate{
		sc:    sc,
		state: s,
	})
}

// UpdateClientConnState forwards cc state (addresses and service config) to the
// balancer.
func (ccb *BalancerManager) UpdateClientConnState(ccs *balancer.ClientConnState) {
	select {
	case <-ccb.ccUpdateCh:
	default:
	}
	ccb.ccUpdateCh <- ccs
}

// Incoming from balancer.

// NewSubConn creates a new subconn.
func (ccb *BalancerManager) NewSubConn(addrs []resolver.Address, opts balancer.NewSubConnOptions) (balancer.SubConn, error) {
	ccb.incomingMu.Lock()
	defer ccb.incomingMu.Unlock()
	if ccb.subConns == nil {
		return nil, fmt.Errorf("BalancerManager: NewSubConn called after balancer is closed")
	}
	sc, err := ccb.cc.NewSubConn(addrs, opts)
	if err != nil {
		return nil, err
	}
	ccb.subConns[sc] = struct{}{}
	return sc, nil

}

// RemoveSubConn removes the subconn.
func (ccb *BalancerManager) RemoveSubConn(sc balancer.SubConn) {
	ccb.incomingMu.Lock()
	defer ccb.incomingMu.Unlock()
	if ccb.subConns == nil {
		return
	}
	delete(ccb.subConns, sc)
	ccb.cc.RemoveSubConn(sc)
}

// UpdateBalancerState updates state and picker.
func (ccb *BalancerManager) UpdateBalancerState(s connectivity.State, p balancer.Picker) {
	ccb.incomingMu.Lock()
	defer ccb.incomingMu.Unlock()
	if ccb.subConns == nil {
		return
	}
	ccb.cc.UpdateBalancerState(s, p)
}

// ResolveNow forwards resolve signal to resolver.
func (ccb *BalancerManager) ResolveNow(o resolver.ResolveNowOption) {
	ccb.cc.ResolveNow(o)
}

// Target returns the ClientConn's target.
func (ccb *BalancerManager) Target() string {
	return ccb.cc.Target()
}
