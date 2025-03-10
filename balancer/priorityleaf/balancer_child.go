package priorityleaf

import (
	"fmt"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

type subConnWrapper struct {
	parent *priorityBalancer
	addr   resolver.Address

	started bool
	sc      balancer.SubConn
	state   balancer.State
	// This is set when the subconn reports TransientFailure, and unset when it
	// reports Ready.
	//
	// This is used to decide if this subconn is in a good state when it's in
	// Idle/Connecting.
	// - If it was in Ready, it's in a good state and we should give it time to
	// connect (don't switch to the lower priority).
	// - If it was in TransientFailure, it's in a bad state (fail-reconnecting) and
	// we should switch to the lower priority.
	reportedTF bool
}

func newSubConnWrapper(parent *priorityBalancer, addr resolver.Address) *subConnWrapper {
	scWrapper := &subConnWrapper{
		parent: parent,
		addr:   addr,
	}
	return scWrapper
}

// start creates the subconn if it's not already started.
func (scw *subConnWrapper) start() {
	if scw.started {
		return
	}
	// NewSubConn is marked as Deprecated but not replaced, and the message reads
	// like a warning.
	sc, err := scw.parent.cc.NewSubConn([]resolver.Address{scw.addr}, balancer.NewSubConnOptions{ //nolint:staticcheck
		StateListener: scw.handleSubConnStateCallback,
	})
	if err != nil {
		// This should never happen, unless the clientConn is being shut
		// down. Do nothing, the LB policy will be closed soon.
		fmt.Printf("Failed to create a subConn for address %v: %v\n", scw.addr.String(), err)
		return
	}
	sc.Connect()
	scw.sc = sc
	scw.started = true
}

// stop shuts down the subconn and resets the state.
func (scw *subConnWrapper) stop() {
	if !scw.started {
		return
	}
	if scw.sc != nil {
		scw.sc.Shutdown()
		scw.sc = nil
	}
	scw.state = balancer.State{}
	scw.reportedTF = false
	scw.started = false
}

func (scw *subConnWrapper) processNewState(state balancer.SubConnState) {
	switch state.ConnectivityState {
	case connectivity.Ready:
		scw.reportedTF = false
		scw.state = balancer.State{
			ConnectivityState: connectivity.Ready,
			Picker:            &picker{result: balancer.PickResult{SubConn: scw.sc}},
		}
		scw.parent.syncPriority(scw.addr)
	case connectivity.TransientFailure:
		scw.reportedTF = true
		scw.state = balancer.State{
			ConnectivityState: connectivity.TransientFailure,
			Picker:            &picker{err: state.ConnectionError},
		}
		scw.parent.syncPriority(scw.addr)
	case connectivity.Idle:
		scw.sc.Connect()
		scw.state = balancer.State{
			ConnectivityState: connectivity.Idle,
			Picker:            &picker{err: balancer.ErrNoSubConnAvailable},
		}
		scw.parent.syncPriority(scw.addr)
	case connectivity.Connecting:
		scw.state = balancer.State{
			ConnectivityState: connectivity.Connecting,
			Picker:            &picker{err: balancer.ErrNoSubConnAvailable},
		}
		scw.parent.syncPriority(scw.addr)
	case connectivity.Shutdown:
	default:
	}
}

// Callback given to the clientconn for subconn health check state change.
func (scw *subConnWrapper) handleHealthCheckStateCallback(state balancer.SubConnState) {
	fmt.Println("received healthcheck subconn state:", state.ConnectivityState)
	scw.processNewState(state)
}

// Callback given to the clientconn for subconn state change.
func (scw *subConnWrapper) handleSubConnStateCallback(state balancer.SubConnState) {
	fmt.Println("received subconn state:", state.ConnectivityState)
	if state.ConnectivityState == connectivity.Ready {
		fmt.Println("skipping subconn state:", state.ConnectivityState)
		// This was a TCP ready state. We still need to do health check.
		scw.sc.RegisterHealthListener(scw.handleHealthCheckStateCallback)
		return
	}

	fmt.Println("not skipping subconn state:", state.ConnectivityState)
	scw.processNewState(state)
}

type picker struct {
	result balancer.PickResult
	err    error
}

func (p *picker) Pick(balancer.PickInfo) (balancer.PickResult, error) {
	return p.result, p.err
}
