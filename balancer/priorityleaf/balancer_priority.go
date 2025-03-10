package priorityleaf

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

var (
	ErrAllPrioritiesRemoved = errors.New("from priority lb, no address is provided, all addresses are removed")
	ErrMultiAddressEndpoint = errors.New("from priority lb, endpoint with multiple addresses is not supported")
)

// syncPriority handles priority after a config update or a subconn
// connectivity state update. It makes sure the subconn state (started or not)
// is in sync with the priorities (even in tricky cases where an address is moved
// from a priority to another).
//
// It's guaranteed that after this function returns:
//
// If some subconn is READY, it is addrInUse, and all lower priorities are
// closed.
//
// If some subconn is newly started (in Connecting/Idle and the previous state
// is not TransientFailure), it is addrInUse, and all lower priorities are
// closed.
//
// Otherwise, the lowest priority is addrInUse (none of the addr is ready, and
// the overall state is not ready).
func (b *priorityBalancer) syncPriority(addrUpdating resolver.Address) {
	for p, addr := range b.priorities {
		scItem, ok := b.subConns.Get(addr)
		if !ok {
			fmt.Printf("warning addr %v is not found in list of child policies\n", addr)
			continue
		}
		scw := scItem.(*subConnWrapper)
		if !scw.started ||
			scw.state.ConnectivityState == connectivity.Ready ||
			(scw.state.ConnectivityState == connectivity.Idle && !scw.reportedTF) ||
			(scw.state.ConnectivityState == connectivity.Connecting && !scw.reportedTF) ||
			p == len(b.priorities)-1 {

			// The for loop and the if above is to find "the first good subconn with
			// the highest priority". When the loop steps in this if, this subconn is
			// - the first one that's not already started (and everything before was bad)
			// - the first one in Ready state
			// - the first one in Idle/Connecting state and didn't transition from TransientFailure
			// - the last one in the list (nothing is good)
			//
			// We should switch to use this subconn.

			if !b.addrInUse.Equal(addr) || addrUpdating.Equal(addr) {
				// If we are switching to a different address (one example is some
				// higher priority subconn turned Ready).
				//
				// Or if we are not switching to a different address, but we just got an
				// update from the addrInUse.
				//
				// In both cases, we need to send the new state and picker to the ClientConn.
				fmt.Printf("sending balancer state up %v, childInUse, childUpdating: %v, %v\n", scw.state, b.addrInUse, addrUpdating)
				b.cc.UpdateState(scw.state)
			}

			// Deal with start()/stop() of the subconns.
			fmt.Printf("Switching to (%v, %v) in syncPriority\n", addr, p)
			b.switchToChild(scw, p)
			break
		}
	}
}

// switchToChild does the following:
// - stop all subconns with lower priorities
// - if addrInUse is not this addr
//   - set addrInUse to this
//   - if this subconn is not started, start it
//
// Note that it does NOT send the current subconn state (picker) to the parent
// ClientConn. The caller needs to send it if necessary.
//
// this can be called when
// 1. first update, start p0
// 2. an update moves a READY addr from a lower priority to higher
// 3. a high priority goes Failure, start next
func (b *priorityBalancer) switchToChild(scw *subConnWrapper, priority int) {
	// Stop lower priorities even if addrInUse is same as this. It's
	// possible this addr was moved from a priority to another.
	b.stopSubBalancersLowerThanPriority(priority)

	// If this addr is already in use, do nothing.
	//
	// This can happen:
	// - all priorities are not READY, an config update always triggers switch to
	// the lowest. In this case, the lowest subconn could still be connecting.
	// - a high priority is READY, an config update always triggers switch to
	// it.
	if b.addrInUse.Equal(scw.addr) && scw.started {
		return
	}
	b.addrInUse = scw.addr

	if !scw.started {
		scw.start()
	}
}

// Stop priorities [p+1, lowest].
func (b *priorityBalancer) stopSubBalancersLowerThanPriority(p int) {
	for i := p + 1; i < len(b.priorities); i++ {
		addr := b.priorities[i]
		scItem, ok := b.subConns.Get(addr)
		if !ok {
			fmt.Printf("Priority %v is not found in list of addresses\n", addr)
			continue
		}
		scItem.(*subConnWrapper).stop()
	}
}
