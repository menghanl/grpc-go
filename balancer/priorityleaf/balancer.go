package priorityleaf

import (
	"fmt"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	_ "google.golang.org/grpc/health" // import grpc/health to enable transparent client side checking
	"google.golang.org/grpc/resolver"
)

// Name is the name of the priority balancer.
const Name = "spirl_grpc_priority"

func init() {
	balancer.Register(bb{})
}

type bb struct{}

func (bb) Build(cc balancer.ClientConn, bOpts balancer.BuildOptions) balancer.Balancer {
	b := &priorityBalancer{
		cc:       cc,
		subConns: *resolver.NewAddressMap(),
	}
	return b
}

func (bb) Name() string {
	return Name
}

type priorityBalancer struct {
	cc balancer.ClientConn

	// The current address, it might be the good high priority address or an
	// address currently connecting, and in the worst case, it can be the lowest
	// priority address but everything is down.
	addrInUse resolver.Address
	// priorities is a list of addresses from higher to lower priority.
	priorities []resolver.Address
	// subConns keeps track of all the subConns, whether they are being connected
	// or not. For addresses that are lower priority than addrInUse, those are
	// empty shell with no real SubConn.
	subConns resolver.AddressMap
}

func (b *priorityBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	fmt.Println(" ----- UpdateClientConnState called -----")
	// Each endpoint is its own priority. We don't support more than one address
	// in each endpoint. The priority is determined by the order of the endpoints
	// list. The first endpoint is the highest priority.
	var addrsPriority []resolver.Address
	for _, e := range s.ResolverState.Endpoints {
		if len(e.Addresses) > 1 {
			fmt.Println("warning: more than one address in an endpoint is not supported")
			return balancer.ErrBadResolverState
		}
		addrsPriority = append(addrsPriority, e.Addresses...)
	}

	// Everything was removed by the update.
	if len(addrsPriority) == 0 {
		fmt.Println("warning: all addresses were removed, ignoring this update and keeping the previous state")
		return balancer.ErrBadResolverState
	}

	// Make sure we keep track of all the addresses, whether they are currently
	// connected/connecting or not.
	newAddrsSet := resolver.NewAddressMap() // A set of all the new addresses, we will use this to delete the removed SubConns.
	for _, addr := range addrsPriority {
		newAddrsSet.Set(addr, struct{}{})
		if _, ok := b.subConns.Get(addr); ok {
			// This is an existing address, no need to add it again.
			continue
		}
		// this is a new address, add it to the subConns map. But note that this
		// adds an empty wrapper. Whether we connect to this address is decided
		// later when syncing the priorities.
		fmt.Println("adding subconn:", addr.String())
		b.subConns.Set(addr, newSubConnWrapper(b, addr))
	}
	for _, addr := range b.subConns.Keys() {
		if _, ok := newAddrsSet.Get(addr); !ok {
			fmt.Println("removing subconn:", addr.String())
			// This is an old address, remove it from the subConns map.
			scw, _ := b.subConns.Get(addr)
			scw.(*subConnWrapper).stop()
			b.subConns.Delete(addr)
		}
	}

	// Update priorities and handle priority changes.
	b.priorities = addrsPriority

	// Recompute the priority, this could be a noop if the current addr in use is
	// good, or this will trigger reconnecting.
	b.syncPriority(b.addrInUse)
	return nil
}

func (b *priorityBalancer) ResolverError(err error) {
	fmt.Printf("Received error from the resolver: %v\n", err)

	// If there are existing subConns, we don't need to do anything. We will wait
	// for the subconns to connect/error.
	if b.subConns.Len() > 0 {
		return
	}

	// Otherwise, we report the error to the ClientConn, so RPCs will fail with
	// better error message.
	b.cc.UpdateState(balancer.State{
		ConnectivityState: connectivity.TransientFailure,
		Picker:            &picker{err: fmt.Errorf("name resolver error: %v", err)},
	})

}

func (b *priorityBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	// Deprecated method but still needed for the interface.
	fmt.Printf("UpdateSubConnState(%v, %+v) called unexpectedly\n", sc, state)
}

func (b *priorityBalancer) Close() {
	b.addrInUse = resolver.Address{}
	for _, sc := range b.subConns.Values() {
		sc.(*subConnWrapper).stop()
	}
	b.subConns = *resolver.NewAddressMap()
	b.priorities = nil
}

// This balancer will never be in IDLE. It always kicks the SubConns to connect.
func (b *priorityBalancer) ExitIdle() {
}
