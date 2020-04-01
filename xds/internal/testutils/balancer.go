package testutils

import (
	"context"
	"fmt"
	"testing"

	envoy_api_v2_core "github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/xds/internal"
)

const TestSubConnsCount = 16

var TestSubConns []*TestSubConn

func init() {
	for i := 0; i < TestSubConnsCount; i++ {
		TestSubConns = append(TestSubConns, &TestSubConn{
			id: fmt.Sprintf("sc%d", i),
		})
	}
}

type TestSubConn struct {
	id string
}

func (tsc *TestSubConn) UpdateAddresses([]resolver.Address) {
	panic("not implemented")
}

func (tsc *TestSubConn) Connect() {
}

// Implement stringer to get human friendly error message.
func (tsc *TestSubConn) String() string {
	return tsc.id
}

type TestClientConn struct {
	t *testing.T // For logging only.

	NewSubConnAddrsCh chan []resolver.Address // the last 10 []Address to create subconn.
	NewSubConnCh      chan balancer.SubConn   // the last 10 subconn created.
	RemoveSubConnCh   chan balancer.SubConn   // the last 10 subconn removed.

	NewPickerCh chan balancer.V2Picker  // the last picker updated.
	NewStateCh  chan connectivity.State // the last state.

	subConnIdx int
}

func NewTestClientConn(t *testing.T) *TestClientConn {
	return &TestClientConn{
		t: t,

		NewSubConnAddrsCh: make(chan []resolver.Address, 10),
		NewSubConnCh:      make(chan balancer.SubConn, 10),
		RemoveSubConnCh:   make(chan balancer.SubConn, 10),

		NewPickerCh: make(chan balancer.V2Picker, 1),
		NewStateCh:  make(chan connectivity.State, 1),
	}
}

func (tcc *TestClientConn) NewSubConn(a []resolver.Address, o balancer.NewSubConnOptions) (balancer.SubConn, error) {
	sc := TestSubConns[tcc.subConnIdx]
	tcc.subConnIdx++

	tcc.t.Logf("testClientConn: NewSubConn(%v, %+v) => %s", a, o, sc)
	select {
	case tcc.NewSubConnAddrsCh <- a:
	default:
	}

	select {
	case tcc.NewSubConnCh <- sc:
	default:
	}

	return sc, nil
}

func (tcc *TestClientConn) RemoveSubConn(sc balancer.SubConn) {
	tcc.t.Logf("testClientCOnn: RemoveSubConn(%p)", sc)
	select {
	case tcc.RemoveSubConnCh <- sc:
	default:
	}
}

func (tcc *TestClientConn) UpdateBalancerState(s connectivity.State, p balancer.Picker) {
	tcc.t.Fatal("not implemented")
}

func (tcc *TestClientConn) UpdateState(bs balancer.State) {
	tcc.t.Logf("testClientConn: UpdateState(%v)", bs)
	select {
	case <-tcc.NewStateCh:
	default:
	}
	tcc.NewStateCh <- bs.ConnectivityState

	select {
	case <-tcc.NewPickerCh:
	default:
	}
	tcc.NewPickerCh <- bs.Picker
}

func (tcc *TestClientConn) ResolveNow(resolver.ResolveNowOptions) {
	panic("not implemented")
}

func (tcc *TestClientConn) Target() string {
	panic("not implemented")
}

type TestServerLoad struct {
	Name string
	D    float64
}

type TestLoadStore struct {
	CallsStarted []internal.Locality
	CallsEnded   []internal.Locality
	CallsCost    []TestServerLoad
}

func NewTestLoadStore() *TestLoadStore {
	return &TestLoadStore{}
}

func (*TestLoadStore) CallDropped(category string) {
	panic("not implemented")
}

func (tls *TestLoadStore) CallStarted(l internal.Locality) {
	tls.CallsStarted = append(tls.CallsStarted, l)
}

func (tls *TestLoadStore) CallFinished(l internal.Locality, err error) {
	tls.CallsEnded = append(tls.CallsEnded, l)
}

func (tls *TestLoadStore) CallServerLoad(l internal.Locality, name string, d float64) {
	tls.CallsCost = append(tls.CallsCost, TestServerLoad{Name: name, D: d})
}

func (*TestLoadStore) ReportTo(ctx context.Context, cc *grpc.ClientConn, clusterName string, node *envoy_api_v2_core.Node) {
	panic("not implemented")
}

// isRoundRobin checks whether f's return value is roundrobin of elements from
// want. But it doesn't check for the order. Note that want can contain
// duplicate items, which makes it weight-round-robin.
//
// Step 1. the return values of f should form a permutation of all elements in
// want, but not necessary in the same order. E.g. if want is {a,a,b}, the check
// fails if f returns:
//  - {a,a,a}: third a is returned before b
//  - {a,b,b}: second b is returned before the second a
//
// If error is found in this step, the returned error contains only the first
// iteration until where it goes wrong.
//
// Step 2. the return values of f should be repetitions of the same permutation.
// E.g. if want is {a,a,b}, the check failes if f returns:
//  - {a,b,a,b,a,a}: though it satisfies step 1, the second iteration is not
//  repeating the first iteration.
//
// If error is found in this step, the returned error contains the first
// iteration + the second iteration until where it goes wrong.
func IsRoundRobin(want []balancer.SubConn, f func() balancer.SubConn) error {
	wantSet := make(map[balancer.SubConn]int) // SubConn -> count, for weighted RR.
	for _, sc := range want {
		wantSet[sc]++
	}

	// The first iteration: makes sure f's return values form a permutation of
	// elements in want.
	//
	// Also keep the returns values in a slice, so we can compare the order in
	// the second iteration.
	gotSliceFirstIteration := make([]balancer.SubConn, 0, len(want))
	for range want {
		got := f()
		gotSliceFirstIteration = append(gotSliceFirstIteration, got)
		wantSet[got]--
		if wantSet[got] < 0 {
			return fmt.Errorf("non-roundrobin want: %v, result: %v", want, gotSliceFirstIteration)
		}
	}

	// The second iteration should repeat the first iteration.
	var gotSliceSecondIteration []balancer.SubConn
	for i := 0; i < 2; i++ {
		for _, w := range gotSliceFirstIteration {
			g := f()
			gotSliceSecondIteration = append(gotSliceSecondIteration, g)
			if w != g {
				return fmt.Errorf("non-roundrobin, first iter: %v, second iter: %v", gotSliceFirstIteration, gotSliceSecondIteration)
			}
		}
	}

	return nil
}

// testClosure is a test util for TestIsRoundRobin.
type testClosure struct {
	r []balancer.SubConn
	i int
}

func (tc *testClosure) next() balancer.SubConn {
	ret := tc.r[tc.i]
	tc.i = (tc.i + 1) % len(tc.r)
	return ret
}
