package client

import (
	"time"

	corepb "github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	"google.golang.org/grpc"
	"google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/xds/internal/client/bootstrap"
)

type testXDSV2Client struct {
	r updateReceiver

	watches map[string]map[string]struct{}
}

func overrideNewV2Client() (<-chan *testXDSV2Client, func()) {
	oldNewV2Client := newV2Client
	ch := make(chan *testXDSV2Client, 1)
	newV2Client = func(parent *Client, cc *grpc.ClientConn, nodeProto *corepb.Node, backoff func(int) time.Duration, logger *grpclog.PrefixLogger) xdsv2Client {
		ret := newTestXDSV2Client(parent)
		ch <- ret
		return ret
	}
	return ch, func() { newV2Client = oldNewV2Client }
}

func newTestXDSV2Client(r updateReceiver) *testXDSV2Client {
	watches := make(map[string]map[string]struct{})
	watches[ldsURL] = make(map[string]struct{})
	watches[rdsURL] = make(map[string]struct{})
	watches[cdsURL] = make(map[string]struct{})
	watches[edsURL] = make(map[string]struct{})
	return &testXDSV2Client{
		r:       r,
		watches: watches,
	}
}

func (c *testXDSV2Client) addWatch(resourceType string, resourceName string) {
	c.watches[resourceType][resourceName] = struct{}{}
}

func (c *testXDSV2Client) removeWatch(resourceType string, resourceName string) {
	delete(c.watches[resourceType], resourceName)
}

func (c *testXDSV2Client) close() {}

func clientOpts(balancerName string) Options {
	return Options{
		Config: bootstrap.Config{
			BalancerName: balancerName,
			Creds:        grpc.WithInsecure(),
			NodeProto:    &corepb.Node{},
		},
		// // WithTimeout is deprecated. But we are OK to call it here from the
		// // test, so we clearly know that the dial failed.
		// DialOpts: []grpc.DialOption{grpc.WithTimeout(5 * time.Second), grpc.WithBlock()},
	}
}
