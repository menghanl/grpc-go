package client

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/internal/grpclog"

	corepb "github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
)

type updateReceiver interface {
	newUpdate(typeURL string, d map[string]interface{})
}

var newV2Client = func(parent *Client, cc *grpc.ClientConn, nodeProto *corepb.Node, backoff func(int) time.Duration, logger *grpclog.PrefixLogger) xdsv2Client {
	return nil
}

type ghostClient struct {
	parent updateReceiver // To call newUpdate()
}

func (c ghostClient) watchCDS() {}
func (c ghostClient) close()    {}
