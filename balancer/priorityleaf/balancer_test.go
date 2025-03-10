package priorityleaf_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mwitkow/grpc-proxy/proxy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"

	"github.com/spirl/spirl/common/grpcloadbalancing/priorityleaf"
	"github.com/spirl/spirl/common/test/grpctest"
)

// When a high priority is ready, adding/removing lower locality doesn't cause
// changes.
//
// Init 0 and 1; 0 is up, use 0; remove 1, use 0; add 1, use 0; remove 0, use 1.
func TestPriority_HighPriorityReady(t *testing.T) {
	t.Parallel()
	testServer := createServerBehindProxyAndDial(t, 2)
	{
		resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
		require.NoError(t, err)
		require.Equal(t, "Hello world from 0", resp.GetMessage())
	}

	testServer.manualResolver.UpdateState(resolver.State{
		Endpoints: testServer.endpoints[:1],
	})

	callBackend := func(id string) func(collect *assert.CollectT) {
		return func(collect *assert.CollectT) {
			resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("Hello world from %v", id), resp.GetMessage())
		}
	}
	require.EventuallyWithT(t, callBackend("0"), time.Second, time.Second/10, "timeout waiting for RPC to switch backend")

	testServer.manualResolver.UpdateState(resolver.State{
		Endpoints: testServer.endpoints,
	})
	for i := 0; i < 20; i++ {
		resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
		require.NoError(t, err)
		require.Equal(t, "Hello world from 0", resp.GetMessage())
	}

	testServer.manualResolver.UpdateState(resolver.State{
		Endpoints: testServer.endpoints[1:],
	})
	for i := 0; i < 20; i++ {
		resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
		require.NoError(t, err)
		require.Equal(t, "Hello world from 1", resp.GetMessage())
	}
}

// Lower priority is used when higher priority is not ready.
//
// Init 0 and 1; 0 is up, use 0; 0 is down, 1 is up, use 1; add 2, use 1; 1 is
// down, use 2; remove 2, use 1.
func TestPriority_SwitchPriority(t *testing.T) {
	t.Parallel()
	testServer := createServerBehindProxyAndDial(t, 2)
	{
		resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
		require.NoError(t, err)
		require.Equal(t, "Hello world from 0", resp.GetMessage())
	}

	callBackend := func(id string) func(collect *assert.CollectT) {
		return func(collect *assert.CollectT) {
			resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("Hello world from %v", id), resp.GetMessage())
		}
	}

	t.Log("set 0 to not serving")
	testServer.hcServers[0].stateCh <- grpc_health_v1.HealthCheckResponse_NOT_SERVING
	require.EventuallyWithT(t, callBackend("1"), time.Second, time.Second/10, "timeout waiting for RPC to switch backend")

	anotherServer, _, _ := createServerBehindProxy(t, "another")
	_ = anotherServer

	endpoints := testServer.endpoints
	endpoints = append(endpoints, resolver.Endpoint{Addresses: []resolver.Address{{Addr: anotherServer}}})
	testServer.manualResolver.UpdateState(resolver.State{
		Endpoints: endpoints,
	})
	for i := 0; i < 20; i++ {
		resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
		require.NoError(t, err)
		require.Equal(t, "Hello world from 1", resp.GetMessage())
	}

	t.Log("set 1 to not serving")
	testServer.hcServers[1].stateCh <- grpc_health_v1.HealthCheckResponse_NOT_SERVING
	require.EventuallyWithT(t, callBackend("another"), time.Second, time.Second/10, "timeout waiting for RPC to switch backend")
}

// When a higher priority is added, it should be used immediately.
//
// Init 0 and 1; 0 is up, use 0; add 2 before 0, use 2; remove 2, use 0.
func TestPriority_AddHigherPriority(t *testing.T) {
	t.Parallel()
	testServer := createServerBehindProxyAndDial(t, 2)
	{
		resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
		require.NoError(t, err)
		require.Equal(t, "Hello world from 0", resp.GetMessage())
	}

	anotherServer, _, _ := createServerBehindProxy(t, "higher")
	_ = anotherServer

	endpoints := []resolver.Endpoint{{Addresses: []resolver.Address{{Addr: anotherServer}}}}
	endpoints = append(endpoints, testServer.endpoints...)
	testServer.manualResolver.UpdateState(resolver.State{
		Endpoints: endpoints,
	})
	for i := 0; i < 20; i++ {
		resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
		require.NoError(t, err)
		require.Equal(t, "Hello world from higher", resp.GetMessage())
	}
}

func TestPriority_RemovesAllPriorities(t *testing.T) {
	t.Parallel()
	testServer := createServerBehindProxyAndDial(t, 2)
	{
		resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
		require.NoError(t, err)
		require.Equal(t, "Hello world from 0", resp.GetMessage())
	}
	testServer.manualResolver.UpdateState(resolver.State{Endpoints: nil})
	for i := 0; i < 20; i++ {
		_, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
		require.Error(t, err)
		require.ErrorContains(t, err, "all addresses are removed")
	}
}

// When a addr is moved from low priority to high.
//
// Init a(p0) and b(p1); a(p0) is up, use a; move b to p0, a to p1, use b.
func TestPriority_MoveAddrToHigherPriority(t *testing.T) {
	t.Parallel()
	testServer := createServerBehindProxyAndDial(t, 2)
	{
		resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
		require.NoError(t, err)
		require.Equal(t, "Hello world from 0", resp.GetMessage())
	}
	endpoints := []resolver.Endpoint{
		testServer.endpoints[1], testServer.endpoints[0],
	}
	testServer.manualResolver.UpdateState(resolver.State{Endpoints: endpoints})
	for i := 0; i < 20; i++ {
		resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
		require.NoError(t, err)
		require.Equal(t, "Hello world from 1", resp.GetMessage())
	}
}

// When a addr is in lower priority, and in use (because higher is down),
// move it from low priority to high.
//
// Init a(p0) and b(p1); a(p0) is down, use b; move b to p0, a to p1, use b.
func TestPriority_MoveReadyAddrToHigherPriority(t *testing.T) {
	t.Parallel()
	testServer := createServerBehindProxyAndDial(t, 2)
	{
		resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
		require.NoError(t, err)
		require.Equal(t, "Hello world from 0", resp.GetMessage())
	}

	callBackend := func(id string) func(collect *assert.CollectT) {
		return func(collect *assert.CollectT) {
			resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("Hello world from %v", id), resp.GetMessage())
		}
	}

	t.Log("set 0 to not serving")
	testServer.hcServers[0].stateCh <- grpc_health_v1.HealthCheckResponse_NOT_SERVING
	require.EventuallyWithT(t, callBackend("1"), time.Second, time.Second/10, "timeout waiting for RPC to switch backend")

	endpoints := []resolver.Endpoint{
		testServer.endpoints[1], testServer.endpoints[0],
	}
	testServer.manualResolver.UpdateState(resolver.State{Endpoints: endpoints})
	for i := 0; i < 20; i++ {
		resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
		require.NoError(t, err)
		require.Equal(t, "Hello world from 1", resp.GetMessage())
	}
}

// When the lowest addr is in use, and is removed, should use the higher
// priority addr even though it's not ready.
//
// Init a(p0) and b(p1); a(p0) is down, use b; move b to p0, a to p1, use b.
func TestPriority_RemoveReadyLowestPriority(t *testing.T) {
	t.Parallel()
	testServer := createServerBehindProxyAndDial(t, 2)
	{
		resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
		require.NoError(t, err)
		require.Equal(t, "Hello world from 0", resp.GetMessage())
	}

	callBackend := func(id string) func(collect *assert.CollectT) {
		return func(collect *assert.CollectT) {
			resp, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("Hello world from %v", id), resp.GetMessage())
		}
	}

	t.Log("set 0 to not serving")
	testServer.hcServers[0].stateCh <- grpc_health_v1.HealthCheckResponse_NOT_SERVING
	require.EventuallyWithT(t, callBackend("1"), time.Second, time.Second/10, "timeout waiting for RPC to switch backend")

	testServer.manualResolver.UpdateState(resolver.State{Endpoints: testServer.endpoints[:1]})
	for i := 0; i < 20; i++ {
		_, err := testServer.greeterClient.SayHello(t.Context(), &helloworld.HelloRequest{Name: "world"})
		require.Error(t, err)
		require.ErrorContains(t, err, "connection active but health check failed")
	}
}

type testServer struct {
	hcServers []*healthCheckServer
	apiSS     []*grpc.Server
	endpoints []resolver.Endpoint

	manualResolver *manual.Resolver
	greeterClient  helloworld.GreeterClient
}

func createServerBehindProxyAndDial(t *testing.T, count int) *testServer {
	var (
		hcServers []*healthCheckServer
		apiSS     []*grpc.Server
		endpoints []resolver.Endpoint
	)
	for i := 0; i < count; i++ {
		proxyAddr, apiS, hcServer := createServerBehindProxy(t, fmt.Sprint(i))
		hcServers = append(hcServers, hcServer)
		apiSS = append(apiSS, apiS)
		endpoints = append(endpoints, resolver.Endpoint{Addresses: []resolver.Address{{Addr: proxyAddr}}})
		t.Logf("server with proxy %d: %s\n", i, proxyAddr)
	}

	r := manual.NewBuilderWithScheme("whatever")
	s := resolver.State{Endpoints: endpoints}
	r.InitialState(s)

	// Connect to the test backend.
	dopts := []grpc.DialOption{
		grpc.WithResolvers(r),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	dopts = append(dopts, grpc.WithDefaultServiceConfig(fmt.Sprintf(`{
		"healthCheckConfig": {"serviceName": ""},
		"loadBalancingConfig": [{"%s":{}}]}`, priorityleaf.Name)))
	cc, err := grpc.NewClient(r.Scheme()+":///whatever", dopts...)
	require.NoError(t, err)
	t.Cleanup(func() {
		cc.Close()
	})

	return &testServer{
		hcServers:      hcServers,
		apiSS:          apiSS,
		endpoints:      endpoints,
		manualResolver: r,
		greeterClient:  helloworld.NewGreeterClient(cc),
	}
}

func createServerBehindProxy(t *testing.T, id string) (string, *grpc.Server, *healthCheckServer) {
	// Start the real backend.
	api := &helloworldServer{id: id}
	hcServer := &healthCheckServer{stateCh: make(chan grpc_health_v1.HealthCheckResponse_ServingStatus)}
	var retS *grpc.Server
	apiCC := grpctest.StartInsecureServerAndDial(t, func(s *grpc.Server) {
		helloworld.RegisterGreeterServer(s, api)
		grpc_health_v1.RegisterHealthServer(s, hcServer)
		retS = s
	})
	t.Logf("api server %s\n", id)
	// Start the proxy server.
	proxyAddr := grpctest.StartInsecureTCPServer(t, func(s *grpc.Server) {}, proxy.DefaultProxyOpt(apiCC))
	t.Logf("proxy server %s: %s\n", id, proxyAddr)
	return proxyAddr, retS, hcServer
}

type helloworldServer struct {
	helloworld.UnimplementedGreeterServer

	id string
}

func (s *helloworldServer) SayHello(_ context.Context, in *helloworld.HelloRequest) (*helloworld.HelloReply, error) {
	return &helloworld.HelloReply{Message: "Hello " + in.GetName() + " from " + s.id}, nil
}

type healthCheckServer struct {
	grpc_health_v1.UnimplementedHealthServer

	stateCh chan grpc_health_v1.HealthCheckResponse_ServingStatus
	// clk clock.Clock
}

func (s *healthCheckServer) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func (s *healthCheckServer) Watch(req *grpc_health_v1.HealthCheckRequest, stream grpc_health_v1.Health_WatchServer) error {
	if req.Service != "" {
		return status.Errorf(codes.InvalidArgument, "no per-service health checks supported")
	}

	if err := stream.Send(&grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}); err != nil {
		return err
	}

	for {
		select {
		// Wait for the test to change the health status.
		case hs := <-s.stateCh:
			// return status.Errorf(codes.Unavailable, "server is not available")
			if err := stream.Send(&grpc_health_v1.HealthCheckResponse{Status: hs}); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}
