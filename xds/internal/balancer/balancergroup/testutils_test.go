package balancergroup

import (
	"testing"

	"google.golang.org/grpc/internal/grpctest"
	"google.golang.org/grpc/xds/internal/testutils"
)

type s struct {
	grpctest.Tester
}

func Test(t *testing.T) {
	grpctest.RunSubTests(t, s{})
}

func init() {
	newRandomWRR = testutils.NewTestWRR
}
