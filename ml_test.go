package ml

import (
	"testing"

	"ml.test.com/leakcheck"

	"cloud.google.com/go/compute/metadata"
)

func TestML(t *testing.T) {

	defer leakcheck.Check(t)

	if metadata.OnGCE() {
		t.Logf("on gce")
	}
}
