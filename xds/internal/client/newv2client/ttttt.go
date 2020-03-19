package client

const (
	ldsURL = "type.googleapis.com/envoy.api.v2.Listener"
	rdsURL = "type.googleapis.com/envoy.api.v2.RouteConfiguration"
	cdsURL = "type.googleapis.com/envoy.api.v2.Cluster"
	edsURL = "type.googleapis.com/envoy.api.v2.ClusterLoadAssignment"
)

// ClusterUpdate contains information from a received CDS response, which is of
// interest to the registered CDS watcher.
type ClusterUpdate struct {
	// ServiceName is the service name corresponding to the clusterName which
	// is being watched for through CDS.
	ServiceName string
	// EnableLRS indicates whether or not load should be reported through LRS.
	EnableLRS bool
}

type cdsCallbackFunc func(ClusterUpdate, error)

type ldsUpdate struct {
	routeName string
}
type ldsCallbackFunc func(ldsUpdate, error)
