package client

import (
	"fmt"
	"time"
)

// The value chosen here is based on the default value of the
// initial_fetch_timeout field in corepb.ConfigSource proto.
var defaultWatchExpiryTimeout = 15 * time.Second

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

// watchInfo holds all the information about a watch call.
type watchInfo struct {
	typeURL string
	target  string

	cdsCallback cdsCallbackFunc
	expiryTimer *time.Timer
}

// WatchCluster uses CDS to discover information about the provided
// clusterName.
//
// Note that during race, there's a small window where the callback can be
// called after the watcher is canceled. The caller needs to handle this case.
func (c *Client) WatchCluster(clusterName string, cdsCb func(ClusterUpdate, error)) (cancel func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	wi := &watchInfo{
		typeURL:     cdsURL,
		target:      clusterName,
		cdsCallback: cdsCb,
	}

	wi.expiryTimer = time.AfterFunc(defaultWatchExpiryTimeout, func() {
		c.scheduleCallback(wi, ClusterUpdate{}, fmt.Errorf("xds: CDS target %s not found, watcher timeout", clusterName))
	})
	return c.watch(wi)
}

func (c *Client) watch(wi *watchInfo) (cancel func()) {
	// If this is a new watcher, will ask lower level to send a new request with
	// the resource name. If this type+name is already being watched, will do
	// nothing.
	var watchers map[string]*watchInfoSet
	switch wi.typeURL {
	// case ldsURL:
	// case rdsURL:
	case cdsURL:
		watchers = c.cdsWatchers
		// case edsURL:
	}

	resourceName := wi.target
	s, ok := watchers[wi.target]
	if !ok {
		s = newWatchInfoSet()
		watchers[resourceName] = s
		c.v2c.addWatch(wi.typeURL, resourceName)
	}
	s.add(wi)

	// If this clusterName is in cache, it will call the callback with the
	// value.
	switch wi.typeURL {
	case cdsURL:
		if v, ok := c.cdsCache[resourceName]; ok {
			c.scheduleCallback(wi, v, nil)
		}
	}

	return func() {
		// Remove this watcher from cdsWatchers.
		c.mu.Lock()
		defer c.mu.Unlock()
		if s := watchers[resourceName]; s != nil {
			wi.expiryTimer.Stop()
			s.remove(wi)
			if s.len() == 0 {
				delete(watchers, resourceName)
				c.v2c.removeWatch(wi.typeURL, resourceName)
			}
		}
	}
}
