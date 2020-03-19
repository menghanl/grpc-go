package client

import (
	"fmt"
	"time"
)

type ldsUpdate struct {
	routeName string
}
type ldsCallbackFunc func(ldsUpdate, error)

// watchLDS starts a listener watcher for the service..
//
// Note that during race, there's a small window where the callback can be
// called after the watcher is canceled. The caller needs to handle this case.
func (c *Client) watchLDS(serviceName string, cb ldsCallbackFunc) (cancel func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	wi := &watchInfo{
		typeURL:     ldsURL,
		target:      serviceName,
		ldsCallback: cb,
	}

	wi.expiryTimer = time.AfterFunc(defaultWatchExpiryTimeout, func() {
		c.scheduleCallback(wi, ldsUpdate{}, fmt.Errorf("xds: LDS target %s not found, watcher timeout", serviceName))
	})
	return c.watch(wi)
}
