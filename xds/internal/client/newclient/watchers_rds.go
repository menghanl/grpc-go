package client

import (
	"fmt"
	"time"
)

type rdsUpdate struct {
	clusterName string
}
type rdsCallbackFunc func(rdsUpdate, error)

// watchRDS starts a listener watcher for the service..
//
// Note that during race, there's a small window where the callback can be
// called after the watcher is canceled. The caller needs to handle this case.
func (c *Client) watchRDS(routeName string, cb rdsCallbackFunc) (cancel func()) {
	c.mu.Lock()
	fmt.Println("watch RDS")
	defer func() { fmt.Println("done watch RDS") }()
	defer c.mu.Unlock()
	wi := &watchInfo{
		typeURL:     rdsURL,
		target:      routeName,
		rdsCallback: cb,
	}

	wi.expiryTimer = time.AfterFunc(defaultWatchExpiryTimeout, func() {
		c.scheduleCallback(wi, rdsUpdate{}, fmt.Errorf("xds: RDS target %s not found, watcher timeout", routeName))
	})
	return c.watch(wi)
}
