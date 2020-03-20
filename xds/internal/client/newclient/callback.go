package client

import "fmt"

type watcherInfoWithUpdate struct {
	wi     *watchInfo
	update interface{}
	err    error
}

func (c *Client) scheduleCallback(wi *watchInfo, update interface{}, err error) {
	c.updateCh.Put(&watcherInfoWithUpdate{
		wi:     wi,
		update: update,
		err:    err,
	})
}

func (c *Client) callCallback(wiu *watcherInfoWithUpdate) {
	c.mu.Lock()
	// Use a closure to capture the callback and type assertion, to save one
	// more switch case.
	//
	// The callback must be called without c.mu. Otherwise if the callback calls
	// another watch() inline, it will cause a deadlock. This leaves a small
	// window that a watcher's callback could be called after the watcher is
	// canceled, and the user needs to take care of it
	var ccb func()
	switch wiu.wi.typeURL {
	case ldsURL:
		if s, ok := c.ldsWatchers[wiu.wi.target]; ok && s.has(wiu.wi) {
			ccb = func() { wiu.wi.ldsCallback(wiu.update.(ldsUpdate), wiu.err) }
		}
	case rdsURL:
		if s, ok := c.rdsWatchers[wiu.wi.target]; ok && s.has(wiu.wi) {
			ccb = func() { wiu.wi.rdsCallback(wiu.update.(rdsUpdate), wiu.err) }
		}
	case cdsURL:
		if s, ok := c.cdsWatchers[wiu.wi.target]; ok && s.has(wiu.wi) {
			ccb = func() { wiu.wi.cdsCallback(wiu.update.(ClusterUpdate), wiu.err) }
		}
	case edsURL:
		if s, ok := c.edsWatchers[wiu.wi.target]; ok && s.has(wiu.wi) {
			ccb = func() { wiu.wi.edsCallback(wiu.update.(EndpointsUpdate), wiu.err) }
		}
	}
	c.mu.Unlock()

	if ccb != nil {
		ccb()
	}
}

func (c *Client) newUpdate(typeURL string, d map[string]interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var watchers map[string]*watchInfoSet
	switch typeURL {
	case ldsURL:
		watchers = c.ldsWatchers
	case rdsURL:
		watchers = c.rdsWatchers
	case cdsURL:
		watchers = c.cdsWatchers
	case edsURL:
		watchers = c.edsWatchers
	}
	fmt.Printf(" handling update %v, %v, with watchers %+v\n", typeURL, d, watchers)
	for name, update := range d {
		if s, ok := watchers[name]; ok {
			s.forEach(func(wi *watchInfo) {
				c.scheduleCallback(wi, update, nil)
			})
		}
	}
	// TODO: for LDS and CDS, handle removing resources.
	// var emptyUpdate interface{}
	// switch typeURL {
	// case ldsURL:
	// 	emptyUpdate = ldsUpdate{}
	// case cdsURL:
	// 	emptyUpdate = ClusterUpdate{}
	// }
	// if emptyUpdate != nil {
	// 	for name, s := range watchers {
	// 		if _, ok := d[name]; !ok {
	// 			s.forEach(func(wi *watchInfo) {
	// 				c.scheduleCallback(wi, emptyUpdate, fmt.Errorf("resource removed"))
	// 			})
	// 		}
	// 	}
	// }
	c.syncCache(typeURL, d)
}

// Caller must hold c.mu.
func (c *Client) syncCache(typeURL string, d map[string]interface{}) {
	var f func(name string, update interface{})
	switch typeURL {
	case ldsURL:
		f = func(name string, update interface{}) {
			if _, ok := c.ldsWatchers[name]; ok {
				c.ldsCache[name] = update.(ldsUpdate)
			}
			fmt.Printf(" syncing cache %v, %v, cache afterwards %+v\n", typeURL, d, c.ldsCache)
		}
	case rdsURL:
		f = func(name string, update interface{}) {
			if _, ok := c.rdsWatchers[name]; ok {
				c.rdsCache[name] = update.(rdsUpdate)
			}
			fmt.Printf(" syncing cache %v, %v, cache afterwards %+v\n", typeURL, d, c.rdsCache)
		}
	case cdsURL:
		f = func(name string, update interface{}) {
			if _, ok := c.cdsWatchers[name]; ok {
				c.cdsCache[name] = update.(ClusterUpdate)
			}
		}
	case edsURL:
		f = func(name string, update interface{}) {
			if _, ok := c.edsWatchers[name]; ok {
				c.edsCache[name] = update.(EndpointsUpdate)
			}
		}
	}
	for name, update := range d {
		f(name, update)
	}
	// TODO: remove item from cache? LDS and CDS, remove if not in d, and also
	// remove corresponding RDS/EDS data?
}
