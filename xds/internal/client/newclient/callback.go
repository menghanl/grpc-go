package client

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
	// FIXME(switch): Other cases.
	case ldsURL:
		if s, ok := c.ldsWatchers[wiu.wi.target]; ok && s.has(wiu.wi) {
			ccb = func() { wiu.wi.ldsCallback(wiu.update.(ldsUpdate), wiu.err) }
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
	// FIXME(switch): Other cases.
	case ldsURL:
		watchers = c.ldsWatchers
	// case rdsURL:
	case cdsURL:
		watchers = c.cdsWatchers
	case edsURL:
		watchers = c.edsWatchers
	}
	for name, update := range d {
		if s, ok := watchers[name]; ok {
			s.forEach(func(wi *watchInfo) {
				c.scheduleCallback(wi, update, nil)
			})
		}
	}
	c.syncCache(typeURL, d)
}

// Caller must hold c.mu.
func (c *Client) syncCache(typeURL string, d map[string]interface{}) {
	var f func(name string, update interface{})
	switch typeURL {
	// FIXME(switch): Other cases.
	case ldsURL:
		f = func(name string, update interface{}) {
			c.ldsCache[name] = update.(ldsUpdate)
		}
	case cdsURL:
		f = func(name string, update interface{}) {
			c.cdsCache[name] = update.(ClusterUpdate)
		}
	case edsURL:
		f = func(name string, update interface{}) {
			c.edsCache[name] = update.(EndpointsUpdate)
		}
	}
	for name, update := range d {
		f(name, update)
	}
	// TODO: remove item from cache? LDS and CDS, remove if not in d, and also
	// remove corresponding RDS/EDS data?
}
