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
	// Other cases.
	case cdsURL:
		if s, ok := c.cdsWatchers[wiu.wi.target]; ok && s.has(wiu.wi) {
			ccb = func() { wiu.wi.cdsCallback(wiu.update.(CDSUpdate), wiu.err) }
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
	// case ldsURL:
	// case rdsURL:
	case cdsURL:
		watchers = c.cdsWatchers
		// case edsURL:
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
	case cdsURL:
		f = func(name string, update interface{}) {
			c.cdsCache[name] = update.(CDSUpdate)
		}
	}
	for name, update := range d {
		f(name, update)
	}
	// TODO: remove item from cache? LDS and CDS, remove if not in d, and also
	// remove corresponding RDS/EDS data?
}
