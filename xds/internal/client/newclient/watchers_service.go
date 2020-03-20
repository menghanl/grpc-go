package client

import (
	"fmt"
	"sync"
)

// ServiceUpdate contains update about the service.
type ServiceUpdate struct {
	Cluster string
}

type serviceCallbackFunc func(ServiceUpdate, error)

// WatchService uses LDS and RDS to discover information about the provided
// serviceName.
//
// WatchService can only be called once. The second call will not start a
// watcher and the callback will get an error. It's this case because an xDS
// client is expected to be used only by one ClientConn.
//
// Note that during race, there's a small window where the callback can be
// called after the watcher is canceled. The caller needs to handle this case.
func (c *Client) WatchService(serviceName string, cb serviceCallbackFunc) (cancel func()) {
	c.mu.Lock()
	if len(c.ldsWatchers) != 0 {
		go cb(ServiceUpdate{}, fmt.Errorf("unexpected WatchService when there's another service being watched"))
		c.mu.Unlock()
		return func() {}
	}
	c.mu.Unlock()

	w := &serviceUpdateWatcher{c: c, serviceCB: cb}
	w.ldsCancel = c.watchLDS(serviceName, w.handleLDSResp)

	return w.close
}

// serviceUpdateWatcher handles LDS and RDS response, and calls the service
// callback at the right time.
type serviceUpdateWatcher struct {
	c         *Client
	ldsCancel func()
	serviceCB serviceCallbackFunc

	mu        sync.Mutex
	closed    bool
	rdsCancel func()
}

func (w *serviceUpdateWatcher) handleLDSResp(update ldsUpdate, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	// Todo: this error case returns early, without canceling the existing RDS
	// watch. If we decided to stop the RDS watch when LDS errors, move this
	// after rdsCancel(). We may also need to check the error type and do
	// different things based on that.
	if err != nil {
		w.serviceCB(ServiceUpdate{}, err)
		return
	}

	if w.rdsCancel != nil {
		w.rdsCancel()
	}
	w.rdsCancel = w.c.watchRDS(update.routeName, w.handleRDSResp)
}

func (w *serviceUpdateWatcher) handleRDSResp(update rdsUpdate, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	if err != nil {
		w.serviceCB(ServiceUpdate{}, err)
		return
	}
	w.serviceCB(ServiceUpdate{Cluster: update.clusterName}, nil)
}

func (w *serviceUpdateWatcher) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	w.ldsCancel()
	if w.rdsCancel != nil {
		w.rdsCancel()
	}
}