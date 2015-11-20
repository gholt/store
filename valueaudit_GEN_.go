package store

import (
	"sync/atomic"
	"time"
)

type valueAuditState struct {
	notifyChan chan *backgroundNotification
	abort      uint32
}

func (store *DefaultValueStore) auditConfig(cfg *ValueStoreConfig) {
	store.auditState.notifyChan = make(chan *backgroundNotification, 1)
}

func (store *DefaultValueStore) auditLaunch() {
	go store.auditLauncher()
}

// DisableAudit will stop any audit passes until EnableAudit is called. An
// audit pass checks on-disk data for errors.
func (store *DefaultValueStore) DisableAudit() {
	c := make(chan struct{}, 1)
	store.auditState.notifyChan <- &backgroundNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableAudit will resume audit passes. An audit pass checks on-disk data for
// errors.
func (store *DefaultValueStore) EnableAudit() {
	c := make(chan struct{}, 1)
	store.auditState.notifyChan <- &backgroundNotification{
		enable:   true,
		doneChan: c,
	}
	<-c
}

// AuditPass will immediately execute a pass at full speed to check the on-disk
// data for errors rather than waiting for the next interval to run the
// standard slow-audit pass. If a pass is currently executing, it will be
// stopped and restarted so that a call to this function ensures one complete
// pass occurs.
func (store *DefaultValueStore) AuditPass() {
	atomic.StoreUint32(&store.auditState.abort, 1)
	c := make(chan struct{}, 1)
	store.auditState.notifyChan <- &backgroundNotification{doneChan: c}
	<-c
}

func (store *DefaultValueStore) auditLauncher() {
	var enabled bool
	for {
		var notification *backgroundNotification
		select {
		case notification = <-store.auditState.notifyChan:
		default:
		}
		if notification != nil {
			if notification.enable {
				enabled = true
				notification.doneChan <- struct{}{}
				continue
			}
			if notification.disable {
				atomic.StoreUint32(&store.auditState.abort, 1)
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&store.auditState.abort, 0)
			store.auditPass(true)
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&store.auditState.abort, 0)
			store.auditPass(false)
		}
	}
}

func (store *DefaultValueStore) auditPass(speed bool) {
	if store.logDebug != nil {
		begin := time.Now()
		defer func() {
			store.logDebug("audit pass took %s\n", time.Now().Sub(begin))
		}()
	}
	time.Sleep(10 * time.Millisecond) // TODO: actual audit pass
}
