package valuestore

import (
	"io/ioutil"
	"sync/atomic"
	"time"
)

type compactionState struct {
	interval   int
	notifyChan chan *backgroundNotification
	abort      uint32
}

func (vs *DefaultValueStore) compactionInit(cfg *config) {
	vs.compactionState.interval = cfg.compactionInterval
	vs.compactionState.notifyChan = make(chan *backgroundNotification, 1)
	go vs.compactionLauncher()
}

// DisableCompaction will stop any compaction passes until
// EnableCompaction is called. A compaction pass searches for files
// with a percentage of XX deleted entries.
func (vs *DefaultValueStore) DisableCompaction() {
	c := make(chan struct{}, 1)
	vs.compactionState.notifyChan <- &backgroundNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableCompaction will resume compaction passes.
// A compaction pass searches for files with a percentage of XX deleted
// entries.
func (vs *DefaultValueStore) EnableCompaction() {
	c := make(chan struct{}, 1)
	vs.compactionState.notifyChan <- &backgroundNotification{
		enable:   true,
		doneChan: c,
	}
	<-c
}

// CompactionPass will immediately execute a compaction pass to compact/stale files.
func (vs *DefaultValueStore) CompactionPass() {
	atomic.StoreUint32(&vs.compactionState.abort, 1)
	c := make(chan struct{}, 1)
	vs.compactionState.notifyChan <- &backgroundNotification{doneChan: c}
	<-c
}

func (vs *DefaultValueStore) compactionLauncher() {
	var enabled bool
	interval := float64(vs.compactionState.interval) * float64(time.Second)
	nextRun := time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
	for {
		var notification *backgroundNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-vs.compactionState.notifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-vs.compactionState.notifyChan:
			default:
			}
		}
		nextRun = time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
		if notification != nil {
			if notification.enable {
				enabled = true
				notification.doneChan <- struct{}{}
				continue
			}
			if notification.disable {
				atomic.StoreUint32(&vs.compactionState.abort, 1)
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&vs.compactionState.abort, 0)
			vs.compactionPass()
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&vs.compactionState.abort, 0)
			vs.compactionPass()
		}
	}
}

func (vs *DefaultValueStore) compactionPass() {
	if vs.logDebug != nil {
		begin := time.Now()
		defer func() {
			vs.logDebug.Printf("compaction pass took %s", time.Now().Sub(begin))
		}()
	}
	vs.doCompaction()
}

func (vs *DefaultValueStore) doCompaction() {
	files, _ := ioutil.ReadDir("./")
	vs.logDebug.Println(files)
}
