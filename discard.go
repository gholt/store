package valuestore

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/gholt/brimtime"
)

const _GLH_DISCARD_BATCH_SIZE = 1024 * 1024

// DisableDiscard will stop any discard passes until EnableDiscard is called. A
// discard pass removes expired tombstones (deletion markers).
func (vs *DefaultValueStore) DisableDiscard() {
	c := make(chan struct{}, 1)
	vs.discardNotifyChan <- &backgroundNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableDiscard will resume discard passes. A discard pass removes expired
// tombstones (deletion markers).
func (vs *DefaultValueStore) EnableDiscard() {
	c := make(chan struct{}, 1)
	vs.discardNotifyChan <- &backgroundNotification{
		enable:   true,
		doneChan: c,
	}
	<-c
}

// DiscardPass will immediately execute a pass to discard expired tombstones
// (deletion markers) rather than waiting for the next interval. If a pass is
// currently executing, it will be stopped and restarted so that a call to this
// function ensures one complete pass occurs.
func (vs *DefaultValueStore) DiscardPass() {
	atomic.StoreUint32(&vs.discardAbort, 1)
	c := make(chan struct{}, 1)
	vs.discardNotifyChan <- &backgroundNotification{doneChan: c}
	<-c
}

func (vs *DefaultValueStore) discardLauncher() {
	var enabled bool
	interval := float64(vs.discardInterval) * float64(time.Second)
	nextRun := time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
	for {
		var notification *backgroundNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-vs.discardNotifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-vs.discardNotifyChan:
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
				atomic.StoreUint32(&vs.discardAbort, 1)
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&vs.discardAbort, 0)
			vs.discardPass()
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&vs.discardAbort, 0)
			vs.discardPass()
		}
	}
}

type localRemovalEntry struct {
	keyA      uint64
	keyB      uint64
	timestamp uint64
}

func (vs *DefaultValueStore) discardPass() {
	if vs.logDebug != nil {
		begin := time.Now()
		defer func() {
			vs.logDebug.Printf("discard pass took %s", time.Now().Sub(begin))
		}()
	}
	vs.vlm.Discard(_TSB_LOCAL_REMOVAL)
	cutoff := (uint64(brimtime.TimeToUnixMicro(time.Now())) << _TSB_UTIL_BITS) - vs.tombstoneAge
	localRemovals := make([]localRemovalEntry, _GLH_DISCARD_BATCH_SIZE)
	var scanStart uint64 = 0
	for {
		prevScanStart := scanStart
		localRemovalsIndex := 0
		vs.vlm.ScanCallback(scanStart, math.MaxUint64, func(keyA uint64, keyB uint64, timestamp uint64, length uint32) {
			if scanStart == prevScanStart && timestamp&_TSB_LOCAL_REMOVAL == 0 && timestamp < cutoff {
				e := &localRemovals[localRemovalsIndex]
				e.keyA = keyA
				e.keyB = keyB
				e.timestamp = timestamp
				localRemovalsIndex++
				if localRemovalsIndex == _GLH_DISCARD_BATCH_SIZE {
					scanStart = keyA
				}
			}
		})
		// Issue local removals in reverse order to give a much lesser chance
		// of reissuing a local removal for the same keys on the next pass.
		for i := localRemovalsIndex - 1; i >= 0; i-- {
			e := &localRemovals[i]
			vs.write(e.keyA, e.keyB, e.timestamp|_TSB_LOCAL_REMOVAL, nil)
		}
		if scanStart == prevScanStart {
			break
		}
	}
}
