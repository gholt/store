package valuestore

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/brimtime-v1"
)

const _GLH_TOMBSTONE_DISCARD_BATCH_SIZE = 1024 * 1024

type tombstoneDiscardState struct {
	interval      int
	age           uint64
	notifyChan    chan *backgroundNotification
	abort         uint32
	localRemovals [][]localRemovalEntry
}

type localRemovalEntry struct {
	keyA          uint64
	keyB          uint64
	timestampbits uint64
}

func (vs *DefaultValueStore) tombstoneDiscardInit(cfg *config) {
	vs.tombstoneDiscardState.interval = cfg.tombstoneDiscardInterval
	vs.tombstoneDiscardState.age = (uint64(cfg.tombstoneAge) * uint64(time.Second) / 1000) << _TSB_UTIL_BITS
	vs.tombstoneDiscardState.notifyChan = make(chan *backgroundNotification, 1)
	go vs.tombstoneDiscardLauncher()
}

// DisableTombstoneDiscard will stop any discard passes until
// EnableTombstoneDiscard is called. A discard pass removes expired tombstones
// (deletion markers).
func (vs *DefaultValueStore) DisableTombstoneDiscard() {
	c := make(chan struct{}, 1)
	vs.tombstoneDiscardState.notifyChan <- &backgroundNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableTombstoneDiscard will resume discard passes. A discard pass removes
// expired tombstones (deletion markers).
func (vs *DefaultValueStore) EnableTombstoneDiscard() {
	c := make(chan struct{}, 1)
	vs.tombstoneDiscardState.notifyChan <- &backgroundNotification{
		enable:   true,
		doneChan: c,
	}
	<-c
}

// TombstoneDiscardPass will immediately execute a pass to discard expired
// tombstones (deletion markers) rather than waiting for the next interval. If
// a pass is currently executing, it will be stopped and restarted so that a
// call to this function ensures one complete pass occurs.
func (vs *DefaultValueStore) TombstoneDiscardPass() {
	atomic.StoreUint32(&vs.tombstoneDiscardState.abort, 1)
	c := make(chan struct{}, 1)
	vs.tombstoneDiscardState.notifyChan <- &backgroundNotification{doneChan: c}
	<-c
}

func (vs *DefaultValueStore) tombstoneDiscardLauncher() {
	var enabled bool
	interval := float64(vs.tombstoneDiscardState.interval) * float64(time.Second)
	nextRun := time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
	for {
		var notification *backgroundNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-vs.tombstoneDiscardState.notifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-vs.tombstoneDiscardState.notifyChan:
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
				atomic.StoreUint32(&vs.tombstoneDiscardState.abort, 1)
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&vs.tombstoneDiscardState.abort, 0)
			vs.tombstoneDiscardPass()
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&vs.tombstoneDiscardState.abort, 0)
			vs.tombstoneDiscardPass()
		}
	}
}

func (vs *DefaultValueStore) tombstoneDiscardPass() {
	if vs.logDebug != nil {
		begin := time.Now()
		defer func() {
			vs.logDebug.Printf("tombstone discard pass took %s", time.Now().Sub(begin))
		}()
	}
	vs.tombstoneDiscardPassLocalRemovals()
	vs.tombstoneDiscardPassExpiredDeletions()
}

func (vs *DefaultValueStore) tombstoneDiscardPassLocalRemovals() {
	rightwardPartitionShift := uint16(0)
	if vs.ring != nil {
		rightwardPartitionShift = 64 - vs.ring.PartitionBits()
	}
	partitionCount := uint64(1) << vs.ring.PartitionBits()
	ws := uint64(vs.workers)
	f := func(p uint64, w uint64) {
		pb := p << rightwardPartitionShift
		rb := pb + ((uint64(1) << rightwardPartitionShift) / ws * w)
		var re uint64
		if w+1 == ws {
			if p+1 == partitionCount {
				re = math.MaxUint64
			} else {
				re = ((p + 1) << rightwardPartitionShift) - 1
			}
		} else {
			re = pb + ((uint64(1) << rightwardPartitionShift) / ws * (w + 1)) - 1
		}
		vs.vlm.Discard(rb, re, _TSB_LOCAL_REMOVAL)
	}
	wg := &sync.WaitGroup{}
	wg.Add(int(ws))
	for w := uint64(0); w < ws; w++ {
		go func(w uint64) {
			pb := partitionCount / ws * w
			for p := pb; p < partitionCount; p++ {
				f(p, w)
			}
			for p := uint64(0); p < pb; p++ {
				f(p, w)
			}
			wg.Done()
		}(w)
	}
	wg.Wait()
}

func (vs *DefaultValueStore) tombstoneDiscardPassExpiredDeletions() {
	rightwardPartitionShift := uint16(0)
	if vs.ring != nil {
		rightwardPartitionShift = 64 - vs.ring.PartitionBits()
	}
	partitionCount := uint64(1) << vs.ring.PartitionBits()
	ws := uint64(vs.workers)
	f := func(p uint64, w uint64, lr []localRemovalEntry) {
		pb := p << rightwardPartitionShift
		rb := pb + ((uint64(1) << rightwardPartitionShift) / ws * w)
		var re uint64
		if w+1 == ws {
			if p+1 == partitionCount {
				re = math.MaxUint64
			} else {
				re = ((p + 1) << rightwardPartitionShift) - 1
			}
		} else {
			re = pb + ((uint64(1) << rightwardPartitionShift) / ws * (w + 1)) - 1
		}
		cutoff := (uint64(brimtime.TimeToUnixMicro(time.Now())) << _TSB_UTIL_BITS) - vs.tombstoneDiscardState.age
		var more bool
		for {
			lri := 0
			rb, more = vs.vlm.ScanCallbackV2(rb, re, _TSB_DELETION, _TSB_LOCAL_REMOVAL, cutoff, _GLH_TOMBSTONE_DISCARD_BATCH_SIZE, func(keyA uint64, keyB uint64, timestampbits uint64, length uint32) {
				e := &lr[lri]
				e.keyA = keyA
				e.keyB = keyB
				e.timestampbits = timestampbits
				lri++
			})
			for i := 0; i < lri; i++ {
				e := &lr[i]
				vs.write(e.keyA, e.keyB, e.timestampbits|_TSB_LOCAL_REMOVAL, nil)
			}
			if !more {
				break
			}
		}
	}
	for len(vs.tombstoneDiscardState.localRemovals) < int(ws) {
		vs.tombstoneDiscardState.localRemovals = append(vs.tombstoneDiscardState.localRemovals, make([]localRemovalEntry, _GLH_TOMBSTONE_DISCARD_BATCH_SIZE))
	}
	wg := &sync.WaitGroup{}
	wg.Add(int(ws))
	for w := uint64(0); w < ws; w++ {
		go func(w uint64) {
			lr := vs.tombstoneDiscardState.localRemovals[w]
			pb := partitionCount / ws * w
			for p := pb; p < partitionCount; p++ {
				f(p, w, lr)
			}
			for p := uint64(0); p < pb; p++ {
				f(p, w, lr)
			}
			wg.Done()
		}(w)
	}
	wg.Wait()
}
