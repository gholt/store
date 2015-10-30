package store

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/gholt/brimtime.v1"
)

type valueTombstoneDiscardState struct {
	interval      int
	age           uint64
	notifyChan    chan *backgroundNotification
	abort         uint32
	localRemovals [][]valueLocalRemovalEntry
	batchSize     int
}

type valueLocalRemovalEntry struct {
	keyA uint64
	keyB uint64

	timestampbits uint64
}

func (store *DefaultValueStore) tombstoneDiscardConfig(cfg *ValueStoreConfig) {
	store.tombstoneDiscardState.interval = cfg.TombstoneDiscardInterval
	store.tombstoneDiscardState.age = (uint64(cfg.TombstoneAge) * uint64(time.Second) / 1000) << _TSB_UTIL_BITS
	store.tombstoneDiscardState.notifyChan = make(chan *backgroundNotification, 1)
	store.tombstoneDiscardState.batchSize = cfg.TombstoneDiscardBatchSize
}

func (store *DefaultValueStore) tombstoneDiscardLaunch() {
	go store.tombstoneDiscardLauncher()
}

// DisableTombstoneDiscard will stop any discard passes until
// EnableTombstoneDiscard is called. A discard pass removes expired tombstones
// (deletion markers).
func (store *DefaultValueStore) DisableTombstoneDiscard() {
	c := make(chan struct{}, 1)
	store.tombstoneDiscardState.notifyChan <- &backgroundNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableTombstoneDiscard will resume discard passes. A discard pass removes
// expired tombstones (deletion markers).
func (store *DefaultValueStore) EnableTombstoneDiscard() {
	c := make(chan struct{}, 1)
	store.tombstoneDiscardState.notifyChan <- &backgroundNotification{
		enable:   true,
		doneChan: c,
	}
	<-c
}

// TombstoneDiscardPass will immediately execute a pass to discard expired
// tombstones (deletion markers) rather than waiting for the next interval. If
// a pass is currently executing, it will be stopped and restarted so that a
// call to this function ensures one complete pass occurs.
func (store *DefaultValueStore) TombstoneDiscardPass() {
	atomic.StoreUint32(&store.tombstoneDiscardState.abort, 1)
	c := make(chan struct{}, 1)
	store.tombstoneDiscardState.notifyChan <- &backgroundNotification{doneChan: c}
	<-c
}

func (store *DefaultValueStore) tombstoneDiscardLauncher() {
	var enabled bool
	interval := float64(store.tombstoneDiscardState.interval) * float64(time.Second)
	store.randMutex.Lock()
	nextRun := time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
	store.randMutex.Unlock()
	for {
		var notification *backgroundNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-store.tombstoneDiscardState.notifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-store.tombstoneDiscardState.notifyChan:
			default:
			}
		}
		store.randMutex.Lock()
		nextRun = time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
		store.randMutex.Unlock()
		if notification != nil {
			if notification.enable {
				enabled = true
				notification.doneChan <- struct{}{}
				continue
			}
			if notification.disable {
				atomic.StoreUint32(&store.tombstoneDiscardState.abort, 1)
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&store.tombstoneDiscardState.abort, 0)
			store.tombstoneDiscardPass()
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&store.tombstoneDiscardState.abort, 0)
			store.tombstoneDiscardPass()
		}
	}
}

func (store *DefaultValueStore) tombstoneDiscardPass() {
	if store.logDebug != nil {
		begin := time.Now()
		defer func() {
			store.logDebug("tombstone discard pass took %s\n", time.Now().Sub(begin))
		}()
	}
	store.tombstoneDiscardPassLocalRemovals()
	store.tombstoneDiscardPassExpiredDeletions()
}

// tombstoneDiscardPassLocalRemovals removes all entries marked with the
// _TSB_LOCAL_REMOVAL bit. These are entries that other routines have indicated
// are no longer needed in memory.
func (store *DefaultValueStore) tombstoneDiscardPassLocalRemovals() {
	// Each worker will perform a pass on a subsection of each partition's key
	// space. Additionally, each worker will start their work on different
	// partition. This reduces contention for a given section of the locmap.
	partitionShift := uint16(0)
	partitionMax := uint64(0)
	if store.msgRing != nil {
		pbc := store.msgRing.Ring().PartitionBitCount()
		partitionShift = 64 - pbc
		partitionMax = (uint64(1) << pbc) - 1
	}
	workerMax := uint64(store.workers - 1)
	workerPartitionPiece := (uint64(1) << partitionShift) / (workerMax + 1)
	work := func(partition uint64, worker uint64) {
		partitionOnLeftBits := partition << partitionShift
		rangeBegin := partitionOnLeftBits + (workerPartitionPiece * worker)
		var rangeEnd uint64
		// A little bit of complexity here to handle where the more general
		// expressions would have overflow issues.
		if worker != workerMax {
			rangeEnd = partitionOnLeftBits + (workerPartitionPiece * (worker + 1)) - 1
		} else {
			if partition != partitionMax {
				rangeEnd = ((partition + 1) << partitionShift) - 1
			} else {
				rangeEnd = math.MaxUint64
			}
		}
		store.locmap.Discard(rangeBegin, rangeEnd, _TSB_LOCAL_REMOVAL)
	}
	wg := &sync.WaitGroup{}
	wg.Add(int(workerMax + 1))
	workerPartitionOffset := (partitionMax + 1) / (workerMax + 1)
	for worker := uint64(0); worker <= workerMax; worker++ {
		go func(worker uint64) {
			partitionBegin := workerPartitionOffset * worker
			for partition := partitionBegin; partition <= partitionMax; partition++ {
				work(partition, worker)
			}
			for partition := uint64(0); partition < partitionBegin; partition++ {
				work(partition, worker)
			}
			wg.Done()
		}(worker)
	}
	wg.Wait()
}

// tombstoneDiscardPassExpiredDeletions scans for entries marked with
// _TSB_DELETION (but not _TSB_LOCAL_REMOVAL) that are older than the maximum
// tombstone age and marks them for _TSB_LOCAL_REMOVAL.
func (store *DefaultValueStore) tombstoneDiscardPassExpiredDeletions() {
	// Each worker will perform a pass on a subsection of each partition's key
	// space. Additionally, each worker will start their work on different
	// partition. This reduces contention for a given section of the locmap.
	partitionShift := uint16(0)
	partitionMax := uint64(0)
	if store.msgRing != nil {
		pbc := store.msgRing.Ring().PartitionBitCount()
		partitionShift = 64 - pbc
		partitionMax = (uint64(1) << pbc) - 1
	}
	workerMax := uint64(store.workers - 1)
	workerPartitionPiece := (uint64(1) << partitionShift) / (workerMax + 1)
	work := func(partition uint64, worker uint64, localRemovals []valueLocalRemovalEntry) {
		partitionOnLeftBits := partition << partitionShift
		rangeBegin := partitionOnLeftBits + (workerPartitionPiece * worker)
		var rangeEnd uint64
		// A little bit of complexity here to handle where the more general
		// expressions would have overflow issues.
		if worker != workerMax {
			rangeEnd = partitionOnLeftBits + (workerPartitionPiece * (worker + 1)) - 1
		} else {
			if partition != partitionMax {
				rangeEnd = ((partition + 1) << partitionShift) - 1
			} else {
				rangeEnd = math.MaxUint64
			}
		}
		cutoff := (uint64(brimtime.TimeToUnixMicro(time.Now())) << _TSB_UTIL_BITS) - store.tombstoneDiscardState.age
		more := true
		for more {
			localRemovalsIndex := 0
			// Since we shouldn't try to modify what we're scanning while we're
			// scanning (lock contention) we instead record in localRemovals
			// what to modify after the scan.
			rangeBegin, more = store.locmap.ScanCallback(rangeBegin, rangeEnd, _TSB_DELETION, _TSB_LOCAL_REMOVAL, cutoff, uint64(store.tombstoneDiscardState.batchSize), func(keyA uint64, keyB uint64, timestampbits uint64, length uint32) bool {
				e := &localRemovals[localRemovalsIndex]
				e.keyA = keyA
				e.keyB = keyB

				e.timestampbits = timestampbits
				localRemovalsIndex++
				return true
			})
			atomic.AddInt32(&store.expiredDeletions, int32(localRemovalsIndex))
			for i := 0; i < localRemovalsIndex; i++ {
				e := &localRemovals[i]
				// These writes go through the entire system, so they're
				// persisted and therefore restored on restarts.
				store.write(e.keyA, e.keyB, e.timestampbits|_TSB_LOCAL_REMOVAL, nil, true)
			}
		}
	}
	// To avoid memory churn, the localRemovals scratchpads are allocated just
	// once and passed in to the workers.
	for len(store.tombstoneDiscardState.localRemovals) <= int(workerMax) {
		store.tombstoneDiscardState.localRemovals = append(store.tombstoneDiscardState.localRemovals, make([]valueLocalRemovalEntry, store.tombstoneDiscardState.batchSize))
	}
	wg := &sync.WaitGroup{}
	wg.Add(int(workerMax + 1))
	for worker := uint64(0); worker <= workerMax; worker++ {
		go func(worker uint64) {
			localRemovals := store.tombstoneDiscardState.localRemovals[worker]
			partitionBegin := (partitionMax + 1) / (workerMax + 1) * worker
			for partition := partitionBegin; ; {
				work(partition, worker, localRemovals)
				partition++
				if partition > partitionMax {
					partition = 0
				}
				if partition == partitionBegin {
					break
				}
			}
			wg.Done()
		}(worker)
	}
	wg.Wait()
}
