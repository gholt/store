package store

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/gholt/brimtime.v1"
)

type valuePushReplicationState struct {
	outWorkers    int
	outInterval   int
	outNotifyChan chan *backgroundNotification
	outAbort      uint32
	outMsgChan    chan *valuePullReplicationMsg
	outLists      [][]uint64
	outValBufs    [][]byte
	outMsgTimeout time.Duration
}

func (store *DefaultValueStore) pushReplicationConfig(cfg *ValueStoreConfig) {
	store.pushReplicationState.outWorkers = cfg.OutPushReplicationWorkers
	store.pushReplicationState.outInterval = cfg.OutPushReplicationInterval
	if store.msgRing != nil {
		store.pushReplicationState.outMsgChan = make(chan *valuePullReplicationMsg, cfg.OutPushReplicationMsgs)
	}
	store.pushReplicationState.outNotifyChan = make(chan *backgroundNotification, 1)
	store.pushReplicationState.outMsgTimeout = time.Duration(cfg.OutPushReplicationMsgTimeout) * time.Millisecond
}

func (store *DefaultValueStore) pushReplicationLaunch() {
	go store.outPushReplicationLauncher()
}

// DisableOutPushReplication will stop any outgoing push replication requests
// until EnableOutPushReplication is called.
func (store *DefaultValueStore) DisableOutPushReplication() {
	c := make(chan struct{}, 1)
	store.pushReplicationState.outNotifyChan <- &backgroundNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableOutPushReplication will resume outgoing push replication requests.
func (store *DefaultValueStore) EnableOutPushReplication() {
	c := make(chan struct{}, 1)
	store.pushReplicationState.outNotifyChan <- &backgroundNotification{
		enable:   true,
		doneChan: c,
	}
	<-c
}

// OutPushReplicationPass will immediately execute an outgoing push replication
// pass rather than waiting for the next interval. If a pass is currently
// executing, it will be stopped and restarted so that a call to this function
// ensures one complete pass occurs. Note that this pass will send the outgoing
// push replication requests, but all the responses will almost certainly not
// have been received when this function returns. These requests are stateless,
// and so synchronization at that level is not possible.
func (store *DefaultValueStore) OutPushReplicationPass() {
	atomic.StoreUint32(&store.pushReplicationState.outAbort, 1)
	c := make(chan struct{}, 1)
	store.pushReplicationState.outNotifyChan <- &backgroundNotification{doneChan: c}
	<-c
}

func (store *DefaultValueStore) outPushReplicationLauncher() {
	var enabled bool
	interval := float64(store.pushReplicationState.outInterval) * float64(time.Second)
	store.randMutex.Lock()
	nextRun := time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
	store.randMutex.Unlock()
	for {
		var notification *backgroundNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-store.pushReplicationState.outNotifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-store.pushReplicationState.outNotifyChan:
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
				atomic.StoreUint32(&store.pushReplicationState.outAbort, 1)
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&store.pushReplicationState.outAbort, 0)
			store.outPushReplicationPass()
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&store.pushReplicationState.outAbort, 0)
			store.outPushReplicationPass()
		}
	}
}

func (store *DefaultValueStore) outPushReplicationPass() {
	if store.msgRing == nil {
		return
	}
	if store.logDebug != nil {
		begin := time.Now()
		defer func() {
			store.logDebug("out push replication pass took %s\n", time.Now().Sub(begin))
		}()
	}
	ring := store.msgRing.Ring()
	if ring == nil {
		return
	}
	ringVersion := ring.Version()
	pbc := ring.PartitionBitCount()
	partitionShift := uint64(64 - pbc)
	partitionMax := (uint64(1) << pbc) - 1
	workerMax := uint64(store.pushReplicationState.outWorkers - 1)
	workerPartitionPiece := (uint64(1) << partitionShift) / (workerMax + 1)
	// To avoid memory churn, the scratchpad areas are allocated just once and
	// passed in to the workers.
	for len(store.pushReplicationState.outLists) < int(workerMax+1) {
		store.pushReplicationState.outLists = append(store.pushReplicationState.outLists, make([]uint64, store.bulkSetState.msgCap/_VALUE_BULK_SET_MSG_MIN_ENTRY_LENGTH*2))
	}
	for len(store.pushReplicationState.outValBufs) < int(workerMax+1) {
		store.pushReplicationState.outValBufs = append(store.pushReplicationState.outValBufs, make([]byte, store.valueCap))
	}
	work := func(partition uint64, worker uint64, list []uint64, valbuf []byte) {
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
		timestampbitsNow := uint64(brimtime.TimeToUnixMicro(time.Now())) << _TSB_UTIL_BITS
		cutoff := timestampbitsNow - store.replicationIgnoreRecent
		tombstoneCutoff := timestampbitsNow - store.tombstoneDiscardState.age
		availableBytes := int64(store.bulkSetState.msgCap)
		list = list[:0]
		// We ignore the "more" option from ScanCallback and just send the
		// first matching batch each full iteration. Once a remote end acks the
		// batch, those keys will have been removed and the first matching
		// batch will start with any remaining keys.
		// First we gather the matching keys to send.
		store.locmap.ScanCallback(rangeBegin, rangeEnd, 0, _TSB_LOCAL_REMOVAL, cutoff, math.MaxUint64, func(keyA uint64, keyB uint64, timestampbits uint64, length uint32) bool {
			inMsgLength := _VALUE_BULK_SET_MSG_ENTRY_HEADER_LENGTH + int64(length)
			if timestampbits&_TSB_DELETION == 0 || timestampbits >= tombstoneCutoff {
				list = append(list, keyA, keyB)
				availableBytes -= inMsgLength
				if availableBytes < inMsgLength {
					return false
				}
			}
			return true
		})
		if len(list) <= 0 || atomic.LoadUint32(&store.pushReplicationState.outAbort) != 0 {
			return
		}
		ring2 := store.msgRing.Ring()
		if ring2 == nil || ring2.Version() != ringVersion {
			return
		}
		// Then we build and send the actual message.
		bsm := store.newOutBulkSetMsg()
		var timestampbits uint64
		var err error
		for i := 0; i < len(list); i += 2 {
			timestampbits, valbuf, err = store.read(list[i], list[i+1], valbuf[:0])
			// This might mean we need to send a deletion or it might mean the
			// key has been completely removed from our records
			// (timestampbits==0).
			if err == ErrNotFound {
				if timestampbits == 0 {
					continue
				}
			} else if err != nil {
				continue
			}
			if timestampbits&_TSB_LOCAL_REMOVAL == 0 && timestampbits < cutoff && (timestampbits&_TSB_DELETION == 0 || timestampbits >= tombstoneCutoff) {
				if !bsm.add(list[i], list[i+1], timestampbits, valbuf) {
					break
				}
				atomic.AddInt32(&store.outBulkSetPushValues, 1)
			}
		}
		atomic.AddInt32(&store.outBulkSetPushes, 1)
		store.msgRing.MsgToOtherReplicas(bsm, uint32(partition), store.pushReplicationState.outMsgTimeout)
	}
	wg := &sync.WaitGroup{}
	wg.Add(int(workerMax + 1))
	for worker := uint64(0); worker <= workerMax; worker++ {
		go func(worker uint64) {
			list := store.pushReplicationState.outLists[worker]
			valbuf := store.pushReplicationState.outValBufs[worker]
			partitionBegin := (partitionMax + 1) / (workerMax + 1) * worker
			for partition := partitionBegin; ; {
				if atomic.LoadUint32(&store.pushReplicationState.outAbort) != 0 {
					break
				}
				ring2 := store.msgRing.Ring()
				if ring2 == nil || ring2.Version() != ringVersion {
					break
				}
				if !ring.Responsible(uint32(partition)) {
					work(partition, worker, list, valbuf)
				}
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
