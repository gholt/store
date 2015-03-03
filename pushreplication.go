package valuestore

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/brimtime-v1"
)

type pushReplicationState struct {
	outWorkers    int
	outInterval   int
	outNotifyChan chan *backgroundNotification
	outAbort      uint32
	outMsgChan    chan *pullReplicationMsg
	outLists      [][]uint64
	outValBufs    [][]byte
}

func (vs *DefaultValueStore) pushReplicationInit(cfg *config) {
	vs.pushReplicationState.outWorkers = cfg.outPushReplicationWorkers
	vs.pushReplicationState.outInterval = cfg.outPushReplicationInterval
	if vs.ring != nil {
		vs.pushReplicationState.outMsgChan = make(chan *pullReplicationMsg, _GLH_OUT_PULL_REPLICATION_MSGS)
	}
	vs.pushReplicationState.outNotifyChan = make(chan *backgroundNotification, 1)
	go vs.outPushReplicationLauncher()
}

// DisableOutPushReplication will stop any outgoing push replication requests
// until EnableOutPushReplication is called.
func (vs *DefaultValueStore) DisableOutPushReplication() {
	c := make(chan struct{}, 1)
	vs.pushReplicationState.outNotifyChan <- &backgroundNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableOutPushReplication will resume outgoing push replication requests.
func (vs *DefaultValueStore) EnableOutPushReplication() {
	c := make(chan struct{}, 1)
	vs.pushReplicationState.outNotifyChan <- &backgroundNotification{
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
func (vs *DefaultValueStore) OutPushReplicationPass() {
	atomic.StoreUint32(&vs.pushReplicationState.outAbort, 1)
	c := make(chan struct{}, 1)
	vs.pushReplicationState.outNotifyChan <- &backgroundNotification{doneChan: c}
	<-c
}

func (vs *DefaultValueStore) outPushReplicationLauncher() {
	var enabled bool
	interval := float64(vs.pushReplicationState.outInterval) * float64(time.Second)
	nextRun := time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
	for {
		var notification *backgroundNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-vs.pushReplicationState.outNotifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-vs.pushReplicationState.outNotifyChan:
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
				atomic.StoreUint32(&vs.pushReplicationState.outAbort, 1)
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&vs.pushReplicationState.outAbort, 0)
			vs.outPushReplicationPass()
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&vs.pushReplicationState.outAbort, 0)
			vs.outPushReplicationPass()
		}
	}
}

func (vs *DefaultValueStore) outPushReplicationPass() {
	if vs.ring == nil {
		return
	}
	if vs.logDebug != nil {
		begin := time.Now()
		defer func() {
			vs.logDebug.Printf("out push replication pass took %s", time.Now().Sub(begin))
		}()
	}
	// TODO: Instead of using this version change thing to indicate ring
	// changes, we should atomic store/load the vs.ring pointer.
	ringVersion := vs.ring.Version()
	rightwardPartitionShift := 64 - vs.ring.PartitionBitCount()
	partitionCount := uint32(1) << vs.ring.PartitionBitCount()
	for len(vs.pushReplicationState.outLists) < vs.pushReplicationState.outWorkers {
		vs.pushReplicationState.outLists = append(vs.pushReplicationState.outLists, make([]uint64, 2*1024*1024))
	}
	for len(vs.pushReplicationState.outValBufs) < vs.pushReplicationState.outWorkers {
		vs.pushReplicationState.outValBufs = append(vs.pushReplicationState.outValBufs, make([]byte, vs.maxValueSize))
	}
	// GLH TODO: Redo this to split up work like tombstoneDiscard does. Also,
	// don't do ScanCount, instead do something like tombstoneDiscard does as
	// well where the ScanCallback stops after max items and reports where it
	// stopped for the next scan. Downside is resending some data do to
	// overlap, need to calculate if that's a huge problem or just a minor one
	// worth the simplicity and (hopefully) overall speed increase. Might just
	// do one area scan per interval to give a better chance of getting acked
	// before scanning the same area again; meaning less or no resends.
	f := func(p uint32, list []uint64, valbuf []byte) {
		list = list[:0]
		start := uint64(p) << uint64(rightwardPartitionShift)
		stop := start + (uint64(1)<<rightwardPartitionShift - 1)
		pullSize := uint64(1) << rightwardPartitionShift
		for vs.vlm.ScanCount(start, start+(pullSize-1), _GLH_BLOOM_FILTER_N) >= _GLH_BLOOM_FILTER_N {
			pullSize /= 2
		}
		timestampbitsnow := uint64(brimtime.TimeToUnixMicro(time.Now())) << _TSB_UTIL_BITS
		cutoff := timestampbitsnow - vs.replicationIgnoreRecent
		tombstoneCutoff := timestampbitsnow - vs.tombstoneDiscardState.age
		substart := start
		substop := start + (pullSize - 1)
		for atomic.LoadUint32(&vs.pushReplicationState.outAbort) == 0 {
			l := int64(_GLH_OUT_BULK_SET_MSG_SIZE)
			vs.vlm.ScanCallback(substart, substop, func(keyA uint64, keyB uint64, timestampbits uint64, length uint32) {
				if l > 0 {
					if timestampbits&_TSB_LOCAL_REMOVAL == 0 && timestampbits < cutoff && (timestampbits&_TSB_DELETION == 0 || timestampbits >= tombstoneCutoff) {
						list = append(list, keyA, keyB)
						// bsm: keyA:8, keyB:8, timestampbits:8, length:4,
						//      value:n
						l -= 28 + int64(length)
					}
				}
			})
			if atomic.LoadUint32(&vs.pushReplicationState.outAbort) != 0 {
				break
			}
			if len(list) > 0 {
				bsm := vs.newOutBulkSetMsg()
				binary.BigEndian.PutUint64(bsm.header, vs.ring.LocalNode().NodeID())
				var timestampbits uint64
				var err error
				for i := 0; i < len(list); i += 2 {
					timestampbits, valbuf, err = vs.read(list[i], list[i+1], valbuf[:0])
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
					}
				}
				if atomic.LoadUint32(&vs.pushReplicationState.outAbort) != 0 {
					bsm.Done()
					break
				}
				vs.ring.MsgToOtherReplicas(ringVersion, p, bsm)
			}
			substart += pullSize
			substop += pullSize
			if substop > stop || substop < stop {
				break
			}
		}
	}
	sp := uint32(vs.rand.Intn(int(partitionCount)))
	wg := &sync.WaitGroup{}
	wg.Add(vs.pushReplicationState.outWorkers)
	for g := 0; g < vs.pushReplicationState.outWorkers; g++ {
		go func(g uint32) {
			list := vs.pushReplicationState.outLists[g]
			valbuf := vs.pushReplicationState.outValBufs[g]
			spg := uint32(sp + g)
			for p := spg; p < partitionCount && atomic.LoadUint32(&vs.pushReplicationState.outAbort) == 0; p += uint32(vs.pushReplicationState.outWorkers) {
				if vs.ring.Version() != ringVersion {
					break
				}
				if !vs.ring.Responsible(p) {
					f(p, list, valbuf)
				}
			}
			for p := uint32(g); p < spg && atomic.LoadUint32(&vs.pushReplicationState.outAbort) == 0; p += uint32(vs.pushReplicationState.outWorkers) {
				if vs.ring.Version() != ringVersion {
					break
				}
				if !vs.ring.Responsible(p) {
					f(p, list, valbuf)
				}
			}
			wg.Done()
		}(uint32(g))
	}
	wg.Wait()
}
