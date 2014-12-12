package valuestore

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/brimtime"
)

// DisableOutPushReplication will stop any outgoing push replication requests
// until EnableOutPushReplication is called.
func (vs *DefaultValueStore) DisableOutPushReplication() {
	c := make(chan struct{}, 1)
	vs.outPushReplicationNotifyChan <- &backgroundNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableOutPushReplication will resume outgoing push replication requests.
func (vs *DefaultValueStore) EnableOutPushReplication() {
	c := make(chan struct{}, 1)
	vs.outPushReplicationNotifyChan <- &backgroundNotification{
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
	atomic.StoreUint32(&vs.outPushReplicationAbort, 1)
	c := make(chan struct{}, 1)
	vs.outPushReplicationNotifyChan <- &backgroundNotification{doneChan: c}
	<-c
}

func (vs *DefaultValueStore) outPushReplicationLauncher() {
	var enabled bool
	interval := float64(vs.outPushReplicationInterval) * float64(time.Second)
	nextRun := time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
	for {
		var notification *backgroundNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-vs.outPushReplicationNotifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-vs.outPushReplicationNotifyChan:
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
				atomic.StoreUint32(&vs.outPushReplicationAbort, 1)
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&vs.outPushReplicationAbort, 0)
			vs.outPushReplicationPass()
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&vs.outPushReplicationAbort, 0)
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
	ringID := vs.ring.ID()
	partitionPower := vs.ring.PartitionPower()
	partitions := uint32(1) << partitionPower
	for len(vs.outPushReplicationLists) < vs.outPushReplicationWorkers {
		vs.outPushReplicationLists = append(vs.outPushReplicationLists, make([]uint64, 2*1024*1024))
	}
	for len(vs.outPushReplicationValBufs) < vs.outPushReplicationWorkers {
		vs.outPushReplicationValBufs = append(vs.outPushReplicationValBufs, make([]byte, vs.maxValueSize))
	}
	f := func(p uint32, list []uint64, valbuf []byte) {
		list = list[:0]
		start := uint64(p) << uint64(64-partitionPower)
		stop := start + (uint64(1)<<(64-partitionPower) - 1)
		pullSize := uint64(1) << (64 - partitionPower)
		for vs.vlm.ScanCount(start, start+(pullSize-1), _GLH_BLOOM_FILTER_N) >= _GLH_BLOOM_FILTER_N {
			pullSize /= 2
		}
		timestampbitsnow := uint64(brimtime.TimeToUnixMicro(time.Now())) << _TSB_UTIL_BITS
		cutoff := timestampbitsnow - vs.replicationIgnoreRecent
		tombstoneCutoff := timestampbitsnow - vs.tombstoneDiscardState.age
		substart := start
		substop := start + (pullSize - 1)
		for atomic.LoadUint32(&vs.outPushReplicationAbort) == 0 {
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
			if atomic.LoadUint32(&vs.outPushReplicationAbort) != 0 {
				break
			}
			if len(list) > 0 {
				bsm := vs.newOutBulkSetMsg()
				binary.BigEndian.PutUint64(bsm.header, vs.ring.NodeID())
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
				if atomic.LoadUint32(&vs.outPushReplicationAbort) != 0 {
					bsm.Done()
					break
				}
				if !vs.ring.MsgToOtherReplicas(ringID, p, bsm) {
					bsm.Done()
				}
			}
			substart += pullSize
			substop += pullSize
			if substop > stop || substop < stop {
				break
			}
		}
	}
	sp := uint32(vs.rand.Intn(int(partitions)))
	wg := &sync.WaitGroup{}
	wg.Add(vs.outPushReplicationWorkers)
	for g := 0; g < vs.outPushReplicationWorkers; g++ {
		go func(g uint32) {
			list := vs.outPushReplicationLists[g]
			valbuf := vs.outPushReplicationValBufs[g]
			spg := uint32(sp + g)
			for p := spg; p < partitions && atomic.LoadUint32(&vs.outPushReplicationAbort) == 0; p += uint32(vs.outPushReplicationWorkers) {
				if vs.ring.ID() != ringID {
					break
				}
				if !vs.ring.Responsible(p) {
					f(p, list, valbuf)
				}
			}
			for p := uint32(g); p < spg && atomic.LoadUint32(&vs.outPushReplicationAbort) == 0; p += uint32(vs.outPushReplicationWorkers) {
				if vs.ring.ID() != ringID {
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
