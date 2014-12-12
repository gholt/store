package valuestore

import (
	"encoding/binary"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/brimtime"
	"github.com/gholt/experimental-ring"
)

const _GLH_IN_PULL_REPLICATION_MSGS = 128
const _GLH_IN_PULL_REPLICATION_HANDLERS = 40
const _GLH_OUT_PULL_REPLICATION_MSGS = 128
const _GLH_BLOOM_FILTER_N = 1000000
const _GLH_BLOOM_FILTER_P = 0.001
const pullReplicationMsgHeaderBytes = 44
const _GLH_IN_PULL_REPLICATION_MSG_TIMEOUT = 300

type pullReplicationMsg struct {
	vs     *DefaultValueStore
	header []byte
	body   []byte
}

// DisableOutPullReplication will stop any outgoing pull replication requests
// until EnableOutPullReplication is called.
func (vs *DefaultValueStore) DisableOutPullReplication() {
	c := make(chan struct{}, 1)
	vs.outPullReplicationNotifyChan <- &backgroundNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableOutPullReplication will resume outgoing pull replication requests.
func (vs *DefaultValueStore) EnableOutPullReplication() {
	c := make(chan struct{}, 1)
	vs.outPullReplicationNotifyChan <- &backgroundNotification{
		enable:   true,
		doneChan: c,
	}
	<-c
}

func (vs *DefaultValueStore) inPullReplication() {
	k := make([]uint64, 2*1024*1024)
	v := make([]byte, vs.maxValueSize)
	for {
		prm := <-vs.inPullReplicationChan
		k = k[:0]
		cutoff := prm.cutoff()
		tombstoneCutoff := (uint64(brimtime.TimeToUnixMicro(time.Now())) << _TSB_UTIL_BITS) - vs.tombstoneAge
		ktbf := prm.ktBloomFilter()
		l := int64(_GLH_OUT_BULK_SET_MSG_SIZE)
		vs.vlm.ScanCallback(prm.rangeStart(), prm.rangeStop(), func(keyA uint64, keyB uint64, timestampbits uint64, length uint32) {
			if l > 0 {
				if timestampbits&_TSB_LOCAL_REMOVAL == 0 && timestampbits < cutoff && (timestampbits&_TSB_DELETION == 0 || timestampbits >= tombstoneCutoff) {
					if !ktbf.mayHave(keyA, keyB, timestampbits) {
						k = append(k, keyA, keyB)
						// bsm: keyA:8, keyB:8, timestampbits:8, length:4,
						//      value:n
						l -= 28 + int64(length)
					}
				}
			}
		})
		nodeID := prm.nodeID()
		vs.freeInPullReplicationChan <- prm
		if len(k) > 0 {
			bsm := vs.newOutBulkSetMsg()
			var t uint64
			var err error
			for i := 0; i < len(k); i += 2 {
				t, v, err = vs.read(k[i], k[i+1], v[:0])
				if err == ErrNotFound {
					if t == 0 {
						continue
					}
				} else if err != nil {
					continue
				}
				if t&_TSB_LOCAL_REMOVAL == 0 {
					if !bsm.add(k[i], k[i+1], t, v) {
						break
					}
				}
			}
			if len(bsm.body) > 0 {
				if !vs.ring.MsgToNode(nodeID, bsm) {
					bsm.Done()
				}
			}
		}
	}
}

// OutPullReplicationPass will immediately execute an outgoing pull replication
// pass rather than waiting for the next interval. If a pass is currently
// executing, it will be stopped and restarted so that a call to this function
// ensures one complete pass occurs. Note that this pass will send the outgoing
// pull replication requests, but all the responses will almost certainly not
// have been received when this function returns. These requests are stateless,
// and so synchronization at that level is not possible.
func (vs *DefaultValueStore) OutPullReplicationPass() {
	atomic.StoreUint32(&vs.outPullReplicationAbort, 1)
	c := make(chan struct{}, 1)
	vs.outPullReplicationNotifyChan <- &backgroundNotification{doneChan: c}
	<-c
}

func (vs *DefaultValueStore) outPullReplicationLauncher() {
	var enabled bool
	interval := float64(vs.outPullReplicationInterval) * float64(time.Second)
	nextRun := time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
	for {
		var notification *backgroundNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-vs.outPullReplicationNotifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-vs.outPullReplicationNotifyChan:
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
				atomic.StoreUint32(&vs.outPullReplicationAbort, 1)
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&vs.outPullReplicationAbort, 0)
			vs.outPullReplicationPass()
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&vs.outPullReplicationAbort, 0)
			vs.outPullReplicationPass()
		}
	}
}

func (vs *DefaultValueStore) outPullReplicationPass() {
	if vs.ring == nil {
		return
	}
	if vs.logDebug != nil {
		begin := time.Now()
		defer func() {
			vs.logDebug.Printf("out pull replication pass took %s", time.Now().Sub(begin))
		}()
	}
	if vs.outPullReplicationIteration == math.MaxUint16 {
		vs.outPullReplicationIteration = 0
	} else {
		vs.outPullReplicationIteration++
	}
	ringID := vs.ring.ID()
	partitionPower := vs.ring.PartitionPower()
	partitions := uint32(1) << partitionPower
	for len(vs.outPullReplicationKTBFs) < vs.outPullReplicationWorkers {
		vs.outPullReplicationKTBFs = append(vs.outPullReplicationKTBFs, newKTBloomFilter(_GLH_BLOOM_FILTER_N, _GLH_BLOOM_FILTER_P, 0))
	}
	f := func(p uint32, ktbf *ktBloomFilter) {
		start := uint64(p) << uint64(64-partitionPower)
		stop := start + (uint64(1)<<(64-partitionPower) - 1)
		pullSize := uint64(1) << (64 - partitionPower)
		for vs.vlm.ScanCount(start, start+(pullSize-1), _GLH_BLOOM_FILTER_N) >= _GLH_BLOOM_FILTER_N {
			pullSize /= 2
		}
		timestampbitsnow := uint64(brimtime.TimeToUnixMicro(time.Now())) << _TSB_UTIL_BITS
		cutoff := timestampbitsnow - vs.replicationIgnoreRecent
		tombstoneCutoff := timestampbitsnow - vs.tombstoneAge
		substart := start
		substop := start + (pullSize - 1)
		for atomic.LoadUint32(&vs.outPullReplicationAbort) == 0 {
			ktbf.reset(vs.outPullReplicationIteration)
			vs.vlm.ScanCallback(substart, substop, func(keyA uint64, keyB uint64, timestampbits uint64, length uint32) {
				if timestampbits&_TSB_LOCAL_REMOVAL == 0 && timestampbits < cutoff && (timestampbits&_TSB_DELETION == 0 || timestampbits >= tombstoneCutoff) {
					ktbf.add(keyA, keyB, timestampbits)
				}
			})
			if atomic.LoadUint32(&vs.outPullReplicationAbort) != 0 {
				break
			}
			prm := vs.newOutPullReplicationMsg(ringID, p, cutoff, substart, substop, ktbf)
			if !vs.ring.MsgToOtherReplicas(ringID, p, prm) {
				prm.Done()
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
	wg.Add(vs.outPullReplicationWorkers)
	for g := 0; g < vs.outPullReplicationWorkers; g++ {
		go func(g uint32) {
			ktbf := vs.outPullReplicationKTBFs[g]
			for p := uint32(sp + g); p < partitions && atomic.LoadUint32(&vs.outPullReplicationAbort) == 0; p += uint32(vs.outPullReplicationWorkers) {
				if vs.ring.ID() != ringID {
					break
				}
				if vs.ring.Responsible(p) {
					f(p, ktbf)
				}
			}
			for p := uint32(g); p < sp && atomic.LoadUint32(&vs.outPullReplicationAbort) == 0; p += uint32(vs.outPullReplicationWorkers) {
				if vs.ring.ID() != ringID {
					break
				}
				if vs.ring.Responsible(p) {
					f(p, ktbf)
				}
			}
			wg.Done()
		}(uint32(g))
	}
	wg.Wait()
}

var toss []byte = make([]byte, 65536)

func (vs *DefaultValueStore) newInPullReplicationMsg(r io.Reader, l uint64) (uint64, error) {
	var prm *pullReplicationMsg
	select {
	case prm = <-vs.freeInPullReplicationChan:
	case <-time.After(_GLH_IN_PULL_REPLICATION_MSG_TIMEOUT * time.Second):
		var n uint64
		var sn int
		var err error
		for n < l {
			sn, err = r.Read(toss)
			n += uint64(sn)
			if err != nil {
				return n, err
			}
		}
		return n, nil
	}
	bl := l - pullReplicationMsgHeaderBytes - uint64(ktBloomFilterHeaderBytes)
	if uint64(cap(prm.body)) < bl {
		prm.body = make([]byte, bl)
	}
	prm.body = prm.body[:bl]
	var n int
	var sn int
	var err error
	for n != len(prm.header) {
		if err != nil {
			return uint64(n), err
		}
		sn, err = r.Read(prm.header[n:])
		n += sn
	}
	n = 0
	for n != len(prm.body) {
		if err != nil {
			return uint64(len(prm.header)) + uint64(n), err
		}
		sn, err = r.Read(prm.body[n:])
		n += sn
	}
	vs.inPullReplicationChan <- prm
	return l, nil
}

func (vs *DefaultValueStore) newOutPullReplicationMsg(ringID uint64, partition uint32, cutoff uint64, rangeStart uint64, rangeStop uint64, ktbf *ktBloomFilter) *pullReplicationMsg {
	prm := <-vs.outPullReplicationChan
	binary.BigEndian.PutUint64(prm.header, vs.ring.NodeID())
	binary.BigEndian.PutUint64(prm.header[8:], ringID)
	binary.BigEndian.PutUint32(prm.header[16:], partition)
	binary.BigEndian.PutUint64(prm.header[20:], cutoff)
	binary.BigEndian.PutUint64(prm.header[28:], rangeStart)
	binary.BigEndian.PutUint64(prm.header[36:], rangeStop)
	ktbf.toMsg(prm, pullReplicationMsgHeaderBytes)
	return prm
}

func (prm *pullReplicationMsg) MsgType() ring.MsgType {
	return ring.MSG_PULL_REPLICATION
}

func (prm *pullReplicationMsg) MsgLength() uint64 {
	return uint64(len(prm.header)) + uint64(len(prm.body))
}

func (prm *pullReplicationMsg) nodeID() uint64 {
	return binary.BigEndian.Uint64(prm.header)
}

func (prm *pullReplicationMsg) ringID() uint64 {
	return binary.BigEndian.Uint64(prm.header[8:])
}

func (prm *pullReplicationMsg) partition() uint32 {
	return binary.BigEndian.Uint32(prm.header[16:])
}

func (prm *pullReplicationMsg) cutoff() uint64 {
	return binary.BigEndian.Uint64(prm.header[20:])
}

func (prm *pullReplicationMsg) rangeStart() uint64 {
	return binary.BigEndian.Uint64(prm.header[28:])
}

func (prm *pullReplicationMsg) rangeStop() uint64 {
	return binary.BigEndian.Uint64(prm.header[36:])
}

func (prm *pullReplicationMsg) ktBloomFilter() *ktBloomFilter {
	return newKTBloomFilterFromMsg(prm, pullReplicationMsgHeaderBytes)
}

func (prm *pullReplicationMsg) WriteContent(w io.Writer) (uint64, error) {
	var n int
	var sn int
	var err error
	sn, err = w.Write(prm.header)
	n += sn
	if err != nil {
		return uint64(n), err
	}
	sn, err = w.Write(prm.body)
	n += sn
	return uint64(n), err
}

func (prm *pullReplicationMsg) Done() {
	prm.vs.outPullReplicationChan <- prm
}
