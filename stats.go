package valuestore

import (
	"fmt"
	"sync/atomic"
	"time"

	"gopkg.in/gholt/brimtext.v1"
)

type Stats struct {
	// Values is the number of values in the ValueStore.
	Values uint64
	// ValuesBytes is the number of bytes of the values in the ValueStore.
	ValueBytes uint64
	// Lookups is the number of calls to Lookup.
	Lookups int32
	// LookupErrors is the number of errors returned by Lookup.
	LookupErrors int32
	// Reads is the number of calls to Read.
	Reads int32
	// ReadErrors is the number of errors returned by Read.
	ReadErrors int32
	// Writes is the number of calls to Write.
	Writes int32
	// WriteErrors is the number of errors returned by Write.
	WriteErrors int32
	// WritesOverridden is the number of calls to Write that resulted in no
	// change.
	WritesOverridden int32
	// Deletes is the number of calls to Delete.
	Deletes int32
	// DeleteErrors is the number of errors returned by Delete.
	DeleteErrors int32
	// DeletesOverridden is the number of calls to Delete that resulted in no
	// change.
	DeletesOverridden int32
	// InBulkSets is the number of incoming bulk-set messages.
	InBulkSets int32
	// InBulkSetDrops is the number of incoming bulk-set messages dropped due
	// to the local system being overworked at the time.
	InBulkSetDrops int32
	// InBulkSetInvalids is the number of incoming bulk-set messages that
	// couldn't be parsed.
	InBulkSetInvalids int32
	// InBulkSetWrites is the number of writes due to incoming bulk-set
	// messages.
	InBulkSetWrites int32
	// InBulkSetWriteErrors is the number of errors returned from writes due to
	// incoming bulk-set messages.
	InBulkSetWriteErrors int32
	// InBulkSetWritesOverridden is the number of writes from incoming bulk-set
	// messages that result in no change.
	InBulkSetWritesOverridden int32
	// OutBulkSetAcks is the number of outgoing bulk-set-ack messages.
	OutBulkSetAcks int32
	// InBulkSetAcks is the number of incoming bulk-set-ack messages.
	InBulkSetAcks int32
	// InBulkSetAckDrops is the number of incoming bulk-set-ack messages
	// dropped due to the local system being overworked at the time.
	InBulkSetAckDrops int32
	// InBulkSetAckInvalids is the number of incoming bulk-set-ack messages
	// that couldn't be parsed.
	InBulkSetAckInvalids int32
	// InBulkSetAckWrites is the number of writes (for local removal) due to
	// incoming bulk-set-ack messages.
	InBulkSetAckWrites int32
	// InBulkSetAckWriteErrors is the number of errors returned from writes due
	// to incoming bulk-set-ack messages.
	InBulkSetAckWriteErrors int32
	// InBulkSetAckWritesOverridden is the number of writes from incoming
	// bulk-set-ack messages that result in no change.
	InBulkSetAckWritesOverridden int32

	debug                      bool
	freeableVMChansCap         int
	freeableVMChansIn          int
	freeVMChanCap              int
	freeVMChanIn               int
	freeVWRChans               int
	freeVWRChansCap            int
	freeVWRChansIn             int
	pendingVWRChans            int
	pendingVWRChansCap         int
	pendingVWRChansIn          int
	vfVMChanCap                int
	vfVMChanIn                 int
	freeTOCBlockChanCap        int
	freeTOCBlockChanIn         int
	pendingTOCBlockChanCap     int
	pendingTOCBlockChanIn      int
	maxValueLocBlockID         uint64
	path                       string
	pathtoc                    string
	workers                    int
	tombstoneDiscardInterval   int
	outPullReplicationWorkers  uint64
	outPullReplicationInterval time.Duration
	outPushReplicationWorkers  int
	outPushReplicationInterval int
	valueCap                   uint32
	pageSize                   uint32
	minValueAlloc              int
	writePagesPerWorker        int
	tombstoneAge               int
	valuesFileCap              uint32
	valuesFileReaders          int
	checksumInterval           uint32
	replicationIgnoreRecent    int
	vlmDebugInfo               fmt.Stringer
}

// Stats returns overall information about the state of the ValueStore. Note
// that this is a relatively expensive call; debug = true will make it even
// more expensive.
//
// The public counter fields returned in the Stats will reset with each read.
// In other words, if Stats().WriteCount gives the value 10 and no more Writes
// occur before Stats() is called again, that second Stats().WriteCount will
// have the value 0.
//
// The various values reported when debug=true are left undocumented because
// they are subject to change based on implementation. They are only provided
// when the Stats.String() is called.
func (vs *DefaultValueStore) Stats(debug bool) fmt.Stringer {
	vs.statsLock.Lock()
	stats := &Stats{
		Lookups:                      atomic.LoadInt32(&vs.lookups),
		LookupErrors:                 atomic.LoadInt32(&vs.lookupErrors),
		Reads:                        atomic.LoadInt32(&vs.reads),
		ReadErrors:                   atomic.LoadInt32(&vs.readErrors),
		Writes:                       atomic.LoadInt32(&vs.writes),
		WriteErrors:                  atomic.LoadInt32(&vs.writeErrors),
		WritesOverridden:             atomic.LoadInt32(&vs.writesOverridden),
		Deletes:                      atomic.LoadInt32(&vs.deletes),
		DeleteErrors:                 atomic.LoadInt32(&vs.deleteErrors),
		DeletesOverridden:            atomic.LoadInt32(&vs.deletesOverridden),
		InBulkSets:                   atomic.LoadInt32(&vs.inBulkSets),
		InBulkSetDrops:               atomic.LoadInt32(&vs.inBulkSetDrops),
		InBulkSetInvalids:            atomic.LoadInt32(&vs.inBulkSetInvalids),
		InBulkSetWrites:              atomic.LoadInt32(&vs.inBulkSetWrites),
		InBulkSetWriteErrors:         atomic.LoadInt32(&vs.inBulkSetWriteErrors),
		InBulkSetWritesOverridden:    atomic.LoadInt32(&vs.inBulkSetWritesOverridden),
		OutBulkSetAcks:               atomic.LoadInt32(&vs.outBulkSetAcks),
		InBulkSetAcks:                atomic.LoadInt32(&vs.inBulkSetAcks),
		InBulkSetAckDrops:            atomic.LoadInt32(&vs.inBulkSetAckDrops),
		InBulkSetAckInvalids:         atomic.LoadInt32(&vs.inBulkSetAckInvalids),
		InBulkSetAckWrites:           atomic.LoadInt32(&vs.inBulkSetAckWrites),
		InBulkSetAckWriteErrors:      atomic.LoadInt32(&vs.inBulkSetAckWriteErrors),
		InBulkSetAckWritesOverridden: atomic.LoadInt32(&vs.inBulkSetAckWritesOverridden),
	}
	atomic.AddInt32(&vs.lookups, -stats.Lookups)
	atomic.AddInt32(&vs.lookupErrors, -stats.LookupErrors)
	atomic.AddInt32(&vs.reads, -stats.Reads)
	atomic.AddInt32(&vs.readErrors, -stats.ReadErrors)
	atomic.AddInt32(&vs.writes, -stats.Writes)
	atomic.AddInt32(&vs.writeErrors, -stats.WriteErrors)
	atomic.AddInt32(&vs.writesOverridden, -stats.WritesOverridden)
	atomic.AddInt32(&vs.writes, -stats.Deletes)
	atomic.AddInt32(&vs.writeErrors, -stats.DeleteErrors)
	atomic.AddInt32(&vs.writesOverridden, -stats.DeletesOverridden)
	atomic.AddInt32(&vs.inBulkSets, -stats.InBulkSets)
	atomic.AddInt32(&vs.inBulkSetDrops, -stats.InBulkSetDrops)
	atomic.AddInt32(&vs.inBulkSetInvalids, -stats.InBulkSetInvalids)
	atomic.AddInt32(&vs.inBulkSetWrites, -stats.InBulkSetWrites)
	atomic.AddInt32(&vs.inBulkSetWriteErrors, -stats.InBulkSetWriteErrors)
	atomic.AddInt32(&vs.inBulkSetWritesOverridden, -stats.InBulkSetWritesOverridden)
	atomic.AddInt32(&vs.outBulkSetAcks, -stats.OutBulkSetAcks)
	atomic.AddInt32(&vs.inBulkSetAcks, -stats.InBulkSetAcks)
	atomic.AddInt32(&vs.inBulkSetAckDrops, -stats.InBulkSetAckDrops)
	atomic.AddInt32(&vs.inBulkSetAckInvalids, -stats.InBulkSetAckInvalids)
	atomic.AddInt32(&vs.inBulkSetAckWrites, -stats.InBulkSetAckWrites)
	atomic.AddInt32(&vs.inBulkSetAckWriteErrors, -stats.InBulkSetAckWriteErrors)
	atomic.AddInt32(&vs.inBulkSetAckWritesOverridden, -stats.InBulkSetAckWritesOverridden)
	vs.statsLock.Unlock()
	if !debug {
		vlmStats := vs.vlm.Stats(false)
		stats.Values = vlmStats.ActiveCount
		stats.ValueBytes = vlmStats.ActiveBytes
		stats.vlmDebugInfo = vlmStats
	} else {
		stats.debug = debug
		for i := 0; i < len(vs.freeableVMChans); i++ {
			stats.freeableVMChansCap += cap(vs.freeableVMChans[i])
			stats.freeableVMChansIn += len(vs.freeableVMChans[i])
		}
		stats.freeVMChanCap = cap(vs.freeVMChan)
		stats.freeVMChanIn = len(vs.freeVMChan)
		stats.freeVWRChans = len(vs.freeVWRChans)
		for i := 0; i < len(vs.freeVWRChans); i++ {
			stats.freeVWRChansCap += cap(vs.freeVWRChans[i])
			stats.freeVWRChansIn += len(vs.freeVWRChans[i])
		}
		stats.pendingVWRChans = len(vs.pendingVWRChans)
		for i := 0; i < len(vs.pendingVWRChans); i++ {
			stats.pendingVWRChansCap += cap(vs.pendingVWRChans[i])
			stats.pendingVWRChansIn += len(vs.pendingVWRChans[i])
		}
		stats.vfVMChanCap = cap(vs.vfVMChan)
		stats.vfVMChanIn = len(vs.vfVMChan)
		stats.freeTOCBlockChanCap = cap(vs.freeTOCBlockChan)
		stats.freeTOCBlockChanIn = len(vs.freeTOCBlockChan)
		stats.pendingTOCBlockChanCap = cap(vs.pendingTOCBlockChan)
		stats.pendingTOCBlockChanIn = len(vs.pendingTOCBlockChan)
		stats.maxValueLocBlockID = atomic.LoadUint64(&vs.valueLocBlockIDer)
		stats.path = vs.path
		stats.pathtoc = vs.pathtoc
		stats.workers = vs.workers
		stats.tombstoneDiscardInterval = vs.tombstoneDiscardState.interval
		stats.outPullReplicationWorkers = vs.pullReplicationState.outWorkers
		stats.outPullReplicationInterval = vs.pullReplicationState.outInterval
		stats.outPushReplicationWorkers = vs.pushReplicationState.outWorkers
		stats.outPushReplicationInterval = vs.pushReplicationState.outInterval
		stats.valueCap = vs.valueCap
		stats.pageSize = vs.pageSize
		stats.minValueAlloc = vs.minValueAlloc
		stats.writePagesPerWorker = vs.writePagesPerWorker
		stats.tombstoneAge = int((vs.tombstoneDiscardState.age >> _TSB_UTIL_BITS) * 1000 / uint64(time.Second))
		stats.valuesFileCap = vs.valuesFileCap
		stats.valuesFileReaders = vs.valuesFileReaders
		stats.checksumInterval = vs.checksumInterval
		stats.replicationIgnoreRecent = int(vs.replicationIgnoreRecent / uint64(time.Second))
		vlmStats := vs.vlm.Stats(true)
		stats.Values = vlmStats.ActiveCount
		stats.ValueBytes = vlmStats.ActiveBytes
		stats.vlmDebugInfo = vlmStats
	}
	return stats
}

func (stats *Stats) String() string {
	report := [][]string{
		{"Values", fmt.Sprintf("%d", stats.Values)},
		{"ValueBytes", fmt.Sprintf("%d", stats.ValueBytes)},
		{"Lookups", fmt.Sprintf("%d", stats.Lookups)},
		{"LookupErrors", fmt.Sprintf("%d", stats.LookupErrors)},
		{"Reads", fmt.Sprintf("%d", stats.Reads)},
		{"ReadErrors", fmt.Sprintf("%d", stats.ReadErrors)},
		{"Writes", fmt.Sprintf("%d", stats.Writes)},
		{"WriteErrors", fmt.Sprintf("%d", stats.WriteErrors)},
		{"WritesOverridden", fmt.Sprintf("%d", stats.WritesOverridden)},
		{"Deletes", fmt.Sprintf("%d", stats.Deletes)},
		{"DeleteErrors", fmt.Sprintf("%d", stats.DeleteErrors)},
		{"DeletesOverridden", fmt.Sprintf("%d", stats.DeletesOverridden)},
		{"InBulkSets", fmt.Sprintf("%d", stats.InBulkSets)},
		{"InBulkSetDrops", fmt.Sprintf("%d", stats.InBulkSetDrops)},
		{"InBulkSetInvalids", fmt.Sprintf("%d", stats.InBulkSetInvalids)},
		{"InBulkSetWrites", fmt.Sprintf("%d", stats.InBulkSetWrites)},
		{"InBulkSetWriteErrors", fmt.Sprintf("%d", stats.InBulkSetWriteErrors)},
		{"InBulkSetWritesOverridden", fmt.Sprintf("%d", stats.InBulkSetWritesOverridden)},
		{"OutBulkSetAcks", fmt.Sprintf("%d", stats.OutBulkSetAcks)},
		{"InBulkSetAcks", fmt.Sprintf("%d", stats.InBulkSetAcks)},
		{"InBulkSetAckDrops", fmt.Sprintf("%d", stats.InBulkSetAckDrops)},
		{"InBulkSetAckInvalids", fmt.Sprintf("%d", stats.InBulkSetAckInvalids)},
		{"InBulkSetAckWrites", fmt.Sprintf("%d", stats.InBulkSetAckWrites)},
		{"InBulkSetAckWriteErrors", fmt.Sprintf("%d", stats.InBulkSetAckWriteErrors)},
		{"InBulkSetAckWritesOverridden", fmt.Sprintf("%d", stats.InBulkSetAckWritesOverridden)},
	}
	if stats.debug {
		report = append(report, [][]string{
			nil,
			{"freeableVMChansCap", fmt.Sprintf("%d", stats.freeableVMChansCap)},
			{"freeableVMChansIn", fmt.Sprintf("%d", stats.freeableVMChansIn)},
			{"freeVMChanCap", fmt.Sprintf("%d", stats.freeVMChanCap)},
			{"freeVMChanIn", fmt.Sprintf("%d", stats.freeVMChanIn)},
			{"freeVWRChans", fmt.Sprintf("%d", stats.freeVWRChans)},
			{"freeVWRChansCap", fmt.Sprintf("%d", stats.freeVWRChansCap)},
			{"freeVWRChansIn", fmt.Sprintf("%d", stats.freeVWRChansIn)},
			{"pendingVWRChans", fmt.Sprintf("%d", stats.pendingVWRChans)},
			{"pendingVWRChansCap", fmt.Sprintf("%d", stats.pendingVWRChansCap)},
			{"pendingVWRChansIn", fmt.Sprintf("%d", stats.pendingVWRChansIn)},
			{"vfVMChanCap", fmt.Sprintf("%d", stats.vfVMChanCap)},
			{"vfVMChanIn", fmt.Sprintf("%d", stats.vfVMChanIn)},
			{"freeTOCBlockChanCap", fmt.Sprintf("%d", stats.freeTOCBlockChanCap)},
			{"freeTOCBlockChanIn", fmt.Sprintf("%d", stats.freeTOCBlockChanIn)},
			{"pendingTOCBlockChanCap", fmt.Sprintf("%d", stats.pendingTOCBlockChanCap)},
			{"pendingTOCBlockChanIn", fmt.Sprintf("%d", stats.pendingTOCBlockChanIn)},
			{"maxValueLocBlockID", fmt.Sprintf("%d", stats.maxValueLocBlockID)},
			{"path", stats.path},
			{"pathtoc", stats.pathtoc},
			{"workers", fmt.Sprintf("%d", stats.workers)},
			{"tombstoneDiscardInterval", fmt.Sprintf("%d", stats.tombstoneDiscardInterval)},
			{"outPullReplicationWorkers", fmt.Sprintf("%d", stats.outPullReplicationWorkers)},
			{"outPullReplicationInterval", fmt.Sprintf("%s", stats.outPullReplicationInterval)},
			{"outPushReplicationWorkers", fmt.Sprintf("%d", stats.outPushReplicationWorkers)},
			{"outPushReplicationInterval", fmt.Sprintf("%d", stats.outPushReplicationInterval)},
			{"valueCap", fmt.Sprintf("%d", stats.valueCap)},
			{"pageSize", fmt.Sprintf("%d", stats.pageSize)},
			{"minValueAlloc", fmt.Sprintf("%d", stats.minValueAlloc)},
			{"writePagesPerWorker", fmt.Sprintf("%d", stats.writePagesPerWorker)},
			{"tombstoneAge", fmt.Sprintf("%d", stats.tombstoneAge)},
			{"valuesFileCap", fmt.Sprintf("%d", stats.valuesFileCap)},
			{"valuesFileReaders", fmt.Sprintf("%d", stats.valuesFileReaders)},
			{"checksumInterval", fmt.Sprintf("%d", stats.checksumInterval)},
			{"replicationIgnoreRecent", fmt.Sprintf("%d", stats.replicationIgnoreRecent)},
			{"vlmDebugInfo", stats.vlmDebugInfo.String()},
		}...)
	}
	return brimtext.Align(report, nil)
}
