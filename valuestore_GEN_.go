package valuestore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/ring"
	"github.com/gholt/valuelocmap"
	"github.com/spaolacci/murmur3"
	"gopkg.in/gholt/brimutil.v1"
)

// ValueStore is an interface for a disk-backed data structure that stores
// []byte values referenced by 128 bit keys with options for replication.
//
// For documentation on each of these functions, see the DefaultValueStore.
type ValueStore interface {
	Lookup(keyA uint64, keyB uint64) (int64, uint32, error)
	Read(keyA uint64, keyB uint64, value []byte) (int64, []byte, error)
	Write(keyA uint64, keyB uint64, timestamp int64, value []byte) (int64, error)
	Delete(keyA uint64, keyB uint64, timestamp int64) (int64, error)
	EnableAll()
	DisableAll()
	DisableAllBackground()
	EnableTombstoneDiscard()
	DisableTombstoneDiscard()
	TombstoneDiscardPass()
	EnableCompaction()
	DisableCompaction()
	CompactionPass()
	EnableOutPullReplication()
	DisableOutPullReplication()
	OutPullReplicationPass()
	EnableOutPushReplication()
	DisableOutPushReplication()
	OutPushReplicationPass()
	EnableWrites()
	DisableWrites()
	Flush()
	Stats(debug bool) fmt.Stringer
	ValueCap() uint32
}

// DefaultValueStore instances are created with NewValueStore.
type DefaultValueStore struct {
	logCritical             LogFunc
	logError                LogFunc
	logWarning              LogFunc
	logInfo                 LogFunc
	logDebug                LogFunc
	randMutex               sync.Mutex
	rand                    *rand.Rand
	freeableVMChans         []chan *valueMem
	freeVMChan              chan *valueMem
	freeVWRChans            []chan *valueWriteReq
	pendingVWRChans         []chan *valueWriteReq
	vfVMChan                chan *valueMem
	freeTOCBlockChan        chan []byte
	pendingTOCBlockChan     chan []byte
	activeTOCA              uint64
	activeTOCB              uint64
	flushedChan             chan struct{}
	locBlocks               []valueLocBlock
	locBlockIDer            uint64
	path                    string
	pathtoc                 string
	vlm                     valuelocmap.ValueLocMap
	workers                 int
	recoveryBatchSize       int
	valueCap                uint32
	pageSize                uint32
	minValueAlloc           int
	writePagesPerWorker     int
	fileCap                 uint32
	fileReaders             int
	checksumInterval        uint32
	msgRing                 ring.MsgRing
	tombstoneDiscardState   valueTombstoneDiscardState
	replicationIgnoreRecent uint64
	pullReplicationState    valuePullReplicationState
	pushReplicationState    valuePushReplicationState
	compactionState         valueCompactionState
	bulkSetState            valueBulkSetState
	bulkSetAckState         valueBulkSetAckState
	disableEnableWritesLock sync.Mutex
	userDisabled            bool
	diskWatcherState        valueDiskWatcherState

	statsLock                    sync.Mutex
	lookups                      int32
	lookupErrors                 int32
	reads                        int32
	readErrors                   int32
	writes                       int32
	writeErrors                  int32
	writesOverridden             int32
	deletes                      int32
	deleteErrors                 int32
	deletesOverridden            int32
	outBulkSets                  int32
	outBulkSetValues             int32
	outBulkSetPushes             int32
	outBulkSetPushValues         int32
	inBulkSets                   int32
	inBulkSetDrops               int32
	inBulkSetInvalids            int32
	inBulkSetWrites              int32
	inBulkSetWriteErrors         int32
	inBulkSetWritesOverridden    int32
	outBulkSetAcks               int32
	inBulkSetAcks                int32
	inBulkSetAckDrops            int32
	inBulkSetAckInvalids         int32
	inBulkSetAckWrites           int32
	inBulkSetAckWriteErrors      int32
	inBulkSetAckWritesOverridden int32
	outPullReplications          int32
	inPullReplications           int32
	inPullReplicationDrops       int32
	inPullReplicationInvalids    int32
	expiredDeletions             int32
	compactions                  int32
	smallFileCompactions         int32
}

type valueWriteReq struct {
	keyA uint64
	keyB uint64

	timestampbits uint64
	value         []byte
	errChan       chan error
	internal      bool
}

var enableValueWriteReq *valueWriteReq = &valueWriteReq{}
var disableValueWriteReq *valueWriteReq = &valueWriteReq{}
var flushValueWriteReq *valueWriteReq = &valueWriteReq{}
var flushValueMem *valueMem = &valueMem{}

type valueLocBlock interface {
	timestampnano() int64
	read(keyA uint64, keyB uint64, timestampbits uint64, offset uint32, length uint32, value []byte) (uint64, []byte, error)
	close() error
}

// NewValueStore creates a DefaultValueStore for use in storing []byte values referenced
// by 128 bit keys.
//
// Note that a lot of buffering, multiple cores, and background processes can
// be in use and therefore DisableAll() and Flush() should be called prior to
// the process exiting to ensure all processing is done and the buffers are
// flushed.
func NewValueStore(c *ValueStoreConfig) (*DefaultValueStore, error) {
	cfg := resolveValueStoreConfig(c)
	vlm := cfg.ValueLocMap
	if vlm == nil {
		vlm = valuelocmap.NewValueLocMap(nil)
	}
	vlm.SetInactiveMask(_TSB_INACTIVE)
	vs := &DefaultValueStore{
		logCritical:             cfg.LogCritical,
		logError:                cfg.LogError,
		logWarning:              cfg.LogWarning,
		logInfo:                 cfg.LogInfo,
		logDebug:                cfg.LogDebug,
		rand:                    cfg.Rand,
		locBlocks:               make([]valueLocBlock, math.MaxUint16),
		path:                    cfg.Path,
		pathtoc:                 cfg.PathTOC,
		vlm:                     vlm,
		workers:                 cfg.Workers,
		recoveryBatchSize:       cfg.RecoveryBatchSize,
		replicationIgnoreRecent: (uint64(cfg.ReplicationIgnoreRecent) * uint64(time.Second) / 1000) << _TSB_UTIL_BITS,
		valueCap:                uint32(cfg.ValueCap),
		pageSize:                uint32(cfg.PageSize),
		minValueAlloc:           cfg.minValueAlloc,
		writePagesPerWorker:     cfg.WritePagesPerWorker,
		fileCap:                 uint32(cfg.FileCap),
		fileReaders:             cfg.FileReaders,
		checksumInterval:        uint32(cfg.ChecksumInterval),
		msgRing:                 cfg.MsgRing,
	}
	vs.freeableVMChans = make([]chan *valueMem, vs.workers)
	for i := 0; i < cap(vs.freeableVMChans); i++ {
		vs.freeableVMChans[i] = make(chan *valueMem, vs.workers)
	}
	vs.freeVMChan = make(chan *valueMem, vs.workers*vs.writePagesPerWorker)
	vs.freeVWRChans = make([]chan *valueWriteReq, vs.workers)
	vs.pendingVWRChans = make([]chan *valueWriteReq, vs.workers)
	vs.vfVMChan = make(chan *valueMem, vs.workers)
	vs.freeTOCBlockChan = make(chan []byte, vs.workers*2)
	vs.pendingTOCBlockChan = make(chan []byte, vs.workers)
	vs.flushedChan = make(chan struct{}, 1)
	for i := 0; i < cap(vs.freeVMChan); i++ {
		vm := &valueMem{
			vs:     vs,
			toc:    make([]byte, 0, vs.pageSize),
			values: make([]byte, 0, vs.pageSize),
		}
		var err error
		vm.id, err = vs.addLocBlock(vm)
		if err != nil {
			return nil, err
		}
		vs.freeVMChan <- vm
	}
	for i := 0; i < len(vs.freeVWRChans); i++ {
		vs.freeVWRChans[i] = make(chan *valueWriteReq, vs.workers*2)
		for j := 0; j < vs.workers*2; j++ {
			vs.freeVWRChans[i] <- &valueWriteReq{errChan: make(chan error, 1)}
		}
	}
	for i := 0; i < len(vs.pendingVWRChans); i++ {
		vs.pendingVWRChans[i] = make(chan *valueWriteReq)
	}
	for i := 0; i < cap(vs.freeTOCBlockChan); i++ {
		vs.freeTOCBlockChan <- make([]byte, 0, vs.pageSize)
	}
	go vs.tocWriter()
	go vs.vfWriter()
	for i := 0; i < len(vs.freeableVMChans); i++ {
		go vs.memClearer(vs.freeableVMChans[i])
	}
	for i := 0; i < len(vs.pendingVWRChans); i++ {
		go vs.memWriter(vs.pendingVWRChans[i])
	}
	err := vs.recovery()
	if err != nil {
		return nil, err
	}
	vs.tombstoneDiscardConfig(cfg)
	vs.compactionConfig(cfg)
	vs.pullReplicationConfig(cfg)
	vs.pushReplicationConfig(cfg)
	vs.bulkSetConfig(cfg)
	vs.bulkSetAckConfig(cfg)
	vs.diskWatcherConfig(cfg)
	vs.tombstoneDiscardLaunch()
	vs.compactionLaunch()
	vs.pullReplicationLaunch()
	vs.pushReplicationLaunch()
	vs.bulkSetLaunch()
	vs.bulkSetAckLaunch()
	vs.diskWatcherLaunch()
	return vs, nil
}

// ValueCap returns the maximum length of a value the ValueStore can
// accept.
func (vs *DefaultValueStore) ValueCap() uint32 {
	return vs.valueCap
}

// DisableAll calls DisableAllBackground(), and DisableWrites().
func (vs *DefaultValueStore) DisableAll() {
	vs.DisableAllBackground()
	vs.DisableWrites()
}

// DisableAllBackground calls DisableTombstoneDiscard(), DisableCompaction(),
// DisableOutPullReplication(), DisableOutPushReplication(), but does *not*
// call DisableWrites().
func (vs *DefaultValueStore) DisableAllBackground() {
	vs.DisableTombstoneDiscard()
	vs.DisableCompaction()
	vs.DisableOutPullReplication()
	vs.DisableOutPushReplication()
}

// EnableAll calls EnableTombstoneDiscard(), EnableCompaction(),
// EnableOutPullReplication(), EnableOutPushReplication(), and EnableWrites().
func (vs *DefaultValueStore) EnableAll() {
	vs.EnableTombstoneDiscard()
	vs.EnableOutPullReplication()
	vs.EnableOutPushReplication()
	vs.EnableWrites()
	vs.EnableCompaction()
}

// DisableWrites will cause any incoming Write or Delete requests to respond
// with ErrDisabled until EnableWrites is called.
func (vs *DefaultValueStore) DisableWrites() {
	vs.disableWrites(true)
}

func (vs *DefaultValueStore) disableWrites(userCall bool) {
	vs.disableEnableWritesLock.Lock()
	if userCall {
		vs.userDisabled = true
	}
	for _, c := range vs.pendingVWRChans {
		c <- disableValueWriteReq
	}
	vs.disableEnableWritesLock.Unlock()
}

// EnableWrites will resume accepting incoming Write and Delete requests.
func (vs *DefaultValueStore) EnableWrites() {
	vs.enableWrites(true)
}

func (vs *DefaultValueStore) enableWrites(userCall bool) {
	vs.disableEnableWritesLock.Lock()
	if userCall || !vs.userDisabled {
		vs.userDisabled = false
		for _, c := range vs.pendingVWRChans {
			c <- enableValueWriteReq
		}
	}
	vs.disableEnableWritesLock.Unlock()
}

// Flush will ensure buffered data (at the time of the call) is written to
// disk.
func (vs *DefaultValueStore) Flush() {
	for _, c := range vs.pendingVWRChans {
		c <- flushValueWriteReq
	}
	<-vs.flushedChan
}

// Lookup will return timestampmicro, length, err for keyA, keyB.
//
// Note that err == ErrNotFound with timestampmicro == 0 indicates keyA, keyB
// was not known at all whereas err == ErrNotFound with timestampmicro != 0
// indicates keyA, keyB was known and had a deletion marker (aka tombstone).
func (vs *DefaultValueStore) Lookup(keyA uint64, keyB uint64) (int64, uint32, error) {
	atomic.AddInt32(&vs.lookups, 1)
	timestampbits, _, length, err := vs.lookup(keyA, keyB)
	if err != nil {
		atomic.AddInt32(&vs.lookupErrors, 1)
	}
	return int64(timestampbits >> _TSB_UTIL_BITS), length, err
}

func (vs *DefaultValueStore) lookup(keyA uint64, keyB uint64) (uint64, uint32, uint32, error) {
	timestampbits, id, _, length := vs.vlm.Get(keyA, keyB)
	if id == 0 || timestampbits&_TSB_DELETION != 0 {
		return timestampbits, id, 0, ErrNotFound
	}
	return timestampbits, id, length, nil
}

// Read will return timestampmicro, value, err for keyA, keyB; if an incoming
// value is provided, the read value will be appended to it and the whole
// returned (useful to reuse an existing []byte).
//
// Note that err == ErrNotFound with timestampmicro == 0 indicates keyA, keyB
// was not known at all whereas err == ErrNotFound with timestampmicro != 0
// indicates keyA, keyB was known and had a deletion marker (aka tombstone).
func (vs *DefaultValueStore) Read(keyA uint64, keyB uint64, value []byte) (int64, []byte, error) {
	atomic.AddInt32(&vs.reads, 1)
	timestampbits, value, err := vs.read(keyA, keyB, value)
	if err != nil {
		atomic.AddInt32(&vs.readErrors, 1)
	}
	return int64(timestampbits >> _TSB_UTIL_BITS), value, err
}

func (vs *DefaultValueStore) read(keyA uint64, keyB uint64, value []byte) (uint64, []byte, error) {
	timestampbits, id, offset, length := vs.vlm.Get(keyA, keyB)
	if id == 0 || timestampbits&_TSB_DELETION != 0 || timestampbits&_TSB_LOCAL_REMOVAL != 0 {
		return timestampbits, value, ErrNotFound
	}
	return vs.locBlock(id).read(keyA, keyB, timestampbits, offset, length, value)
}

// Write stores timestampmicro, value for keyA, keyB and returns the previously
// stored timestampmicro or returns any error; a newer timestampmicro already
// in place is not reported as an error. Note that with a write and a delete
// for the exact same timestampmicro, the delete wins.
func (vs *DefaultValueStore) Write(keyA uint64, keyB uint64, timestampmicro int64, value []byte) (int64, error) {
	atomic.AddInt32(&vs.writes, 1)
	if timestampmicro < TIMESTAMPMICRO_MIN {
		atomic.AddInt32(&vs.writeErrors, 1)
		return 0, fmt.Errorf("timestamp %d < %d", timestampmicro, TIMESTAMPMICRO_MIN)
	}
	if timestampmicro > TIMESTAMPMICRO_MAX {
		atomic.AddInt32(&vs.writeErrors, 1)
		return 0, fmt.Errorf("timestamp %d > %d", timestampmicro, TIMESTAMPMICRO_MAX)
	}
	timestampbits, err := vs.write(keyA, keyB, uint64(timestampmicro)<<_TSB_UTIL_BITS, value, false)
	if err != nil {
		atomic.AddInt32(&vs.writeErrors, 1)
	}
	if timestampmicro <= int64(timestampbits>>_TSB_UTIL_BITS) {
		atomic.AddInt32(&vs.writesOverridden, 1)
	}
	return int64(timestampbits >> _TSB_UTIL_BITS), err
}

func (vs *DefaultValueStore) write(keyA uint64, keyB uint64, timestampbits uint64, value []byte, internal bool) (uint64, error) {
	i := int(keyA>>1) % len(vs.freeVWRChans)
	vwr := <-vs.freeVWRChans[i]
	vwr.keyA = keyA
	vwr.keyB = keyB

	vwr.timestampbits = timestampbits
	vwr.value = value
	vwr.internal = internal
	vs.pendingVWRChans[i] <- vwr
	err := <-vwr.errChan
	ptimestampbits := vwr.timestampbits
	vwr.value = nil
	vs.freeVWRChans[i] <- vwr
	return ptimestampbits, err
}

// Delete stores timestampmicro for keyA, keyB and returns the previously
// stored timestampmicro or returns any error; a newer timestampmicro already
// in place is not reported as an error. Note that with a write and a delete
// for the exact same timestampmicro, the delete wins.
func (vs *DefaultValueStore) Delete(keyA uint64, keyB uint64, timestampmicro int64) (int64, error) {
	atomic.AddInt32(&vs.deletes, 1)
	if timestampmicro < TIMESTAMPMICRO_MIN {
		atomic.AddInt32(&vs.deleteErrors, 1)
		return 0, fmt.Errorf("timestamp %d < %d", timestampmicro, TIMESTAMPMICRO_MIN)
	}
	if timestampmicro > TIMESTAMPMICRO_MAX {
		atomic.AddInt32(&vs.deleteErrors, 1)
		return 0, fmt.Errorf("timestamp %d > %d", timestampmicro, TIMESTAMPMICRO_MAX)
	}
	// TODO: Fix group part
	ptimestampbits, err := vs.write(keyA, keyB, (uint64(timestampmicro)<<_TSB_UTIL_BITS)|_TSB_DELETION, nil, true)
	if err != nil {
		atomic.AddInt32(&vs.deleteErrors, 1)
	}
	if timestampmicro <= int64(ptimestampbits>>_TSB_UTIL_BITS) {
		atomic.AddInt32(&vs.deletesOverridden, 1)
	}
	return int64(ptimestampbits >> _TSB_UTIL_BITS), err
}

func (vs *DefaultValueStore) locBlock(locBlockID uint32) valueLocBlock {
	return vs.locBlocks[locBlockID]
}

func (vs *DefaultValueStore) addLocBlock(block valueLocBlock) (uint32, error) {
	id := atomic.AddUint64(&vs.locBlockIDer, 1)
	if id >= math.MaxUint32 {
		return 0, errors.New("too many loc blocks")
	}
	vs.locBlocks[id] = block
	return uint32(id), nil
}

func (vs *DefaultValueStore) locBlockIDFromTimestampnano(tsn int64) uint32 {
	for i := 1; i <= len(vs.locBlocks); i++ {
		if vs.locBlocks[i] == nil {
			return 0
		} else {
			if tsn == vs.locBlocks[i].timestampnano() {
				return uint32(i)
			}
		}
	}
	return 0
}

func (vs *DefaultValueStore) closeLocBlock(locBlockID uint32) error {
	return vs.locBlocks[locBlockID].close()
}

func (vs *DefaultValueStore) memClearer(freeableVMChan chan *valueMem) {
	var tb []byte
	var tbTS int64
	var tbOffset int
	for {
		vm := <-freeableVMChan
		if vm == flushValueMem {
			if tb != nil {
				vs.pendingTOCBlockChan <- tb
				tb = nil
			}
			vs.pendingTOCBlockChan <- nil
			continue
		}
		vf := vs.locBlock(vm.vfID)
		if tb != nil && tbTS != vf.timestampnano() {
			vs.pendingTOCBlockChan <- tb
			tb = nil
		}
		for vmTOCOffset := 0; vmTOCOffset < len(vm.toc); vmTOCOffset += 32 {
			keyA := binary.BigEndian.Uint64(vm.toc[vmTOCOffset:])
			keyB := binary.BigEndian.Uint64(vm.toc[vmTOCOffset+8:])
			timestampbits := binary.BigEndian.Uint64(vm.toc[vmTOCOffset+16:])
			var blockID uint32
			var offset uint32
			var length uint32
			if timestampbits&_TSB_LOCAL_REMOVAL == 0 {
				blockID = vm.vfID
				offset = vm.vfOffset + binary.BigEndian.Uint32(vm.toc[vmTOCOffset+24:])
				length = binary.BigEndian.Uint32(vm.toc[vmTOCOffset+28:])
			}
			// TODO: nameKey needs to go all throughout the code.
			if vs.vlm.Set(keyA, keyB, timestampbits, blockID, offset, length, true) > timestampbits {
				continue
			}
			if tb != nil && tbOffset+32 > cap(tb) {
				vs.pendingTOCBlockChan <- tb
				tb = nil
			}
			if tb == nil {
				tb = <-vs.freeTOCBlockChan
				tbTS = vf.timestampnano()
				tb = tb[:8]
				binary.BigEndian.PutUint64(tb, uint64(tbTS))
				tbOffset = 8
			}
			tb = tb[:tbOffset+32]
			binary.BigEndian.PutUint64(tb[tbOffset:], keyA)
			binary.BigEndian.PutUint64(tb[tbOffset+8:], keyB)
			binary.BigEndian.PutUint64(tb[tbOffset+16:], timestampbits)
			binary.BigEndian.PutUint32(tb[tbOffset+24:], offset)
			binary.BigEndian.PutUint32(tb[tbOffset+28:], length)
			tbOffset += 32
		}
		vm.discardLock.Lock()
		vm.vfID = 0
		vm.vfOffset = 0
		vm.toc = vm.toc[:0]
		vm.values = vm.values[:0]
		vm.discardLock.Unlock()
		vs.freeVMChan <- vm
	}
}

func (vs *DefaultValueStore) memWriter(pendingVWRChan chan *valueWriteReq) {
	var enabled bool
	var vm *valueMem
	var vmTOCOffset int
	var vmMemOffset int
	for {
		vwr := <-pendingVWRChan
		if vwr == enableValueWriteReq {
			enabled = true
			continue
		}
		if vwr == disableValueWriteReq {
			enabled = false
			continue
		}
		if vwr == flushValueWriteReq {
			if vm != nil && len(vm.toc) > 0 {
				vs.vfVMChan <- vm
				vm = nil
			}
			vs.vfVMChan <- flushValueMem
			continue
		}
		if !enabled && !vwr.internal {
			vwr.errChan <- ErrDisabled
			continue
		}
		length := len(vwr.value)
		if length > int(vs.valueCap) {
			vwr.errChan <- fmt.Errorf("value length of %d > %d", length, vs.valueCap)
			continue
		}
		alloc := length
		if alloc < vs.minValueAlloc {
			alloc = vs.minValueAlloc
		}
		if vm != nil && (vmTOCOffset+32 > cap(vm.toc) || vmMemOffset+alloc > cap(vm.values)) {
			vs.vfVMChan <- vm
			vm = nil
		}
		if vm == nil {
			vm = <-vs.freeVMChan
			vmTOCOffset = 0
			vmMemOffset = 0
		}
		vm.discardLock.Lock()
		vm.values = vm.values[:vmMemOffset+alloc]
		vm.discardLock.Unlock()
		copy(vm.values[vmMemOffset:], vwr.value)
		if alloc > length {
			for i, j := vmMemOffset+length, vmMemOffset+alloc; i < j; i++ {
				vm.values[i] = 0
			}
		}
		// TODO: nameKey needs to go all throughout the code.
		ptimestampbits := vs.vlm.Set(vwr.keyA, vwr.keyB, vwr.timestampbits, vm.id, uint32(vmMemOffset), uint32(length), false)
		if ptimestampbits < vwr.timestampbits {
			vm.toc = vm.toc[:vmTOCOffset+32]
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset:], vwr.keyA)
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+8:], vwr.keyB)
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+16:], vwr.timestampbits)
			binary.BigEndian.PutUint32(vm.toc[vmTOCOffset+24:], uint32(vmMemOffset))
			binary.BigEndian.PutUint32(vm.toc[vmTOCOffset+28:], uint32(length))
			vmTOCOffset += 32
			vmMemOffset += alloc
		} else {
			vm.discardLock.Lock()
			vm.values = vm.values[:vmMemOffset]
			vm.discardLock.Unlock()
		}
		vwr.timestampbits = ptimestampbits
		vwr.errChan <- nil
	}
}

func (vs *DefaultValueStore) vfWriter() {
	var vf *valueFile
	memWritersFlushLeft := len(vs.pendingVWRChans)
	var tocLen uint64
	var valueLen uint64
	for {
		vm := <-vs.vfVMChan
		if vm == flushValueMem {
			memWritersFlushLeft--
			if memWritersFlushLeft > 0 {
				continue
			}
			if vf != nil {
				err := vf.close()
				if err != nil {
					vs.logCritical("error closing %s: %s\n", vf.name, err)
				}
				vf = nil
			}
			for i := 0; i < len(vs.freeableVMChans); i++ {
				vs.freeableVMChans[i] <- flushValueMem
			}
			memWritersFlushLeft = len(vs.pendingVWRChans)
			continue
		}
		if vf != nil && (tocLen+uint64(len(vm.toc)) >= uint64(vs.fileCap) || valueLen+uint64(len(vm.values)) > uint64(vs.fileCap)) {
			err := vf.close()
			if err != nil {
				vs.logCritical("error closing %s: %s\n", vf.name, err)
			}
			vf = nil
		}
		if vf == nil {
			var err error
			vf, err = createValueFile(vs, osCreateWriteCloser, osOpenReadSeeker)
			if err != nil {
				vs.logCritical("vfWriter: %s\n", err)
				break
			}
			tocLen = 32
			valueLen = 32
		}
		vf.write(vm)
		tocLen += uint64(len(vm.toc))
		valueLen += uint64(len(vm.values))
	}
}

func (vs *DefaultValueStore) tocWriter() {
	// writerA is the current toc file while writerB is the previously active toc
	// writerB is kept around in case a "late" key arrives to be flushed whom's value
	// is actually in the previous values file.
	memClearersFlushLeft := len(vs.freeableVMChans)
	var writerA io.WriteCloser
	var offsetA uint64
	var writerB io.WriteCloser
	var offsetB uint64
	var err error
	head := []byte("VALUESTORETOC v0                ")
	binary.BigEndian.PutUint32(head[28:], uint32(vs.checksumInterval))
	term := make([]byte, 16)
	copy(term[12:], "TERM")
OuterLoop:
	for {
		t := <-vs.pendingTOCBlockChan
		if t == nil {
			memClearersFlushLeft--
			if memClearersFlushLeft > 0 {
				continue
			}
			if writerB != nil {
				binary.BigEndian.PutUint64(term[4:], offsetB)
				if _, err = writerB.Write(term); err != nil {
					break OuterLoop
				}
				if err = writerB.Close(); err != nil {
					break OuterLoop
				}
				writerB = nil
				atomic.StoreUint64(&vs.activeTOCB, 0)
				offsetB = 0
			}
			if writerA != nil {
				binary.BigEndian.PutUint64(term[4:], offsetA)
				if _, err = writerA.Write(term); err != nil {
					break OuterLoop
				}
				if err = writerA.Close(); err != nil {
					break OuterLoop
				}
				writerA = nil
				atomic.StoreUint64(&vs.activeTOCA, 0)
				offsetA = 0
			}
			vs.flushedChan <- struct{}{}
			memClearersFlushLeft = len(vs.freeableVMChans)
			continue
		}
		if len(t) > 8 {
			bts := binary.BigEndian.Uint64(t)
			switch bts {
			case atomic.LoadUint64(&vs.activeTOCA):
				if _, err = writerA.Write(t[8:]); err != nil {
					break OuterLoop
				}
				offsetA += uint64(len(t) - 8)
			case atomic.LoadUint64(&vs.activeTOCB):
				if _, err = writerB.Write(t[8:]); err != nil {
					break OuterLoop
				}
				offsetB += uint64(len(t) - 8)
			default:
				// An assumption is made here: If the timestampnano for this
				// toc block doesn't match the last two seen timestampnanos
				// then we expect no more toc blocks for the oldest
				// timestampnano and can close that toc file.
				if writerB != nil {
					binary.BigEndian.PutUint64(term[4:], offsetB)
					if _, err = writerB.Write(term); err != nil {
						break OuterLoop
					}
					if err = writerB.Close(); err != nil {
						break OuterLoop
					}
				}
				atomic.StoreUint64(&vs.activeTOCB, atomic.LoadUint64(&vs.activeTOCA))
				writerB = writerA
				offsetB = offsetA
				atomic.StoreUint64(&vs.activeTOCA, bts)
				var fp *os.File
				fp, err = os.Create(path.Join(vs.pathtoc, fmt.Sprintf("%d.valuestoc", bts)))
				if err != nil {
					break OuterLoop
				}
				writerA = brimutil.NewMultiCoreChecksummedWriter(fp, int(vs.checksumInterval), murmur3.New32, vs.workers)
				if _, err = writerA.Write(head); err != nil {
					break OuterLoop
				}
				if _, err = writerA.Write(t[8:]); err != nil {
					break OuterLoop
				}
				offsetA = 32 + uint64(len(t)-8)
			}
		}
		vs.freeTOCBlockChan <- t[:0]
	}
	if err != nil {
		vs.logCritical("tocWriter: %s\n", err)
	}
	if writerA != nil {
		writerA.Close()
	}
	if writerB != nil {
		writerB.Close()
	}
}

func (vs *DefaultValueStore) recovery() error {
	start := time.Now()
	fromDiskCount := 0
	causedChangeCount := int64(0)
	type writeReq struct {
		keyA          uint64
		keyB          uint64
		timestampbits uint64
		blockID       uint32
		offset        uint32
		length        uint32
	}
	workers := uint64(vs.workers)
	pendingBatchChans := make([]chan []writeReq, workers)
	freeBatchChans := make([]chan []writeReq, len(pendingBatchChans))
	for i := 0; i < len(pendingBatchChans); i++ {
		pendingBatchChans[i] = make(chan []writeReq, 4)
		freeBatchChans[i] = make(chan []writeReq, 4)
		for j := 0; j < cap(freeBatchChans[i]); j++ {
			freeBatchChans[i] <- make([]writeReq, vs.recoveryBatchSize)
		}
	}
	wg := &sync.WaitGroup{}
	wg.Add(len(pendingBatchChans))
	for i := 0; i < len(pendingBatchChans); i++ {
		go func(pendingBatchChan chan []writeReq, freeBatchChan chan []writeReq) {
			for {
				batch := <-pendingBatchChan
				if batch == nil {
					break
				}
				for j := 0; j < len(batch); j++ {
					wr := &batch[j]
					if wr.timestampbits&_TSB_LOCAL_REMOVAL != 0 {
						wr.blockID = 0
					}
					if vs.logDebug != nil {
						// TODO: nameKey needs to go all throughout the code.
						if vs.vlm.Set(wr.keyA, wr.keyB, wr.timestampbits, wr.blockID, wr.offset, wr.length, true) < wr.timestampbits {
							atomic.AddInt64(&causedChangeCount, 1)
						}
					} else {
						// TODO: nameKey needs to go all throughout the code.
						vs.vlm.Set(wr.keyA, wr.keyB, wr.timestampbits, wr.blockID, wr.offset, wr.length, true)
					}
				}
				freeBatchChan <- batch
			}
			wg.Done()
		}(pendingBatchChans[i], freeBatchChans[i])
	}
	fromDiskBuf := make([]byte, vs.checksumInterval+4)
	fromDiskOverflow := make([]byte, 0, 32)
	batches := make([][]writeReq, len(freeBatchChans))
	batchesPos := make([]int, len(batches))
	fp, err := os.Open(vs.pathtoc)
	if err != nil {
		return err
	}
	names, err := fp.Readdirnames(-1)
	fp.Close()
	if err != nil {
		return err
	}
	sort.Strings(names)
	for i := 0; i < len(names); i++ {
		if !strings.HasSuffix(names[i], ".valuestoc") {
			continue
		}
		namets := int64(0)
		if namets, err = strconv.ParseInt(names[i][:len(names[i])-len(".valuestoc")], 10, 64); err != nil {
			vs.logError("bad timestamp in name: %#v\n", names[i])
			continue
		}
		if namets == 0 {
			vs.logError("bad timestamp in name: %#v\n", names[i])
			continue
		}
		vf, err := newValueFile(vs, namets, osOpenReadSeeker)
		if err != nil {
			vs.logError("error opening %s: %s\n", names[i], err)
			continue
		}
		fp, err := os.Open(path.Join(vs.pathtoc, names[i]))
		if err != nil {
			vs.logError("error opening %s: %s\n", names[i], err)
			continue
		}
		checksumFailures := 0
		first := true
		terminated := false
		fromDiskOverflow = fromDiskOverflow[:0]
		for {
			n, err := io.ReadFull(fp, fromDiskBuf)
			if n < 4 {
				if err != io.EOF && err != io.ErrUnexpectedEOF {
					vs.logError("error reading %s: %s\n", names[i], err)
				}
				break
			}
			n -= 4
			if murmur3.Sum32(fromDiskBuf[:n]) != binary.BigEndian.Uint32(fromDiskBuf[n:]) {
				checksumFailures++
			} else {
				j := 0
				if first {
					if !bytes.Equal(fromDiskBuf[:28], []byte("VALUESTORETOC v0            ")) {
						vs.logError("bad header: %s\n", names[i])
						break
					}
					if binary.BigEndian.Uint32(fromDiskBuf[28:]) != vs.checksumInterval {
						vs.logError("bad header checksum interval: %s\n", names[i])
						break
					}
					j += 32
					first = false
				}
				if n < int(vs.checksumInterval) {
					if binary.BigEndian.Uint32(fromDiskBuf[n-16:]) != 0 {
						vs.logError("bad terminator size marker: %s\n", names[i])
						break
					}
					if !bytes.Equal(fromDiskBuf[n-4:n], []byte("TERM")) {
						vs.logError("bad terminator: %s\n", names[i])
						break
					}
					n -= 16
					terminated = true
				}
				if len(fromDiskOverflow) > 0 {
					j += 32 - len(fromDiskOverflow)
					fromDiskOverflow = append(fromDiskOverflow, fromDiskBuf[j-32+len(fromDiskOverflow):j]...)
					keyB := binary.BigEndian.Uint64(fromDiskOverflow[8:])
					k := keyB % workers
					if batches[k] == nil {
						batches[k] = <-freeBatchChans[k]
						batchesPos[k] = 0
					}
					wr := &batches[k][batchesPos[k]]
					wr.keyA = binary.BigEndian.Uint64(fromDiskOverflow)
					wr.keyB = keyB
					wr.timestampbits = binary.BigEndian.Uint64(fromDiskOverflow[16:])
					wr.blockID = vf.id
					wr.offset = binary.BigEndian.Uint32(fromDiskOverflow[24:])
					wr.length = binary.BigEndian.Uint32(fromDiskOverflow[28:])
					batchesPos[k]++
					if batchesPos[k] >= vs.recoveryBatchSize {
						pendingBatchChans[k] <- batches[k]
						batches[k] = nil
					}
					fromDiskCount++
					fromDiskOverflow = fromDiskOverflow[:0]
				}
				for ; j+32 <= n; j += 32 {
					keyB := binary.BigEndian.Uint64(fromDiskBuf[j+8:])
					k := keyB % workers
					if batches[k] == nil {
						batches[k] = <-freeBatchChans[k]
						batchesPos[k] = 0
					}
					wr := &batches[k][batchesPos[k]]
					wr.keyA = binary.BigEndian.Uint64(fromDiskBuf[j:])
					wr.keyB = keyB
					wr.timestampbits = binary.BigEndian.Uint64(fromDiskBuf[j+16:])
					wr.blockID = vf.id
					wr.offset = binary.BigEndian.Uint32(fromDiskBuf[j+24:])
					wr.length = binary.BigEndian.Uint32(fromDiskBuf[j+28:])
					batchesPos[k]++
					if batchesPos[k] >= vs.recoveryBatchSize {
						pendingBatchChans[k] <- batches[k]
						batches[k] = nil
					}
					fromDiskCount++
				}
				if j != n {
					fromDiskOverflow = fromDiskOverflow[:n-j]
					copy(fromDiskOverflow, fromDiskBuf[j:])
				}
			}
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				vs.logError("error reading %s: %s\n", names[i], err)
				break
			}
		}
		fp.Close()
		if !terminated {
			vs.logError("early end of file: %s\n", names[i])
		}
		if checksumFailures > 0 {
			vs.logWarning("%d checksum failures for %s\n", checksumFailures, names[i])
		}
	}
	for i := 0; i < len(batches); i++ {
		if batches[i] != nil {
			pendingBatchChans[i] <- batches[i][:batchesPos[i]]
		}
		pendingBatchChans[i] <- nil
	}
	wg.Wait()
	if vs.logDebug != nil {
		dur := time.Now().Sub(start)
		stats := vs.Stats(false).(*ValueStoreStats)
		vs.logInfo("%d key locations loaded in %s, %.0f/s; %d caused change; %d resulting locations referencing %d bytes.\n", fromDiskCount, dur, float64(fromDiskCount)/(float64(dur)/float64(time.Second)), causedChangeCount, stats.Values, stats.ValueBytes)
	}
	return nil
}
