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

// GroupStore is an interface for a disk-backed data structure that stores
// []byte values referenced by 128 bit keys with options for replication.
//
// For documentation on each of these functions, see the DefaultGroupStore.
type GroupStore interface {
	Lookup(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64) (int64, uint32, error)

	LookupGroup(keyA uint64, keyB uint64) []LookupGroupItem

	Read(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, value []byte) (int64, []byte, error)

	ReadGroup(keyA uint64, keyB uint64) (int, chan *ReadGroupItem)

	Write(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, timestamp int64, value []byte) (int64, error)
	Delete(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, timestamp int64) (int64, error)
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

// DefaultGroupStore instances are created with NewGroupStore.
type DefaultGroupStore struct {
	logCritical             LogFunc
	logError                LogFunc
	logWarning              LogFunc
	logInfo                 LogFunc
	logDebug                LogFunc
	randMutex               sync.Mutex
	rand                    *rand.Rand
	freeableVMChans         []chan *groupMem
	freeVMChan              chan *groupMem
	freeVWRChans            []chan *groupWriteReq
	pendingVWRChans         []chan *groupWriteReq
	vfVMChan                chan *groupMem
	freeTOCBlockChan        chan []byte
	pendingTOCBlockChan     chan []byte
	activeTOCA              uint64
	activeTOCB              uint64
	flushedChan             chan struct{}
	locBlocks               []groupLocBlock
	locBlockIDer            uint64
	path                    string
	pathtoc                 string
	vlm                     valuelocmap.GroupLocMap
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
	tombstoneDiscardState   groupTombstoneDiscardState
	replicationIgnoreRecent uint64
	pullReplicationState    groupPullReplicationState
	pushReplicationState    groupPushReplicationState
	compactionState         groupCompactionState
	bulkSetState            groupBulkSetState
	bulkSetAckState         groupBulkSetAckState
	disableEnableWritesLock sync.Mutex
	userDisabled            bool
	diskWatcherState        groupDiskWatcherState

	statsLock                    sync.Mutex
	lookups                      int32
	lookupErrors                 int32
	lookupGroups                 int32
	lookupGroupItems             int32
	reads                        int32
	readErrors                   int32
	readGroups                   int32
	readGroupItems               int32
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

type groupWriteReq struct {
	keyA uint64
	keyB uint64

	nameKeyA uint64
	nameKeyB uint64

	timestampbits uint64
	value         []byte
	errChan       chan error
	internal      bool
}

var enableGroupWriteReq *groupWriteReq = &groupWriteReq{}
var disableGroupWriteReq *groupWriteReq = &groupWriteReq{}
var flushGroupWriteReq *groupWriteReq = &groupWriteReq{}
var flushGroupMem *groupMem = &groupMem{}

type groupLocBlock interface {
	timestampnano() int64
	read(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, timestampbits uint64, offset uint32, length uint32, value []byte) (uint64, []byte, error)
	close() error
}

// NewGroupStore creates a DefaultGroupStore for use in storing []byte values
// referenced by 128 bit keys.
//
// Note that a lot of buffering, multiple cores, and background processes can
// be in use and therefore DisableAll() and Flush() should be called prior to
// the process exiting to ensure all processing is done and the buffers are
// flushed.
func NewGroupStore(c *GroupStoreConfig) (*DefaultGroupStore, error) {
	cfg := resolveGroupStoreConfig(c)
	vlm := cfg.GroupLocMap
	if vlm == nil {
		vlm = valuelocmap.NewGroupLocMap(nil)
	}
	vlm.SetInactiveMask(_TSB_INACTIVE)
	vs := &DefaultGroupStore{
		logCritical:             cfg.LogCritical,
		logError:                cfg.LogError,
		logWarning:              cfg.LogWarning,
		logInfo:                 cfg.LogInfo,
		logDebug:                cfg.LogDebug,
		rand:                    cfg.Rand,
		locBlocks:               make([]groupLocBlock, math.MaxUint16),
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
	vs.freeableVMChans = make([]chan *groupMem, vs.workers)
	for i := 0; i < cap(vs.freeableVMChans); i++ {
		vs.freeableVMChans[i] = make(chan *groupMem, vs.workers)
	}
	vs.freeVMChan = make(chan *groupMem, vs.workers*vs.writePagesPerWorker)
	vs.freeVWRChans = make([]chan *groupWriteReq, vs.workers)
	vs.pendingVWRChans = make([]chan *groupWriteReq, vs.workers)
	vs.vfVMChan = make(chan *groupMem, vs.workers)
	vs.freeTOCBlockChan = make(chan []byte, vs.workers*2)
	vs.pendingTOCBlockChan = make(chan []byte, vs.workers)
	vs.flushedChan = make(chan struct{}, 1)
	for i := 0; i < cap(vs.freeVMChan); i++ {
		vm := &groupMem{
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
		vs.freeVWRChans[i] = make(chan *groupWriteReq, vs.workers*2)
		for j := 0; j < vs.workers*2; j++ {
			vs.freeVWRChans[i] <- &groupWriteReq{errChan: make(chan error, 1)}
		}
	}
	for i := 0; i < len(vs.pendingVWRChans); i++ {
		vs.pendingVWRChans[i] = make(chan *groupWriteReq)
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

// ValueCap returns the maximum length of a value the GroupStore can accept.
func (vs *DefaultGroupStore) ValueCap() uint32 {
	return vs.valueCap
}

// DisableAll calls DisableAllBackground(), and DisableWrites().
func (vs *DefaultGroupStore) DisableAll() {
	vs.DisableAllBackground()
	vs.DisableWrites()
}

// DisableAllBackground calls DisableTombstoneDiscard(), DisableCompaction(),
// DisableOutPullReplication(), DisableOutPushReplication(), but does *not*
// call DisableWrites().
func (vs *DefaultGroupStore) DisableAllBackground() {
	vs.DisableTombstoneDiscard()
	vs.DisableCompaction()
	vs.DisableOutPullReplication()
	vs.DisableOutPushReplication()
}

// EnableAll calls EnableTombstoneDiscard(), EnableCompaction(),
// EnableOutPullReplication(), EnableOutPushReplication(), and EnableWrites().
func (vs *DefaultGroupStore) EnableAll() {
	vs.EnableTombstoneDiscard()
	vs.EnableOutPullReplication()
	vs.EnableOutPushReplication()
	vs.EnableWrites()
	vs.EnableCompaction()
}

// DisableWrites will cause any incoming Write or Delete requests to respond
// with ErrDisabled until EnableWrites is called.
func (vs *DefaultGroupStore) DisableWrites() {
	vs.disableWrites(true)
}

func (vs *DefaultGroupStore) disableWrites(userCall bool) {
	vs.disableEnableWritesLock.Lock()
	if userCall {
		vs.userDisabled = true
	}
	for _, c := range vs.pendingVWRChans {
		c <- disableGroupWriteReq
	}
	vs.disableEnableWritesLock.Unlock()
}

// EnableWrites will resume accepting incoming Write and Delete requests.
func (vs *DefaultGroupStore) EnableWrites() {
	vs.enableWrites(true)
}

func (vs *DefaultGroupStore) enableWrites(userCall bool) {
	vs.disableEnableWritesLock.Lock()
	if userCall || !vs.userDisabled {
		vs.userDisabled = false
		for _, c := range vs.pendingVWRChans {
			c <- enableGroupWriteReq
		}
	}
	vs.disableEnableWritesLock.Unlock()
}

// Flush will ensure buffered data (at the time of the call) is written to
// disk.
func (vs *DefaultGroupStore) Flush() {
	for _, c := range vs.pendingVWRChans {
		c <- flushGroupWriteReq
	}
	<-vs.flushedChan
}

// Lookup will return timestampmicro, length, err for keyA, keyB, nameKeyA, nameKeyB.
//
// Note that err == ErrNotFound with timestampmicro == 0 indicates keyA, keyB, nameKeyA, nameKeyB
// was not known at all whereas err == ErrNotFound with timestampmicro != 0
// indicates keyA, keyB, nameKeyA, nameKeyB
// was known and had a deletion marker (aka tombstone).
func (vs *DefaultGroupStore) Lookup(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64) (int64, uint32, error) {
	atomic.AddInt32(&vs.lookups, 1)
	timestampbits, _, length, err := vs.lookup(keyA, keyB, nameKeyA, nameKeyB)
	if err != nil {
		atomic.AddInt32(&vs.lookupErrors, 1)
	}
	return int64(timestampbits >> _TSB_UTIL_BITS), length, err
}

func (vs *DefaultGroupStore) lookup(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64) (uint64, uint32, uint32, error) {
	timestampbits, id, _, length := vs.vlm.Get(keyA, keyB, nameKeyA, nameKeyB)
	if id == 0 || timestampbits&_TSB_DELETION != 0 {
		return timestampbits, id, 0, ErrNotFound
	}
	return timestampbits, id, length, nil
}

type LookupGroupItem struct {
	NameKeyA       uint64
	NameKeyB       uint64
	TimestampMicro uint64
}

// LookupGroup returns all the nameKeyA, nameKeyB, TimestampMicro items
// matching under keyA, keyB.
func (vs *DefaultGroupStore) LookupGroup(keyA uint64, keyB uint64) []LookupGroupItem {
	atomic.AddInt32(&vs.lookupGroups, 1)
	items := vs.vlm.GetGroup(keyA, keyB)
	if len(items) == 0 {
		return nil
	}
	atomic.AddInt32(&vs.lookupGroupItems, int32(len(items)))
	rv := make([]LookupGroupItem, len(items))
	for i, item := range items {
		rv[i].NameKeyA = item.NameKeyA
		rv[i].NameKeyB = item.NameKeyB
		rv[i].TimestampMicro = item.Timestamp >> _TSB_UTIL_BITS
	}
	return rv
}

// Read will return timestampmicro, value, err for keyA, keyB, nameKeyA, nameKeyB;
// if an incoming value is provided, the read value will be appended to it and
// the whole returned (useful to reuse an existing []byte).
//
// Note that err == ErrNotFound with timestampmicro == 0 indicates keyA, keyB, nameKeyA, nameKeyB
// was not known at all whereas err == ErrNotFound with timestampmicro != 0
// indicates keyA, keyB, nameKeyA, nameKeyB was known and had a deletion marker (aka tombstone).
func (vs *DefaultGroupStore) Read(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, value []byte) (int64, []byte, error) {
	atomic.AddInt32(&vs.reads, 1)
	timestampbits, value, err := vs.read(keyA, keyB, nameKeyA, nameKeyB, value)
	if err != nil {
		atomic.AddInt32(&vs.readErrors, 1)
	}
	return int64(timestampbits >> _TSB_UTIL_BITS), value, err
}

func (vs *DefaultGroupStore) read(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, value []byte) (uint64, []byte, error) {
	timestampbits, id, offset, length := vs.vlm.Get(keyA, keyB, nameKeyA, nameKeyB)
	if id == 0 || timestampbits&_TSB_DELETION != 0 || timestampbits&_TSB_LOCAL_REMOVAL != 0 {
		return timestampbits, value, ErrNotFound
	}
	return vs.locBlock(id).read(keyA, keyB, nameKeyA, nameKeyB, timestampbits, offset, length, value)
}

type ReadGroupItem struct {
	Error          error
	NameKeyA       uint64
	NameKeyB       uint64
	TimestampMicro uint64
	Value          []byte
}

// ReadGroup returns all the items with keyA, keyB; the returned int indicates
// a an estimate of the item count and the items are return through the
// channel. Note that the int is just an estimate; a different number of items
// may be returned.
func (vs *DefaultGroupStore) ReadGroup(keyA uint64, keyB uint64) (int, chan *ReadGroupItem) {
	atomic.AddInt32(&vs.readGroups, 1)
	items := vs.vlm.GetGroup(keyA, keyB)
	c := make(chan *ReadGroupItem, vs.workers)
	if len(items) == 0 {
		close(c)
		return 0, c
	}
	atomic.AddInt32(&vs.readGroupItems, int32(len(items)))
	go func() {
		for _, item := range items {
			t, v, err := vs.read(keyA, keyB, item.NameKeyA, item.NameKeyB, nil)
			if err != nil {
				if err != ErrNotFound {
					c <- &ReadGroupItem{Error: err}
					break
				}
			}
			c <- &ReadGroupItem{
				NameKeyA:       item.NameKeyA,
				NameKeyB:       item.NameKeyB,
				TimestampMicro: t,
				Value:          v,
			}
		}
		close(c)
	}()
	return len(items), c
}

// Write stores timestampmicro, value for keyA, keyB, nameKeyA, nameKeyB
// and returns the previously stored timestampmicro or returns any error; a
// newer timestampmicro already in place is not reported as an error. Note that
// with a write and a delete for the exact same timestampmicro, the delete
// wins.
func (vs *DefaultGroupStore) Write(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, timestampmicro int64, value []byte) (int64, error) {
	atomic.AddInt32(&vs.writes, 1)
	if timestampmicro < TIMESTAMPMICRO_MIN {
		atomic.AddInt32(&vs.writeErrors, 1)
		return 0, fmt.Errorf("timestamp %d < %d", timestampmicro, TIMESTAMPMICRO_MIN)
	}
	if timestampmicro > TIMESTAMPMICRO_MAX {
		atomic.AddInt32(&vs.writeErrors, 1)
		return 0, fmt.Errorf("timestamp %d > %d", timestampmicro, TIMESTAMPMICRO_MAX)
	}
	timestampbits, err := vs.write(keyA, keyB, nameKeyA, nameKeyB, uint64(timestampmicro)<<_TSB_UTIL_BITS, value, false)
	if err != nil {
		atomic.AddInt32(&vs.writeErrors, 1)
	}
	if timestampmicro <= int64(timestampbits>>_TSB_UTIL_BITS) {
		atomic.AddInt32(&vs.writesOverridden, 1)
	}
	return int64(timestampbits >> _TSB_UTIL_BITS), err
}

func (vs *DefaultGroupStore) write(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, timestampbits uint64, value []byte, internal bool) (uint64, error) {
	i := int(keyA>>1) % len(vs.freeVWRChans)
	vwr := <-vs.freeVWRChans[i]
	vwr.keyA = keyA
	vwr.keyB = keyB

	vwr.nameKeyA = nameKeyA
	vwr.nameKeyB = nameKeyB

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

// Delete stores timestampmicro for keyA, keyB, nameKeyA, nameKeyB
// and returns the previously stored timestampmicro or returns any error; a
// newer timestampmicro already in place is not reported as an error. Note that
// with a write and a delete for the exact same timestampmicro, the delete
// wins.
func (vs *DefaultGroupStore) Delete(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, timestampmicro int64) (int64, error) {
	atomic.AddInt32(&vs.deletes, 1)
	if timestampmicro < TIMESTAMPMICRO_MIN {
		atomic.AddInt32(&vs.deleteErrors, 1)
		return 0, fmt.Errorf("timestamp %d < %d", timestampmicro, TIMESTAMPMICRO_MIN)
	}
	if timestampmicro > TIMESTAMPMICRO_MAX {
		atomic.AddInt32(&vs.deleteErrors, 1)
		return 0, fmt.Errorf("timestamp %d > %d", timestampmicro, TIMESTAMPMICRO_MAX)
	}
	ptimestampbits, err := vs.write(keyA, keyB, nameKeyA, nameKeyB, (uint64(timestampmicro)<<_TSB_UTIL_BITS)|_TSB_DELETION, nil, true)
	if err != nil {
		atomic.AddInt32(&vs.deleteErrors, 1)
	}
	if timestampmicro <= int64(ptimestampbits>>_TSB_UTIL_BITS) {
		atomic.AddInt32(&vs.deletesOverridden, 1)
	}
	return int64(ptimestampbits >> _TSB_UTIL_BITS), err
}

func (vs *DefaultGroupStore) locBlock(locBlockID uint32) groupLocBlock {
	return vs.locBlocks[locBlockID]
}

func (vs *DefaultGroupStore) addLocBlock(block groupLocBlock) (uint32, error) {
	id := atomic.AddUint64(&vs.locBlockIDer, 1)
	if id >= math.MaxUint32 {
		return 0, errors.New("too many loc blocks")
	}
	vs.locBlocks[id] = block
	return uint32(id), nil
}

func (vs *DefaultGroupStore) locBlockIDFromTimestampnano(tsn int64) uint32 {
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

func (vs *DefaultGroupStore) closeLocBlock(locBlockID uint32) error {
	return vs.locBlocks[locBlockID].close()
}

func (vs *DefaultGroupStore) memClearer(freeableVMChan chan *groupMem) {
	var tb []byte
	var tbTS int64
	var tbOffset int
	for {
		vm := <-freeableVMChan
		if vm == flushGroupMem {
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
		for vmTOCOffset := 0; vmTOCOffset < len(vm.toc); vmTOCOffset += _GROUP_FILE_ENTRY_SIZE {

			keyA := binary.BigEndian.Uint64(vm.toc[vmTOCOffset:])
			keyB := binary.BigEndian.Uint64(vm.toc[vmTOCOffset+8:])
			nameKeyA := binary.BigEndian.Uint64(vm.toc[vmTOCOffset+16:])
			nameKeyB := binary.BigEndian.Uint64(vm.toc[vmTOCOffset+24:])
			timestampbits := binary.BigEndian.Uint64(vm.toc[vmTOCOffset+32:])

			var blockID uint32
			var offset uint32
			var length uint32
			if timestampbits&_TSB_LOCAL_REMOVAL == 0 {
				blockID = vm.vfID
				offset = vm.vfOffset + binary.BigEndian.Uint32(vm.toc[vmTOCOffset+24:])
				length = binary.BigEndian.Uint32(vm.toc[vmTOCOffset+28:])
			}
			if vs.vlm.Set(keyA, keyB, nameKeyA, nameKeyB, timestampbits, blockID, offset, length, true) > timestampbits {
				continue
			}
			if tb != nil && tbOffset+_GROUP_FILE_ENTRY_SIZE > cap(tb) {
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
			tb = tb[:tbOffset+_GROUP_FILE_ENTRY_SIZE]
			binary.BigEndian.PutUint64(tb[tbOffset:], keyA)
			binary.BigEndian.PutUint64(tb[tbOffset+8:], keyB)
			binary.BigEndian.PutUint64(tb[tbOffset+16:], timestampbits)
			binary.BigEndian.PutUint32(tb[tbOffset+24:], offset)
			binary.BigEndian.PutUint32(tb[tbOffset+28:], length)
			tbOffset += _GROUP_FILE_ENTRY_SIZE
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

func (vs *DefaultGroupStore) memWriter(pendingVWRChan chan *groupWriteReq) {
	var enabled bool
	var vm *groupMem
	var vmTOCOffset int
	var vmMemOffset int
	for {
		vwr := <-pendingVWRChan
		if vwr == enableGroupWriteReq {
			enabled = true
			continue
		}
		if vwr == disableGroupWriteReq {
			enabled = false
			continue
		}
		if vwr == flushGroupWriteReq {
			if vm != nil && len(vm.toc) > 0 {
				vs.vfVMChan <- vm
				vm = nil
			}
			vs.vfVMChan <- flushGroupMem
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
		if vm != nil && (vmTOCOffset+_GROUP_FILE_ENTRY_SIZE > cap(vm.toc) || vmMemOffset+alloc > cap(vm.values)) {
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
		ptimestampbits := vs.vlm.Set(vwr.keyA, vwr.keyB, vwr.nameKeyA, vwr.nameKeyB, vwr.timestampbits, vm.id, uint32(vmMemOffset), uint32(length), false)
		if ptimestampbits < vwr.timestampbits {
			vm.toc = vm.toc[:vmTOCOffset+_GROUP_FILE_ENTRY_SIZE]

			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset:], vwr.keyA)
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+8:], vwr.keyB)
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+16:], vwr.nameKeyA)
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+24:], vwr.nameKeyB)
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+32:], vwr.timestampbits)
			binary.BigEndian.PutUint32(vm.toc[vmTOCOffset+40:], uint32(vmMemOffset))
			binary.BigEndian.PutUint32(vm.toc[vmTOCOffset+44:], uint32(length))

			vmTOCOffset += _GROUP_FILE_ENTRY_SIZE
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

func (vs *DefaultGroupStore) vfWriter() {
	var vf *groupFile
	memWritersFlushLeft := len(vs.pendingVWRChans)
	var tocLen uint64
	var valueLen uint64
	for {
		vm := <-vs.vfVMChan
		if vm == flushGroupMem {
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
				vs.freeableVMChans[i] <- flushGroupMem
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
			vf, err = createGroupFile(vs, osCreateWriteCloser, osOpenReadSeeker)
			if err != nil {
				vs.logCritical("vfWriter: %s\n", err)
				break
			}
			tocLen = _GROUP_FILE_HEADER_SIZE
			valueLen = _GROUP_FILE_HEADER_SIZE
		}
		vf.write(vm)
		tocLen += uint64(len(vm.toc))
		valueLen += uint64(len(vm.values))
	}
}

func (vs *DefaultGroupStore) tocWriter() {
	// writerA is the current toc file while writerB is the previously active
	// toc writerB is kept around in case a "late" key arrives to be flushed
	// whom's value is actually in the previous value file.
	memClearersFlushLeft := len(vs.freeableVMChans)
	var writerA io.WriteCloser
	var offsetA uint64
	var writerB io.WriteCloser
	var offsetB uint64
	var err error
	head := []byte("GROUPSTORETOC v0                ")
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
				fp, err = os.Create(path.Join(vs.pathtoc, fmt.Sprintf("%d.grouptoc", bts)))
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
				offsetA = _GROUP_FILE_HEADER_SIZE + uint64(len(t)-8)
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

func (vs *DefaultGroupStore) recovery() error {
	start := time.Now()
	fromDiskCount := 0
	causedChangeCount := int64(0)
	type writeReq struct {
		keyA uint64
		keyB uint64

		nameKeyA uint64
		nameKeyB uint64

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
						if vs.vlm.Set(wr.keyA, wr.keyB, wr.nameKeyA, wr.nameKeyB, wr.timestampbits, wr.blockID, wr.offset, wr.length, true) < wr.timestampbits {
							atomic.AddInt64(&causedChangeCount, 1)
						}
					} else {
						vs.vlm.Set(wr.keyA, wr.keyB, wr.nameKeyA, wr.nameKeyB, wr.timestampbits, wr.blockID, wr.offset, wr.length, true)
					}
				}
				freeBatchChan <- batch
			}
			wg.Done()
		}(pendingBatchChans[i], freeBatchChans[i])
	}
	fromDiskBuf := make([]byte, vs.checksumInterval+4)
	fromDiskOverflow := make([]byte, 0, _GROUP_FILE_ENTRY_SIZE)
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
		if !strings.HasSuffix(names[i], ".grouptoc") {
			continue
		}
		namets := int64(0)
		if namets, err = strconv.ParseInt(names[i][:len(names[i])-len(".grouptoc")], 10, 64); err != nil {
			vs.logError("bad timestamp in name: %#v\n", names[i])
			continue
		}
		if namets == 0 {
			vs.logError("bad timestamp in name: %#v\n", names[i])
			continue
		}
		vf, err := newGroupFile(vs, namets, osOpenReadSeeker)
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
					if !bytes.Equal(fromDiskBuf[:_GROUP_FILE_HEADER_SIZE-4], []byte("GROUPSTORETOC v0            ")) {
						vs.logError("bad header: %s\n", names[i])
						break
					}
					if binary.BigEndian.Uint32(fromDiskBuf[_GROUP_FILE_HEADER_SIZE-4:]) != vs.checksumInterval {
						vs.logError("bad header checksum interval: %s\n", names[i])
						break
					}
					j += _GROUP_FILE_HEADER_SIZE
					first = false
				}
				if n < int(vs.checksumInterval) {
					if binary.BigEndian.Uint32(fromDiskBuf[n-_GROUP_FILE_TRAILER_SIZE:]) != 0 {
						vs.logError("bad terminator size marker: %s\n", names[i])
						break
					}
					if !bytes.Equal(fromDiskBuf[n-4:n], []byte("TERM")) {
						vs.logError("bad terminator: %s\n", names[i])
						break
					}
					n -= _GROUP_FILE_TRAILER_SIZE
					terminated = true
				}
				if len(fromDiskOverflow) > 0 {
					j += _GROUP_FILE_ENTRY_SIZE - len(fromDiskOverflow)
					fromDiskOverflow = append(fromDiskOverflow, fromDiskBuf[j-_GROUP_FILE_ENTRY_SIZE+len(fromDiskOverflow):j]...)
					keyB := binary.BigEndian.Uint64(fromDiskOverflow[8:])
					k := keyB % workers
					if batches[k] == nil {
						batches[k] = <-freeBatchChans[k]
						batchesPos[k] = 0
					}
					wr := &batches[k][batchesPos[k]]

					wr.keyA = binary.BigEndian.Uint64(fromDiskOverflow)
					wr.keyB = keyB
					wr.nameKeyA = binary.BigEndian.Uint64(fromDiskOverflow[16:])
					wr.nameKeyB = binary.BigEndian.Uint64(fromDiskOverflow[24:])
					wr.timestampbits = binary.BigEndian.Uint64(fromDiskOverflow[32:])
					wr.blockID = vf.id
					wr.offset = binary.BigEndian.Uint32(fromDiskOverflow[40:])
					wr.length = binary.BigEndian.Uint32(fromDiskOverflow[44:])

					batchesPos[k]++
					if batchesPos[k] >= vs.recoveryBatchSize {
						pendingBatchChans[k] <- batches[k]
						batches[k] = nil
					}
					fromDiskCount++
					fromDiskOverflow = fromDiskOverflow[:0]
				}
				for ; j+_GROUP_FILE_ENTRY_SIZE <= n; j += _GROUP_FILE_ENTRY_SIZE {
					keyB := binary.BigEndian.Uint64(fromDiskBuf[j+8:])
					k := keyB % workers
					if batches[k] == nil {
						batches[k] = <-freeBatchChans[k]
						batchesPos[k] = 0
					}
					wr := &batches[k][batchesPos[k]]

					wr.keyA = binary.BigEndian.Uint64(fromDiskBuf[j:])
					wr.keyB = keyB
					wr.nameKeyA = binary.BigEndian.Uint64(fromDiskBuf[j+16:])
					wr.nameKeyB = binary.BigEndian.Uint64(fromDiskBuf[j+24:])
					wr.timestampbits = binary.BigEndian.Uint64(fromDiskBuf[j+32:])
					wr.blockID = vf.id
					wr.offset = binary.BigEndian.Uint32(fromDiskBuf[j+40:])
					wr.length = binary.BigEndian.Uint32(fromDiskBuf[j+44:])

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
		stats := vs.Stats(false).(*GroupStoreStats)
		vs.logInfo("%d key locations loaded in %s, %.0f/s; %d caused change; %d resulting locations referencing %d bytes.\n", fromDiskCount, dur, float64(fromDiskCount)/(float64(dur)/float64(time.Second)), causedChangeCount, stats.Values, stats.ValueBytes)
	}
	return nil
}
