package store

import (
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

	"github.com/gholt/locmap"
	"github.com/gholt/ring"
	"github.com/spaolacci/murmur3"
	"gopkg.in/gholt/brimutil.v1"
)

// DefaultValueStore instances are created with NewValueStore.
type DefaultValueStore struct {
	logCritical             LogFunc
	logError                LogFunc
	logWarning              LogFunc
	logInfo                 LogFunc
	logDebug                LogFunc
	randMutex               sync.Mutex
	rand                    *rand.Rand
	freeableMemBlockChans   []chan *valueMemBlock
	freeMemBlockChan        chan *valueMemBlock
	freeWriteReqChans       []chan *valueWriteReq
	pendingWriteReqChans    []chan *valueWriteReq
	fileMemBlockChan        chan *valueMemBlock
	freeTOCBlockChan        chan *valueTOCBlock
	pendingTOCBlockChan     chan *valueTOCBlock
	activeTOCA              uint64
	activeTOCB              uint64
	flushedChan             chan struct{}
	locBlocks               []valueLocBlock
	locBlockIDer            uint64
	path                    string
	pathtoc                 string
	locmap                  locmap.ValueLocMap
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
	auditState              valueAuditState
	replicationIgnoreRecent uint64
	pullReplicationState    valuePullReplicationState
	pushReplicationState    valuePushReplicationState
	compactionState         valueCompactionState
	bulkSetState            valueBulkSetState
	bulkSetAckState         valueBulkSetAckState
	disableEnableWritesLock sync.Mutex
	userDisabled            bool
	flusherState            valueFlusherState
	diskWatcherState        valueDiskWatcherState
	restartChan             chan error

	statsLock                    sync.Mutex
	lookups                      int32
	lookupErrors                 int32
	lookupGroups                 int32
	lookupGroupItems             int32
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

	// Used by the flusher only
	modifications int32
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
var flushValueMemBlock *valueMemBlock = &valueMemBlock{}
var flushValueTOCBlock *valueTOCBlock = &valueTOCBlock{}

type valueTOCBlock struct {
	data []byte
}

type valueLocBlock interface {
	timestampnano() int64
	read(keyA uint64, keyB uint64, timestampbits uint64, offset uint32, length uint32, value []byte) (uint64, []byte, error)
	close() error
}

// NewValueStore creates a DefaultValueStore for use in storing []byte values
// referenced by 128 bit keys; the store, restart channel (chan error), or any
// error during construction is returned.
//
// The restart channel should be read from continually during the life of the
// store and, upon any error from the channel, the store should be discarded
// and a new one created in its place. This restart procedure is needed when
// data on disk is detected as corrupted and cannot be easily recovered from; a
// restart will cause only good entries to be loaded therefore discarding any
// bad entries due to the corruption. A restart may also be requested if the
// store reaches an unrecoverable state, such as no longer being able to open
// new files.
//
// Note that a lot of buffering, multiple cores, and background processes can
// be in use and therefore DisableAll() and Flush() should be called prior to
// the process exiting to ensure all processing is done and the buffers are
// flushed.
func NewValueStore(c *ValueStoreConfig) (*DefaultValueStore, chan error, error) {
	cfg := resolveValueStoreConfig(c)
	lcmap := cfg.ValueLocMap
	if lcmap == nil {
		lcmap = locmap.NewValueLocMap(nil)
	}
	lcmap.SetInactiveMask(_TSB_INACTIVE)
	store := &DefaultValueStore{
		logCritical:             cfg.LogCritical,
		logError:                cfg.LogError,
		logWarning:              cfg.LogWarning,
		logInfo:                 cfg.LogInfo,
		logDebug:                cfg.LogDebug,
		rand:                    cfg.Rand,
		locBlocks:               make([]valueLocBlock, math.MaxUint16),
		path:                    cfg.Path,
		pathtoc:                 cfg.PathTOC,
		locmap:                  lcmap,
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
		restartChan:             make(chan error),
	}
	// freeableMemBlockChans is a slice of channels so that the individual
	// memClearers can be communicated with later (flushes, etc.)
	store.freeableMemBlockChans = make([]chan *valueMemBlock, store.workers)
	for i := 0; i < cap(store.freeableMemBlockChans); i++ {
		store.freeableMemBlockChans[i] = make(chan *valueMemBlock, store.workers)
	}
	store.freeMemBlockChan = make(chan *valueMemBlock, store.workers*store.writePagesPerWorker)
	store.freeWriteReqChans = make([]chan *valueWriteReq, store.workers)
	store.pendingWriteReqChans = make([]chan *valueWriteReq, store.workers)
	store.fileMemBlockChan = make(chan *valueMemBlock, store.workers)
	store.freeTOCBlockChan = make(chan *valueTOCBlock, store.workers*2)
	store.pendingTOCBlockChan = make(chan *valueTOCBlock, store.workers)
	store.flushedChan = make(chan struct{}, 1)
	for i := 0; i < cap(store.freeMemBlockChan); i++ {
		memBlock := &valueMemBlock{
			store:  store,
			toc:    make([]byte, 0, store.pageSize),
			values: make([]byte, 0, store.pageSize),
		}
		var err error
		memBlock.id, err = store.addLocBlock(memBlock)
		if err != nil {
			return nil, nil, err
		}
		store.freeMemBlockChan <- memBlock
	}
	for i := 0; i < len(store.freeWriteReqChans); i++ {
		store.freeWriteReqChans[i] = make(chan *valueWriteReq, store.workers*2)
		for j := 0; j < store.workers*2; j++ {
			store.freeWriteReqChans[i] <- &valueWriteReq{errChan: make(chan error, 1)}
		}
	}
	for i := 0; i < len(store.pendingWriteReqChans); i++ {
		store.pendingWriteReqChans[i] = make(chan *valueWriteReq)
	}
	for i := 0; i < cap(store.freeTOCBlockChan); i++ {
		store.freeTOCBlockChan <- &valueTOCBlock{make([]byte, 0, store.pageSize)}
	}
	go store.tocWriter()
	go store.fileWriter()
	for i := 0; i < len(store.freeableMemBlockChans); i++ {
		go store.memClearer(store.freeableMemBlockChans[i])
	}
	for i := 0; i < len(store.pendingWriteReqChans); i++ {
		go store.memWriter(store.pendingWriteReqChans[i])
	}
	store.tombstoneDiscardConfig(cfg)
	store.compactionConfig(cfg)
	store.auditConfig(cfg)
	store.pullReplicationConfig(cfg)
	store.pushReplicationConfig(cfg)
	store.bulkSetConfig(cfg)
	store.bulkSetAckConfig(cfg)
	store.flusherConfig(cfg)
	store.diskWatcherConfig(cfg)
	err := store.recovery()
	if err != nil {
		return nil, nil, err
	}
	return store, store.restartChan, nil
}

// ValueCap returns the maximum length of a value the ValueStore can accept.
func (store *DefaultValueStore) ValueCap() uint32 {
	return store.valueCap
}

func (store *DefaultValueStore) DisableAll() {
	store.DisableAllBackground()
	store.DisableWrites()
}

func (store *DefaultValueStore) DisableAllBackground() {
	wg := &sync.WaitGroup{}
	for i, f := range []func(){
		store.DisableDiskWatcher,
		store.DisableFlusher,
		store.DisableAudit,
		store.DisableCompaction,
		store.DisableInPullReplication,
		store.DisableOutPullReplication,
		store.DisableOutPushReplication,
		store.DisableInBulkSet,
		store.DisableInBulkSetAck,
		store.DisableTombstoneDiscard,
	} {
		wg.Add(1)
		go func(ii int, ff func()) {
			ff()
			wg.Done()
		}(i, f)
	}
	wg.Wait()
}

func (store *DefaultValueStore) EnableAll() {
	wg := &sync.WaitGroup{}
	for _, f := range []func(){
		store.EnableWrites,
		store.EnableTombstoneDiscard,
		store.EnableInBulkSetAck,
		store.EnableInBulkSet,
		store.EnableOutPushReplication,
		store.EnableOutPullReplication,
		store.EnableInPullReplication,
		store.EnableCompaction,
		store.EnableAudit,
		store.EnableFlusher,
		store.EnableDiskWatcher,
	} {
		wg.Add(1)
		go func(ff func()) {
			ff()
			wg.Done()
		}(f)
	}
	wg.Wait()
}

// DisableWrites will cause any incoming Write or Delete requests to respond
// with ErrDisabled until EnableWrites is called.
func (store *DefaultValueStore) DisableWrites() {
	store.disableWrites(true)
}

func (store *DefaultValueStore) disableWrites(userCall bool) {
	store.disableEnableWritesLock.Lock()
	if userCall {
		store.userDisabled = true
	}
	for _, c := range store.pendingWriteReqChans {
		c <- disableValueWriteReq
	}
	store.disableEnableWritesLock.Unlock()
}

// EnableWrites will resume accepting incoming Write and Delete requests.
func (store *DefaultValueStore) EnableWrites() {
	store.enableWrites(true)
}

func (store *DefaultValueStore) enableWrites(userCall bool) {
	store.disableEnableWritesLock.Lock()
	if userCall || !store.userDisabled {
		store.userDisabled = false
		for _, c := range store.pendingWriteReqChans {
			c <- enableValueWriteReq
		}
	}
	store.disableEnableWritesLock.Unlock()
}

// Flush will ensure buffered data (at the time of the call) is written to
// disk.
func (store *DefaultValueStore) Flush() {
	for _, c := range store.pendingWriteReqChans {
		c <- flushValueWriteReq
	}
	<-store.flushedChan
}

// Lookup will return timestampmicro, length, err for keyA, keyB.
//
// Note that err == ErrNotFound with timestampmicro == 0 indicates keyA, keyB
// was not known at all whereas err == ErrNotFound with timestampmicro != 0
// indicates keyA, keyB
// was known and had a deletion marker (aka tombstone).
func (store *DefaultValueStore) Lookup(keyA uint64, keyB uint64) (int64, uint32, error) {
	atomic.AddInt32(&store.lookups, 1)
	timestampbits, _, length, err := store.lookup(keyA, keyB)
	if err != nil {
		atomic.AddInt32(&store.lookupErrors, 1)
	}
	return int64(timestampbits >> _TSB_UTIL_BITS), length, err
}

func (store *DefaultValueStore) lookup(keyA uint64, keyB uint64) (uint64, uint32, uint32, error) {
	timestampbits, id, _, length := store.locmap.Get(keyA, keyB)
	if id == 0 || timestampbits&_TSB_DELETION != 0 {
		return timestampbits, id, 0, ErrNotFound
	}
	return timestampbits, id, length, nil
}

// Read will return timestampmicro, value, err for keyA, keyB;
// if an incoming value is provided, the read value will be appended to it and
// the whole returned (useful to reuse an existing []byte).
//
// Note that err == ErrNotFound with timestampmicro == 0 indicates keyA, keyB
// was not known at all whereas err == ErrNotFound with timestampmicro != 0
// indicates keyA, keyB was known and had a deletion marker (aka tombstone).
func (store *DefaultValueStore) Read(keyA uint64, keyB uint64, value []byte) (int64, []byte, error) {
	atomic.AddInt32(&store.reads, 1)
	timestampbits, value, err := store.read(keyA, keyB, value)
	if err != nil {
		atomic.AddInt32(&store.readErrors, 1)
	}
	return int64(timestampbits >> _TSB_UTIL_BITS), value, err
}

func (store *DefaultValueStore) read(keyA uint64, keyB uint64, value []byte) (uint64, []byte, error) {
	timestampbits, id, offset, length := store.locmap.Get(keyA, keyB)
	if id == 0 || timestampbits&_TSB_DELETION != 0 || timestampbits&_TSB_LOCAL_REMOVAL != 0 {
		return timestampbits, value, ErrNotFound
	}
	return store.locBlock(id).read(keyA, keyB, timestampbits, offset, length, value)
}

// Write stores timestampmicro, value for keyA, keyB
// and returns the previously stored timestampmicro or returns any error; a
// newer timestampmicro already in place is not reported as an error. Note that
// with a write and a delete for the exact same timestampmicro, the delete
// wins.
func (store *DefaultValueStore) Write(keyA uint64, keyB uint64, timestampmicro int64, value []byte) (int64, error) {
	atomic.AddInt32(&store.writes, 1)
	if timestampmicro < TIMESTAMPMICRO_MIN {
		atomic.AddInt32(&store.writeErrors, 1)
		return 0, fmt.Errorf("timestamp %d < %d", timestampmicro, TIMESTAMPMICRO_MIN)
	}
	if timestampmicro > TIMESTAMPMICRO_MAX {
		atomic.AddInt32(&store.writeErrors, 1)
		return 0, fmt.Errorf("timestamp %d > %d", timestampmicro, TIMESTAMPMICRO_MAX)
	}
	timestampbits, err := store.write(keyA, keyB, uint64(timestampmicro)<<_TSB_UTIL_BITS, value, false)
	if err != nil {
		atomic.AddInt32(&store.writeErrors, 1)
	} else if timestampmicro <= int64(timestampbits>>_TSB_UTIL_BITS) {
		atomic.AddInt32(&store.writesOverridden, 1)
	}
	return int64(timestampbits >> _TSB_UTIL_BITS), err
}

func (store *DefaultValueStore) write(keyA uint64, keyB uint64, timestampbits uint64, value []byte, internal bool) (uint64, error) {
	i := int(keyA>>1) % len(store.freeWriteReqChans)
	writeReq := <-store.freeWriteReqChans[i]
	writeReq.keyA = keyA
	writeReq.keyB = keyB

	writeReq.timestampbits = timestampbits
	writeReq.value = value
	writeReq.internal = internal
	store.pendingWriteReqChans[i] <- writeReq
	err := <-writeReq.errChan
	ptimestampbits := writeReq.timestampbits
	writeReq.value = nil
	store.freeWriteReqChans[i] <- writeReq
	// This is for the flusher
	if err == nil && ptimestampbits < timestampbits {
		atomic.AddInt32(&store.modifications, 1)
	}
	return ptimestampbits, err
}

// Delete stores timestampmicro for keyA, keyB
// and returns the previously stored timestampmicro or returns any error; a
// newer timestampmicro already in place is not reported as an error. Note that
// with a write and a delete for the exact same timestampmicro, the delete
// wins.
func (store *DefaultValueStore) Delete(keyA uint64, keyB uint64, timestampmicro int64) (int64, error) {
	atomic.AddInt32(&store.deletes, 1)
	if timestampmicro < TIMESTAMPMICRO_MIN {
		atomic.AddInt32(&store.deleteErrors, 1)
		return 0, fmt.Errorf("timestamp %d < %d", timestampmicro, TIMESTAMPMICRO_MIN)
	}
	if timestampmicro > TIMESTAMPMICRO_MAX {
		atomic.AddInt32(&store.deleteErrors, 1)
		return 0, fmt.Errorf("timestamp %d > %d", timestampmicro, TIMESTAMPMICRO_MAX)
	}
	ptimestampbits, err := store.write(keyA, keyB, (uint64(timestampmicro)<<_TSB_UTIL_BITS)|_TSB_DELETION, nil, true)
	if err != nil {
		atomic.AddInt32(&store.deleteErrors, 1)
	} else if timestampmicro <= int64(ptimestampbits>>_TSB_UTIL_BITS) {
		atomic.AddInt32(&store.deletesOverridden, 1)
	}
	return int64(ptimestampbits >> _TSB_UTIL_BITS), err
}

func (store *DefaultValueStore) locBlock(locBlockID uint32) valueLocBlock {
	return store.locBlocks[locBlockID]
}

func (store *DefaultValueStore) addLocBlock(block valueLocBlock) (uint32, error) {
	id := atomic.AddUint64(&store.locBlockIDer, 1)
	// TODO: We should probably issue a restart request if
	// id >= math.MaxUint32 / 2 since it's almost certainly not the case that
	// there are too many on-disk files, just that the process has been running
	// long enough to chew through ids. Issuing the restart at half the true
	// max would all but guarantee a restart occurs before reaching the true
	// max.
	if id >= math.MaxUint32 {
		return 0, errors.New("too many loc blocks")
	}
	store.locBlocks[id] = block
	return uint32(id), nil
}

func (store *DefaultValueStore) locBlockIDFromTimestampnano(tsn int64) uint32 {
	for i := 1; i <= len(store.locBlocks); i++ {
		if store.locBlocks[i] == nil {
			return 0
		} else {
			if tsn == store.locBlocks[i].timestampnano() {
				return uint32(i)
			}
		}
	}
	return 0
}

func (store *DefaultValueStore) closeLocBlock(locBlockID uint32) error {
	return store.locBlocks[locBlockID].close()
}

func (store *DefaultValueStore) memClearer(freeableMemBlockChan chan *valueMemBlock) {
	var tb *valueTOCBlock
	var tbTS int64
	var tbOffset int
	for {
		memBlock := <-freeableMemBlockChan
		if memBlock == flushValueMemBlock {
			if tb != nil {
				store.pendingTOCBlockChan <- tb
				tb = nil
			}
			store.pendingTOCBlockChan <- flushValueTOCBlock
			continue
		}
		fl := store.locBlock(memBlock.fileID)
		if tb != nil && tbTS != fl.timestampnano() {
			store.pendingTOCBlockChan <- tb
			tb = nil
		}
		for memBlockTOCOffset := 0; memBlockTOCOffset < len(memBlock.toc); memBlockTOCOffset += _VALUE_FILE_ENTRY_SIZE {

			keyA := binary.BigEndian.Uint64(memBlock.toc[memBlockTOCOffset:])
			keyB := binary.BigEndian.Uint64(memBlock.toc[memBlockTOCOffset+8:])
			timestampbits := binary.BigEndian.Uint64(memBlock.toc[memBlockTOCOffset+16:])

			var blockID uint32
			var offset uint32
			var length uint32
			if timestampbits&_TSB_LOCAL_REMOVAL == 0 {
				blockID = memBlock.fileID

				offset = memBlock.fileOffset + binary.BigEndian.Uint32(memBlock.toc[memBlockTOCOffset+24:])
				length = binary.BigEndian.Uint32(memBlock.toc[memBlockTOCOffset+28:])

			}
			if store.locmap.Set(keyA, keyB, timestampbits, blockID, offset, length, true) > timestampbits {
				continue
			}
			if tb != nil && tbOffset+_VALUE_FILE_ENTRY_SIZE > cap(tb.data) {
				store.pendingTOCBlockChan <- tb
				tb = nil
			}
			if tb == nil {
				tb = <-store.freeTOCBlockChan
				tbTS = fl.timestampnano()
				tb.data = tb.data[:8]
				binary.BigEndian.PutUint64(tb.data, uint64(tbTS))
				tbOffset = 8
			}
			tb.data = tb.data[:tbOffset+_VALUE_FILE_ENTRY_SIZE]
			tbd := tb.data[tbOffset : tbOffset+_VALUE_FILE_ENTRY_SIZE]

			binary.BigEndian.PutUint64(tbd, keyA)
			binary.BigEndian.PutUint64(tbd[8:], keyB)
			binary.BigEndian.PutUint64(tbd[16:], timestampbits)
			binary.BigEndian.PutUint32(tbd[24:], offset)
			binary.BigEndian.PutUint32(tbd[28:], length)

			tbOffset += _VALUE_FILE_ENTRY_SIZE
		}
		memBlock.discardLock.Lock()
		memBlock.fileID = 0
		memBlock.fileOffset = 0
		memBlock.toc = memBlock.toc[:0]
		memBlock.values = memBlock.values[:0]
		memBlock.discardLock.Unlock()
		store.freeMemBlockChan <- memBlock
	}
}

func (store *DefaultValueStore) memWriter(pendingWriteReqChan chan *valueWriteReq) {
	var enabled bool
	var memBlock *valueMemBlock
	var memBlockTOCOffset int
	var memBlockMemOffset int
	for {
		writeReq := <-pendingWriteReqChan
		if writeReq == enableValueWriteReq {
			enabled = true
			continue
		}
		if writeReq == disableValueWriteReq {
			enabled = false
			continue
		}
		if writeReq == flushValueWriteReq {
			if memBlock != nil && len(memBlock.toc) > 0 {
				store.fileMemBlockChan <- memBlock
				memBlock = nil
			}
			store.fileMemBlockChan <- flushValueMemBlock
			continue
		}
		if !enabled && !writeReq.internal {
			writeReq.errChan <- ErrDisabled
			continue
		}
		length := len(writeReq.value)
		if length > int(store.valueCap) {
			writeReq.errChan <- fmt.Errorf("value length of %d > %d", length, store.valueCap)
			continue
		}
		alloc := length
		if alloc < store.minValueAlloc {
			alloc = store.minValueAlloc
		}
		if memBlock != nil && (memBlockTOCOffset+_VALUE_FILE_ENTRY_SIZE > cap(memBlock.toc) || memBlockMemOffset+alloc > cap(memBlock.values)) {
			store.fileMemBlockChan <- memBlock
			memBlock = nil
		}
		if memBlock == nil {
			memBlock = <-store.freeMemBlockChan
			memBlockTOCOffset = 0
			memBlockMemOffset = 0
		}
		memBlock.discardLock.Lock()
		memBlock.values = memBlock.values[:memBlockMemOffset+alloc]
		memBlock.discardLock.Unlock()
		copy(memBlock.values[memBlockMemOffset:], writeReq.value)
		if alloc > length {
			for i, j := memBlockMemOffset+length, memBlockMemOffset+alloc; i < j; i++ {
				memBlock.values[i] = 0
			}
		}
		ptimestampbits := store.locmap.Set(writeReq.keyA, writeReq.keyB, writeReq.timestampbits & ^uint64(_TSB_COMPACTION_REWRITE), memBlock.id, uint32(memBlockMemOffset), uint32(length), writeReq.timestampbits&_TSB_COMPACTION_REWRITE != 0)
		if ptimestampbits < writeReq.timestampbits {
			memBlock.toc = memBlock.toc[:memBlockTOCOffset+_VALUE_FILE_ENTRY_SIZE]

			binary.BigEndian.PutUint64(memBlock.toc[memBlockTOCOffset:], writeReq.keyA)
			binary.BigEndian.PutUint64(memBlock.toc[memBlockTOCOffset+8:], writeReq.keyB)
			binary.BigEndian.PutUint64(memBlock.toc[memBlockTOCOffset+16:], writeReq.timestampbits & ^uint64(_TSB_COMPACTION_REWRITE))
			binary.BigEndian.PutUint32(memBlock.toc[memBlockTOCOffset+24:], uint32(memBlockMemOffset))
			binary.BigEndian.PutUint32(memBlock.toc[memBlockTOCOffset+28:], uint32(length))

			memBlockTOCOffset += _VALUE_FILE_ENTRY_SIZE
			memBlockMemOffset += alloc
		} else {
			memBlock.discardLock.Lock()
			memBlock.values = memBlock.values[:memBlockMemOffset]
			memBlock.discardLock.Unlock()
		}
		writeReq.timestampbits = ptimestampbits
		writeReq.errChan <- nil
	}
}

func (store *DefaultValueStore) fileWriter() {
	var fl *valueStoreFile
	memWritersFlushLeft := len(store.pendingWriteReqChans)
	var tocLen uint64
	var valueLen uint64
	var disabledDueToError error
	freeableMemBlockChanIndex := 0
	var disabledDueToErrorLogTime time.Time
	for {
		memBlock := <-store.fileMemBlockChan
		if memBlock == flushValueMemBlock {
			memWritersFlushLeft--
			if memWritersFlushLeft > 0 {
				continue
			}
			if fl != nil {
				err := fl.closeWriting()
				if err != nil {
					// TODO: Trigger an audit based on this file being in an
					// unknown state.
					store.logError("fileWriter: error closing %s: %s", fl.fullPath, err)
				}
				fl = nil
			}
			for i := 0; i < len(store.freeableMemBlockChans); i++ {
				store.freeableMemBlockChans[i] <- flushValueMemBlock
			}
			memWritersFlushLeft = len(store.pendingWriteReqChans)
			continue
		}
		if disabledDueToError != nil {
			if disabledDueToErrorLogTime.Before(time.Now()) {
				store.logCritical("fileWriter: disabled due to previous critical error: %s", disabledDueToError)
				disabledDueToErrorLogTime = time.Now().Add(5 * time.Minute)
			}
			store.freeableMemBlockChans[freeableMemBlockChanIndex] <- memBlock
			freeableMemBlockChanIndex++
			if freeableMemBlockChanIndex >= len(store.freeableMemBlockChans) {
				freeableMemBlockChanIndex = 0
			}
			continue
		}
		if fl != nil && (tocLen+uint64(len(memBlock.toc)) >= uint64(store.fileCap) || valueLen+uint64(len(memBlock.values)) > uint64(store.fileCap)) {
			err := fl.closeWriting()
			if err != nil {
				// TODO: Trigger an audit based on this file being in an
				// unknown state.
				store.logError("fileWriter: error closing %s: %s", fl.fullPath, err)
			}
			fl = nil
		}
		if fl == nil {
			var err error
			fl, err = createValueReadWriteFile(store, osCreateWriteCloser, osOpenReadSeeker)
			if err != nil {
				store.logCritical("fileWriter: must shutdown because no new files can be opened: %s", err)
				disabledDueToError = err
				disabledDueToErrorLogTime = time.Now().Add(5 * time.Minute)
				go func() {
					store.DisableAll()
					store.Flush()
					store.restartChan <- errors.New("no new files can be opened")
				}()
			}
			tocLen = _VALUE_FILE_HEADER_SIZE
			valueLen = _VALUE_FILE_HEADER_SIZE
		}
		fl.write(memBlock)
		tocLen += uint64(len(memBlock.toc))
		valueLen += uint64(len(memBlock.values))
	}
}

func (store *DefaultValueStore) tocWriter() {
	// writerA is the current toc file while writerB is the previously active
	// toc writerB is kept around in case a "late" key arrives to be flushed
	// whom's value is actually in the previous value file.
	memClearersFlushLeft := len(store.freeableMemBlockChans)
	var writerA io.WriteCloser
	var offsetA uint64
	var writerB io.WriteCloser
	var offsetB uint64
	var err error
	head := []byte("VALUESTORETOC v0                ")
	binary.BigEndian.PutUint32(head[28:], uint32(store.checksumInterval))
	// Make sure any trailing data is covered by a checksum by writing an
	// additional block of zeros (entry offsets of zero are ignored on
	// recovery).
	term := make([]byte, store.checksumInterval)
	copy(term[len(term)-8:], []byte("TERM v0 "))
OuterLoop:
	for {
		t := <-store.pendingTOCBlockChan
		if t == flushValueTOCBlock {
			memClearersFlushLeft--
			if memClearersFlushLeft > 0 {
				continue
			}
			if writerB != nil {
				if _, err = writerB.Write(term); err != nil {
					break OuterLoop
				}
				if err = writerB.Close(); err != nil {
					break OuterLoop
				}
				writerB = nil
				atomic.StoreUint64(&store.activeTOCB, 0)
				offsetB = 0
			}
			if writerA != nil {
				if _, err = writerA.Write(term); err != nil {
					break OuterLoop
				}
				if err = writerA.Close(); err != nil {
					break OuterLoop
				}
				writerA = nil
				atomic.StoreUint64(&store.activeTOCA, 0)
				offsetA = 0
			}
			store.flushedChan <- struct{}{}
			memClearersFlushLeft = len(store.freeableMemBlockChans)
			continue
		}
		if len(t.data) > 8 {
			bts := binary.BigEndian.Uint64(t.data)
			switch bts {
			case atomic.LoadUint64(&store.activeTOCA):
				if _, err = writerA.Write(t.data[8:]); err != nil {
					break OuterLoop
				}
				offsetA += uint64(len(t.data) - 8)
			case atomic.LoadUint64(&store.activeTOCB):
				if _, err = writerB.Write(t.data[8:]); err != nil {
					break OuterLoop
				}
				offsetB += uint64(len(t.data) - 8)
			default:
				// An assumption is made here: If the timestampnano for this
				// toc block doesn't match the last two seen timestampnanos
				// then we expect no more toc blocks for the oldest
				// timestampnano and can close that toc file.
				if writerB != nil {
					if _, err = writerB.Write(term); err != nil {
						break OuterLoop
					}
					if err = writerB.Close(); err != nil {
						break OuterLoop
					}
				}
				atomic.StoreUint64(&store.activeTOCB, atomic.LoadUint64(&store.activeTOCA))
				writerB = writerA
				offsetB = offsetA
				atomic.StoreUint64(&store.activeTOCA, bts)
				var fp *os.File
				fp, err = os.Create(path.Join(store.pathtoc, fmt.Sprintf("%d.valuetoc", bts)))
				if err != nil {
					break OuterLoop
				}
				writerA = brimutil.NewMultiCoreChecksummedWriter(fp, int(store.checksumInterval), murmur3.New32, store.workers)
				if _, err = writerA.Write(head); err != nil {
					break OuterLoop
				}
				if _, err = writerA.Write(t.data[8:]); err != nil {
					break OuterLoop
				}
				offsetA = _VALUE_FILE_HEADER_SIZE + uint64(len(t.data)-8)
			}
		}
		store.freeTOCBlockChan <- t
	}
	if err != nil {
		store.logCritical("tocWriter: %s", err)
	}
	if writerA != nil {
		writerA.Close()
	}
	if writerB != nil {
		writerB.Close()
	}
}

func (store *DefaultValueStore) recovery() error {
	start := time.Now()
	causedChangeCount := int64(0)
	workers := uint64(store.workers)
	pendingBatchChans := make([]chan []valueTOCEntry, workers)
	freeBatchChans := make([]chan []valueTOCEntry, len(pendingBatchChans))
	for i := 0; i < len(pendingBatchChans); i++ {
		pendingBatchChans[i] = make(chan []valueTOCEntry, 3)
		freeBatchChans[i] = make(chan []valueTOCEntry, cap(pendingBatchChans[i]))
		for j := 0; j < cap(freeBatchChans[i]); j++ {
			freeBatchChans[i] <- make([]valueTOCEntry, store.recoveryBatchSize)
		}
	}
	wg := &sync.WaitGroup{}
	wg.Add(len(pendingBatchChans))
	for i := 0; i < len(pendingBatchChans); i++ {
		go func(pendingBatchChan chan []valueTOCEntry, freeBatchChan chan []valueTOCEntry) {
			for {
				batch := <-pendingBatchChan
				if batch == nil {
					break
				}
				for j := 0; j < len(batch); j++ {
					wr := &batch[j]
					if wr.TimestampBits&_TSB_LOCAL_REMOVAL != 0 {
						wr.BlockID = 0
					}
					if store.logDebug != nil {
						if store.locmap.Set(wr.KeyA, wr.KeyB, wr.TimestampBits, wr.BlockID, wr.Offset, wr.Length, true) < wr.TimestampBits {
							atomic.AddInt64(&causedChangeCount, 1)
						}
					} else {
						store.locmap.Set(wr.KeyA, wr.KeyB, wr.TimestampBits, wr.BlockID, wr.Offset, wr.Length, true)
					}
				}
				freeBatchChan <- batch
			}
			wg.Done()
		}(pendingBatchChans[i], freeBatchChans[i])
	}
	spindown := func() {
		for i := 0; i < len(pendingBatchChans); i++ {
			pendingBatchChans[i] <- nil
		}
		wg.Wait()
	}
	fp, err := os.Open(store.pathtoc)
	if err != nil {
		spindown()
		return err
	}
	names, err := fp.Readdirnames(-1)
	fp.Close()
	if err != nil {
		spindown()
		return err
	}
	fromDiskCount := 0
	sort.Strings(names)
	var compactNames []string
	var compactBlockIDs []uint32
	for i := 0; i < len(names); i++ {
		if !strings.HasSuffix(names[i], ".valuetoc") {
			continue
		}
		namets := int64(0)
		if namets, err = strconv.ParseInt(names[i][:len(names[i])-len(".valuetoc")], 10, 64); err != nil {
			store.logError("recovery: bad timestamp in name: %#v", names[i])
			continue
		}
		if namets == 0 {
			store.logError("recovery: bad timestamp in name: %#v", names[i])
			continue
		}
		fpr, err := osOpenReadSeeker(path.Join(store.pathtoc, names[i]))
		if err != nil {
			store.logError("recovery: error opening %s: %s", names[i], err)
			continue
		}
		fl, err := newValueReadFile(store, namets, osOpenReadSeeker)
		if err != nil {
			store.logError("recovery: error opening %s: %s", names[i][:len(names[i])-3], err)
			closeIfCloser(fpr)
			continue
		}
		fdc, errs := valueReadTOCEntriesBatched(fpr, fl.id, freeBatchChans, pendingBatchChans, make(chan struct{}))
		fromDiskCount += fdc
		for _, err := range errs {
			store.logError("recovery: error with %s: %s", names[i], err)
			// TODO: The auditor should catch this eventually, but we should be
			// proactive and notify the auditor of the issue here.
		}
		if len(errs) != 0 {
			compactNames = append(compactNames, names[i])
			compactBlockIDs = append(compactBlockIDs, fl.id)
		}
		closeIfCloser(fpr)
	}
	spindown()
	if store.logDebug != nil {
		dur := time.Now().Sub(start)
		stats := store.Stats(false).(*ValueStoreStats)
		store.logDebug("recovery: %d key locations loaded in %s, %.0f/s; %d caused change; %d resulting locations referencing %d bytes.", fromDiskCount, dur, float64(fromDiskCount)/(float64(dur)/float64(time.Second)), causedChangeCount, stats.Values, stats.ValueBytes)
	}
	if len(compactNames) > 0 {
		if store.logDebug != nil {
			store.logDebug("recovery: secondary recovery started for %d files.", len(compactNames))
		}
		for i, name := range compactNames {
			store.compactFile(name, compactBlockIDs[i], make(chan struct{}))
		}
		if store.logDebug != nil {
			store.logDebug("recovery: secondary recovery completed.")
		}
	}
	return nil
}
