package brimstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/brimstore/ktbloomfilter"
	"github.com/gholt/brimstore/valuelocmap"
	"github.com/gholt/brimtext"
	"github.com/gholt/brimutil"
	"github.com/spaolacci/murmur3"
)

type config struct {
	cores              int
	backgroundCores    int
	backgroundInterval int
	maxValueSize       int
	checksumInterval   int
	pageSize           int
	minValueAlloc      int
	writePagesPerCore  int
	tombstoneAge       int
	valuesFileSize     int
	valuesFileReaders  int
}

func resolveConfig(opts ...func(*config)) *config {
	cfg := &config{}
	cfg.cores = runtime.GOMAXPROCS(0)
	if env := os.Getenv("BRIMSTORE_CORES"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.cores = val
		}
	}
	cfg.backgroundCores = 1
	if env := os.Getenv("BRIMSTORE_BACKGROUNDCORES"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.backgroundCores = val
		}
	}
	cfg.backgroundInterval = 60
	if env := os.Getenv("BRIMSTORE_BACKGROUNDINTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.backgroundInterval = val
		}
	}
	cfg.maxValueSize = 4 * 1024 * 1024
	if env := os.Getenv("BRIMSTORE_MAXVALUESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.maxValueSize = val
		}
	}
	cfg.checksumInterval = 65532
	if env := os.Getenv("BRIMSTORE_CHECKSUMINTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.checksumInterval = val
		}
	}
	cfg.pageSize = 4 * 1024 * 1024
	if env := os.Getenv("BRIMSTORE_PAGESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.pageSize = val
		}
	}
	cfg.writePagesPerCore = 3
	if env := os.Getenv("BRIMSTORE_WRITEPAGESPERCORE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.writePagesPerCore = val
		}
	}
	cfg.tombstoneAge = 4 * 60 * 60
	if env := os.Getenv("BRIMSTORE_TOMBSTONEAGE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.tombstoneAge = val
		}
	}
	cfg.valuesFileSize = math.MaxUint32
	if env := os.Getenv("BRIMSTORE_VALUESFILESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.valuesFileSize = val
		}
	}
	cfg.valuesFileReaders = cfg.cores
	if env := os.Getenv("BRIMSTORE_VALUESFILEREADERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.valuesFileReaders = val
		}
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.cores < 1 {
		cfg.cores = 1
	}
	if cfg.backgroundCores < 1 {
		cfg.backgroundCores = 1
	}
	if cfg.backgroundInterval < 1 {
		cfg.backgroundInterval = 1
	}
	if cfg.maxValueSize < 0 {
		cfg.maxValueSize = 0
	}
	if cfg.maxValueSize > math.MaxUint32 {
		cfg.maxValueSize = math.MaxUint32
	}
	if cfg.checksumInterval < 1 {
		cfg.checksumInterval = 1
	}
	// Ensure each page will have at least checksumInterval worth of data in it
	// so that each page written will at least flush the previous page's data.
	if cfg.pageSize < cfg.maxValueSize+cfg.checksumInterval {
		cfg.pageSize = cfg.maxValueSize + cfg.checksumInterval
	}
	// Absolute minimum: timestamp leader plus at least one TOC entry
	if cfg.pageSize < 40 {
		cfg.pageSize = 40
	}
	if cfg.pageSize > math.MaxUint32 {
		cfg.pageSize = math.MaxUint32
	}
	// Ensure a full TOC page will have an associated data page of at least
	// checksumInterval in size, again so that each page written will at least
	// flush the previous page's data.
	cfg.minValueAlloc = cfg.checksumInterval/(cfg.pageSize/32+1) + 1
	if cfg.writePagesPerCore < 2 {
		cfg.writePagesPerCore = 2
	}
	if cfg.tombstoneAge < 0 {
		cfg.tombstoneAge = 0
	}
	if cfg.valuesFileSize < 48+cfg.maxValueSize { // header value trailer
		cfg.valuesFileSize = 48 + cfg.maxValueSize
	}
	if cfg.valuesFileSize > math.MaxUint32 {
		cfg.valuesFileSize = math.MaxUint32
	}
	if cfg.valuesFileReaders < 1 {
		cfg.valuesFileReaders = 1
	}
	return cfg
}

// OptList returns a slice with the opts given; useful if you want to possibly
// append more options to the list before using it with NewValueStore(list...).
func OptList(opts ...func(*config)) []func(*config) {
	return opts
}

// OptCores indicates how many cores may be in use (for calculating the number
// of locks to create, for example) and how many cores may be used for resizes.
// Defaults to env BRIMSTORE_CORES, or GOMAXPROCS.
func OptCores(cores int) func(*config) {
	return func(cfg *config) {
		cfg.cores = cores
	}
}

// OptBackgroundCores indicates how many cores may be used for background
// tasks. Defaults to env BRIMSTORE_BACKGROUNDCORES or 1.
func OptBackgroundCores(cores int) func(*config) {
	return func(cfg *config) {
		cfg.backgroundCores = cores
	}
}

// OptBackgroundInterval indicates the minimum number of seconds betweeen the
// starts of background tasks. For example, if set to 60 seconds and the tasks
// take 10 seconds to run, they will wait 50 seconds (with a small amount of
// randomization) between the stop of one run and the start of the next. This
// is really just meant to keep nearly empty structures from using a lot of
// resources doing nearly nothing. Normally, you'd want your tasks to be
// running constantly so that replication and cleanup are as fast as possible
// and the load constant. The default of 60 seconds is almost always fine.
// Defaults to env BRIMSTORE_BACKGROUNDINTERVAL or 60.
func OptBackgroundInterval(seconds int) func(*config) {
	return func(cfg *config) {
		cfg.backgroundInterval = seconds
	}
}

// OptMaxValueSize indicates the maximum number of bytes any given value may
// be. Defaults to env BRIMSTORE_MAXVALUESIZE or 4,194,304.
func OptMaxValueSize(bytes int) func(*config) {
	return func(cfg *config) {
		cfg.maxValueSize = bytes
	}
}

// OptPageSize controls the size of each chunk of memory allocated. Defaults to
// env BRIMSTORE_PAGESIZE or 4,194,304.
func OptPageSize(bytes int) func(*config) {
	return func(cfg *config) {
		cfg.pageSize = bytes
	}
}

// OptWritePagesPerCore controls how many pages are created per core for
// caching recently written values. Defaults to env BRIMSTORE_WRITEPAGESPERCORE
// or 3.
func OptWritePagesPerCore(number int) func(*config) {
	return func(cfg *config) {
		cfg.writePagesPerCore = number
	}
}

// OptTombstoneAge indicates how many seconds old a deletion marker may be
// before it is permanently removed. Defaults to env BRIMSTORE_TOMBSTONEAGE or
// 14,400 (4 hours).
func OptTombstoneAge(seconds int) func(*config) {
	return func(cfg *config) {
		cfg.tombstoneAge = seconds
	}
}

// OptValuesFileSize indicates how large a value size can be before closing it
// and opening a new one. Defaults to env BRIMSTORE_VALUESFILESIZE or
// 4,294,967,295.
func OptValuesFileSize(bytes int) func(*config) {
	return func(cfg *config) {
		cfg.valuesFileSize = bytes
	}
}

// OptValuesFileReaders indicates how many open file descriptors are allowed per
// value file for reading. Defaults to env BRIMSTORE_VALUESFILEREADERS or the
// configured number of cores.
func OptValuesFileReaders(bytes int) func(*config) {
	return func(cfg *config) {
		cfg.valuesFileReaders = bytes
	}
}

var ErrValueNotFound error = errors.New("value not found")

type ValueLocMap interface {
	Get(keyA uint64, keyB uint64) (timestamp uint64, blockID uint16, offset uint32, length uint32)
	Set(keyA uint64, keyB uint64, timestamp uint64, blockID uint16, offset uint32, length uint32, evenIfSameTimestamp bool) (previousTimestamp uint64)
	GatherStats(goroutines int, debug bool) (count uint64, length uint64, debugInfo fmt.Stringer)
	Scan(tombstoneCutoff uint64, start uint64, stop uint64)
	ScanCount(tombstoneCutoff uint64, start uint64, stop uint64, max uint64) uint64
	ScanCallback(start uint64, stop uint64, callback func(keyA uint64, keyB uint64, timestamp uint64, blockID uint16, offset uint32, length uint32) bool)
}

type ValueStore struct {
	freeableVMChan       chan *valuesMem
	freeVMChan           chan *valuesMem
	freeVWRChans         []chan *valueWriteReq
	pendingVWRChans      []chan *valueWriteReq
	vfVMChan             chan *valuesMem
	freeTOCBlockChan     chan []byte
	pendingTOCBlockChan  chan []byte
	tocWriterDoneChan    chan struct{}
	valueLocBlocks       []valueLocBlock
	valueLocBlockIDer    uint32
	vlm                  ValueLocMap
	cores                int
	backgroundCores      int
	backgroundInterval   int
	maxValueSize         uint32
	pageSize             uint32
	minValueAlloc        int
	writePagesPerCore    int
	tombstoneAge         uint64
	valuesFileSize       uint32
	valuesFileReaders    int
	checksumInterval     uint32
	backgroundNotifyChan chan *backgroundNotification
	backgroundDoneChan   chan struct{}
	backgroundIteration  uint16
}

type valueWriteReq struct {
	keyA      uint64
	keyB      uint64
	timestamp uint64
	value     []byte
	errChan   chan error
}

type valueLocBlock interface {
	timestamp() int64
	read(keyA uint64, keyB uint64, timestamp uint64, offset uint32, length uint32, value []byte) (uint64, []byte, error)
}

type valueStoreStats struct {
	debug                  bool
	freeableVMChanCap      int
	freeableVMChanIn       int
	freeVMChanCap          int
	freeVMChanIn           int
	freeVWRChans           int
	freeVWRChansCap        int
	freeVWRChansIn         int
	pendingVWRChans        int
	pendingVWRChansCap     int
	pendingVWRChansIn      int
	vfVMChanCap            int
	vfVMChanIn             int
	freeTOCBlockChanCap    int
	freeTOCBlockChanIn     int
	pendingTOCBlockChanCap int
	pendingTOCBlockChanIn  int
	maxValueLocBlockID     uint32
	cores                  int
	backgroundCores        int
	backgroundInterval     int
	maxValueSize           uint32
	pageSize               uint32
	minValueAlloc          int
	writePagesPerCore      int
	tombstoneAge           int
	valuesFileSize         uint32
	valuesFileReaders      int
	checksumInterval       uint32
	vlmCount               uint64
	vlmLength              uint64
	vlmDebugInfo           fmt.Stringer
}

type backgroundNotification struct {
	goroutines int
	doneChan   chan struct{}
}

type backgroundRun struct {
	vlm             *ValueLocMap
	goroutines      int
	funcChan        chan func()
	iteration       uint16
	tombstoneCutoff uint64
}

// NewValueStore creates a ValueStore for use in storing []byte values
// referenced by 128 bit keys.
//
// Note that a lot of buffering and multiple cores can be in use and therefore
// Close should be called prior to the process exiting to ensure all processing
// is done and the buffers are flushed.
func NewValueStore(opts ...func(*config)) *ValueStore {
	cfg := resolveConfig(opts...)
	vs := &ValueStore{
		valueLocBlocks:     make([]valueLocBlock, math.MaxUint16),
		vlm:                valuelocmap.NewValueLocMap(),
		cores:              cfg.cores,
		backgroundCores:    cfg.backgroundCores,
		backgroundInterval: cfg.backgroundInterval,
		maxValueSize:       uint32(cfg.maxValueSize),
		checksumInterval:   uint32(cfg.checksumInterval),
		pageSize:           uint32(cfg.pageSize),
		minValueAlloc:      cfg.minValueAlloc,
		writePagesPerCore:  cfg.writePagesPerCore,
		tombstoneAge:       uint64(cfg.tombstoneAge) * uint64(time.Second),
		valuesFileSize:     uint32(cfg.valuesFileSize),
		valuesFileReaders:  cfg.valuesFileReaders,
	}
	vs.freeableVMChan = make(chan *valuesMem, vs.cores*vs.writePagesPerCore)
	vs.freeVMChan = make(chan *valuesMem, vs.cores)
	vs.freeVWRChans = make([]chan *valueWriteReq, vs.cores)
	vs.pendingVWRChans = make([]chan *valueWriteReq, vs.cores)
	vs.vfVMChan = make(chan *valuesMem, vs.cores)
	vs.freeTOCBlockChan = make(chan []byte, vs.cores*2)
	vs.pendingTOCBlockChan = make(chan []byte, vs.cores)
	vs.tocWriterDoneChan = make(chan struct{}, 1)
	for i := 0; i < cap(vs.freeableVMChan); i++ {
		vm := &valuesMem{
			vs:     vs,
			toc:    make([]byte, 0, vs.pageSize),
			values: make([]byte, 0, vs.pageSize),
		}
		vm.id = vs.addValueLocBlock(vm)
		vs.freeableVMChan <- vm
	}
	for i := 0; i < len(vs.freeVWRChans); i++ {
		vs.freeVWRChans[i] = make(chan *valueWriteReq, vs.cores*2)
		for j := 0; j < vs.cores*2; j++ {
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
	for i := 0; i < vs.cores; i++ {
		go vs.memClearer()
	}
	for i := 0; i < len(vs.pendingVWRChans); i++ {
		go vs.memWriter(vs.pendingVWRChans[i])
	}
	vs.recovery()
	return vs
}

// MaxValueSize returns the maximum length of a value the ValueStore can
// accept.
func (vs *ValueStore) MaxValueSize() uint32 {
	return vs.maxValueSize
}

// Close shuts down all background processing and the ValueStore will refuse
// any additional writes; reads may still occur.
func (vs *ValueStore) Close() {
	for _, c := range vs.pendingVWRChans {
		c <- nil
	}
	for i := 0; i < cap(vs.freeableVMChan); i++ {
		<-vs.freeVMChan
	}
	<-vs.tocWriterDoneChan
	vs.BackgroundStop()
}

// Lookup will return timestamp, length, err for keyA, keyB.
//
// Note that err == ErrValueNotFound with timestamp == 0 indicates keyA, keyB
// was not known at all whereas err == ErrValueNotFound with timestamp != 0
// (also timestamp & 1 == 1) indicates keyA, keyB was known and had a deletion
// marker (aka tombstone).
//
// This may be called even after Close.
func (vs *ValueStore) Lookup(keyA uint64, keyB uint64) (uint64, uint32, error) {
	timestamp, id, _, length := vs.vlm.Get(keyA, keyB)
	if id == 0 || timestamp&1 == 1 {
		return timestamp, 0, ErrValueNotFound
	}
	return timestamp, length, nil
}

// Read will return timestamp, value, err for keyA, keyB; if an incoming value
// is provided, the read value will be appended to it and the whole returned
// (useful to reuse an existing []byte).
//
// Note that err == ErrValueNotFound with timestamp == 0 indicates keyA, keyB
// was not known at all whereas err == ErrValueNotFound with timestamp != 0
// (also timestamp & 1 == 1) indicates keyA, keyB was known and had a deletion
// marker (aka tombstone).
//
// This may be called even after Close.
func (vs *ValueStore) Read(keyA uint64, keyB uint64, value []byte) (uint64, []byte, error) {
	timestamp, id, offset, length := vs.vlm.Get(keyA, keyB)
	if id == 0 || timestamp&1 == 1 {
		return timestamp, value, ErrValueNotFound
	}
	return vs.valueLocBlock(id).read(keyA, keyB, timestamp, offset, length, value)
}

// Write stores timestamp & 0xfffffffffffffffe (lowest bit zeroed), value for
// keyA, keyB or returns any error; a newer timestamp already in place is not
// reported as an error.
//
// This may no longer be called after Close.
func (vs *ValueStore) Write(keyA uint64, keyB uint64, timestamp uint64, value []byte) (uint64, error) {
	i := int(keyA>>1) % len(vs.freeVWRChans)
	vwr := <-vs.freeVWRChans[i]
	vwr.keyA = keyA
	vwr.keyB = keyB
	vwr.timestamp = timestamp & 0xfffffffffffffffe
	vwr.value = value
	vs.pendingVWRChans[i] <- vwr
	err := <-vwr.errChan
	previousTimestamp := vwr.timestamp
	vwr.value = nil
	vs.freeVWRChans[i] <- vwr
	return previousTimestamp, err
}

// Delete stores timestamp | 1 for keyA, keyB or returns any error; a newer
// timestamp already in place is not reported as an error.
//
// This may no longer be called after Close.
func (vs *ValueStore) Delete(keyA uint64, keyB uint64, timestamp uint64) (uint64, error) {
	i := int(keyA>>1) % len(vs.freeVWRChans)
	vwr := <-vs.freeVWRChans[i]
	vwr.keyA = keyA
	vwr.keyB = keyB
	vwr.timestamp = timestamp | 1
	vwr.value = nil
	vs.pendingVWRChans[i] <- vwr
	err := <-vwr.errChan
	previousTimestamp := vwr.timestamp
	vs.freeVWRChans[i] <- vwr
	return previousTimestamp, err
}

// GatherStats returns overall information about the state of the ValueStore.
//
// This may be called even after Close.
func (vs *ValueStore) GatherStats(goroutines int, debug bool) (count uint64, length uint64, debugInfo fmt.Stringer) {
	stats := &valueStoreStats{}
	if debug {
		stats.debug = debug
		stats.freeableVMChanCap = cap(vs.freeableVMChan)
		stats.freeableVMChanIn = len(vs.freeableVMChan)
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
		stats.maxValueLocBlockID = atomic.LoadUint32(&vs.valueLocBlockIDer)
		stats.cores = vs.cores
		stats.backgroundCores = vs.backgroundCores
		stats.backgroundInterval = vs.backgroundInterval
		stats.maxValueSize = vs.maxValueSize
		stats.pageSize = vs.pageSize
		stats.minValueAlloc = vs.minValueAlloc
		stats.writePagesPerCore = vs.writePagesPerCore
		stats.tombstoneAge = int(vs.tombstoneAge / uint64(time.Second))
		stats.valuesFileSize = vs.valuesFileSize
		stats.valuesFileReaders = vs.valuesFileReaders
		stats.checksumInterval = vs.checksumInterval
		stats.vlmCount, stats.vlmLength, stats.vlmDebugInfo = vs.vlm.GatherStats(goroutines, true)
	} else {
		stats.vlmCount, stats.vlmLength, stats.vlmDebugInfo = vs.vlm.GatherStats(goroutines, false)
	}
	return stats.vlmCount, stats.vlmLength, stats
}

func (vs *ValueStore) valueLocBlock(valueLocBlockID uint16) valueLocBlock {
	return vs.valueLocBlocks[valueLocBlockID]
}

func (vs *ValueStore) addValueLocBlock(block valueLocBlock) uint16 {
	id := atomic.AddUint32(&vs.valueLocBlockIDer, 1)
	if id >= 65536 {
		panic("too many valueLocBlocks")
	}
	vs.valueLocBlocks[id] = block
	return uint16(id)
}

func (vs *ValueStore) memClearer() {
	var tb []byte
	var tbTS int64
	var tbOffset int
	for {
		vm := <-vs.freeableVMChan
		if vm == nil {
			if tb != nil {
				vs.pendingTOCBlockChan <- tb
			}
			vs.pendingTOCBlockChan <- nil
			break
		}
		vf := vs.valueLocBlock(vm.vfID)
		if tb != nil && tbTS != vf.timestamp() {
			vs.pendingTOCBlockChan <- tb
			tb = nil
		}
		for vmTOCOffset := 0; vmTOCOffset < len(vm.toc); vmTOCOffset += 32 {
			keyA := binary.BigEndian.Uint64(vm.toc[vmTOCOffset:])
			keyB := binary.BigEndian.Uint64(vm.toc[vmTOCOffset+8:])
			timestamp := binary.BigEndian.Uint64(vm.toc[vmTOCOffset+16:])
			vmMemOffset := binary.BigEndian.Uint32(vm.toc[vmTOCOffset+24:])
			length := binary.BigEndian.Uint32(vm.toc[vmTOCOffset+28:])
			previousTimestamp := vs.vlm.Set(keyA, keyB, timestamp, vm.vfID, vm.vfOffset+vmMemOffset, length, true)
			if previousTimestamp != timestamp {
				continue
			}
			if tb != nil && tbOffset+32 > cap(tb) {
				vs.pendingTOCBlockChan <- tb
				tb = nil
			}
			if tb == nil {
				tb = <-vs.freeTOCBlockChan
				tbTS = vf.timestamp()
				tb = tb[:8]
				binary.BigEndian.PutUint64(tb, uint64(tbTS))
				tbOffset = 8
			}
			tb = tb[:tbOffset+32]
			binary.BigEndian.PutUint64(tb[tbOffset:], keyA)
			binary.BigEndian.PutUint64(tb[tbOffset+8:], keyB)
			binary.BigEndian.PutUint64(tb[tbOffset+16:], timestamp)
			binary.BigEndian.PutUint32(tb[tbOffset+24:], vm.vfOffset+vmMemOffset)
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

func (vs *ValueStore) memWriter(pendingVWRChan chan *valueWriteReq) {
	var vm *valuesMem
	var vmTOCOffset int
	var vmMemOffset int
	for {
		vwr := <-pendingVWRChan
		if vwr == nil {
			if vm != nil && len(vm.toc) > 0 {
				vs.vfVMChan <- vm
			}
			vs.vfVMChan <- nil
			break
		}
		length := len(vwr.value)
		if length > int(vs.maxValueSize) {
			vwr.errChan <- fmt.Errorf("value length of %d > %d", length, vs.maxValueSize)
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
		previousTimestamp := vs.vlm.Set(vwr.keyA, vwr.keyB, vwr.timestamp, vm.id, uint32(vmMemOffset), uint32(length), false)
		if previousTimestamp < vwr.timestamp {
			vm.toc = vm.toc[:vmTOCOffset+32]
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset:], vwr.keyA)
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+8:], vwr.keyB)
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+16:], vwr.timestamp)
			binary.BigEndian.PutUint32(vm.toc[vmTOCOffset+24:], uint32(vmMemOffset))
			binary.BigEndian.PutUint32(vm.toc[vmTOCOffset+28:], uint32(length))
			vmTOCOffset += 32
			vmMemOffset += alloc
		} else {
			vm.discardLock.Lock()
			vm.values = vm.values[:vmMemOffset]
			vm.discardLock.Unlock()
		}
		vwr.timestamp = previousTimestamp
		vwr.errChan <- nil
	}
}

func (vs *ValueStore) vfWriter() {
	var vf *valuesFile
	memWritersLeft := vs.cores
	var tocLen uint64
	var valueLen uint64
	for {
		vm := <-vs.vfVMChan
		if vm == nil {
			memWritersLeft--
			if memWritersLeft < 1 {
				if vf != nil {
					vf.close()
				}
				for i := 0; i < vs.cores; i++ {
					vs.freeableVMChan <- nil
				}
				break
			}
			continue
		}
		if vf != nil && (tocLen+uint64(len(vm.toc)) >= uint64(vs.valuesFileSize) || valueLen+uint64(len(vm.values)) > uint64(vs.valuesFileSize)) {
			vf.close()
			vf = nil
		}
		if vf == nil {
			vf = createValuesFile(vs)
			tocLen = 32
			valueLen = 32
		}
		vf.write(vm)
		tocLen += uint64(len(vm.toc))
		valueLen += uint64(len(vm.values))
	}
}

func (vs *ValueStore) tocWriter() {
	var btsA uint64
	var writerA io.WriteCloser
	var offsetA uint64
	var btsB uint64
	var writerB io.WriteCloser
	var offsetB uint64
	head := []byte("BRIMSTORE VALUETOC v0           ")
	binary.BigEndian.PutUint32(head[28:], uint32(vs.checksumInterval))
	term := make([]byte, 16)
	copy(term[12:], "TERM")
	memClearersLeft := vs.cores
	for {
		t := <-vs.pendingTOCBlockChan
		if t == nil {
			memClearersLeft--
			if memClearersLeft < 1 {
				if writerB != nil {
					binary.BigEndian.PutUint64(term[4:], offsetB)
					if _, err := writerB.Write(term); err != nil {
						panic(err)
					}
					if err := writerB.Close(); err != nil {
						panic(err)
					}
				}
				if writerA != nil {
					binary.BigEndian.PutUint64(term[4:], offsetA)
					if _, err := writerA.Write(term); err != nil {
						panic(err)
					}
					if err := writerA.Close(); err != nil {
						panic(err)
					}
				}
				break
			}
			continue
		}
		if len(t) > 8 {
			bts := binary.BigEndian.Uint64(t)
			switch bts {
			case btsA:
				if _, err := writerA.Write(t[8:]); err != nil {
					panic(err)
				}
				offsetA += uint64(len(t) - 8)
			case btsB:
				if _, err := writerB.Write(t[8:]); err != nil {
					panic(err)
				}
				offsetB += uint64(len(t) - 8)
			default:
				// An assumption is made here: If the timestamp for this toc
				// block doesn't match the last two seen timestamps then we
				// expect no more toc blocks for the oldest timestamp and can
				// close that toc file.
				if writerB != nil {
					binary.BigEndian.PutUint64(term[4:], offsetB)
					if _, err := writerB.Write(term); err != nil {
						panic(err)
					}
					if err := writerB.Close(); err != nil {
						panic(err)
					}
				}
				btsB = btsA
				writerB = writerA
				offsetB = offsetA
				btsA = bts
				fp, err := os.Create(fmt.Sprintf("%d.valuestoc", bts))
				if err != nil {
					panic(err)
				}
				writerA = brimutil.NewMultiCoreChecksummedWriter(fp, int(vs.checksumInterval), murmur3.New32, vs.cores)
				if _, err := writerA.Write(head); err != nil {
					panic(err)
				}
				if _, err := writerA.Write(t[8:]); err != nil {
					panic(err)
				}
				offsetA = 32 + uint64(len(t)-8)
			}
		}
		vs.freeTOCBlockChan <- t[:0]
	}
	vs.tocWriterDoneChan <- struct{}{}
}

func (vs *ValueStore) recovery() {
	start := time.Now()
	dfp, err := os.Open(".")
	if err != nil {
		panic(err)
	}
	names, err := dfp.Readdirnames(-1)
	if err != nil {
		panic(err)
	}
	sort.Strings(names)
	fromDiskCount := 0
	count := int64(0)
	type writeReq struct {
		keyA      uint64
		keyB      uint64
		timestamp uint64
		blockID   uint16
		offset    uint32
		length    uint32
	}
	pendingChan := make(chan []writeReq, vs.cores)
	freeChan := make(chan []writeReq, cap(pendingChan)*2)
	for i := 0; i < cap(freeChan); i++ {
		freeChan <- make([]writeReq, 0, 65536)
	}
	wg := &sync.WaitGroup{}
	wg.Add(cap(pendingChan))
	for i := 0; i < cap(pendingChan); i++ {
		go func() {
			for {
				wrs := <-pendingChan
				if wrs == nil {
					break
				}
				for i := len(wrs) - 1; i >= 0; i-- {
					wr := &wrs[i]
					if vs.vlm.Set(wr.keyA, wr.keyB, wr.timestamp, wr.blockID, wr.offset, wr.length, false) < wr.timestamp {
						atomic.AddInt64(&count, 1)
					}
				}
				freeChan <- wrs[:0]
			}
			wg.Done()
		}()
	}
	wrs := <-freeChan
	wix := 0
	maxwix := cap(wrs) - 1
	wrs = wrs[:maxwix+1]
	for i := len(names) - 1; i >= 0; i-- {
		if !strings.HasSuffix(names[i], ".valuestoc") {
			continue
		}
		bts := int64(0)
		if bts, err = strconv.ParseInt(names[i][:len(names[i])-len(".valuestoc")], 10, 64); err != nil {
			log.Printf("bad timestamp name: %#v\n", names[i])
			continue
		}
		if bts == 0 {
			log.Printf("bad timestamp name: %#v\n", names[i])
			continue
		}
		vf := newValuesFile(vs, bts)
		fp, err := os.Open(names[i])
		if err != nil {
			log.Printf("error opening %s: %s\n", names[i], err)
			continue
		}
		buf := make([]byte, vs.checksumInterval+4)
		checksumFailures := 0
		overflow := make([]byte, 0, 32)
		first := true
		terminated := false
		for {
			n, err := io.ReadFull(fp, buf)
			if n < 4 {
				if err != io.EOF && err != io.ErrUnexpectedEOF {
					log.Printf("error reading %s: %s\n", names[i], err)
				}
				break
			}
			n -= 4
			if murmur3.Sum32(buf[:n]) != binary.BigEndian.Uint32(buf[n:]) {
				checksumFailures++
			} else {
				i := 0
				if first {
					if !bytes.Equal(buf[:28], []byte("BRIMSTORE VALUETOC v0       ")) {
						log.Printf("bad header: %s\n", names[i])
						break
					}
					if binary.BigEndian.Uint32(buf[28:]) != vs.checksumInterval {
						log.Printf("bad header checksum interval: %s\n", names[i])
						break
					}
					i += 32
					first = false
				}
				if n < int(vs.checksumInterval) {
					if binary.BigEndian.Uint32(buf[n-16:]) != 0 {
						log.Printf("bad terminator size marker: %s\n", names[i])
						break
					}
					if !bytes.Equal(buf[n-4:n], []byte("TERM")) {
						log.Printf("bad terminator: %s\n", names[i])
						break
					}
					n -= 16
					terminated = true
				}
				if len(overflow) > 0 {
					i += 32 - len(overflow)
					overflow = append(overflow, buf[i-32+len(overflow):i]...)
					if wrs == nil {
						wrs = (<-freeChan)[:maxwix+1]
						wix = 0
					}
					wr := &wrs[wix]
					wr.keyA = binary.BigEndian.Uint64(overflow)
					wr.keyB = binary.BigEndian.Uint64(overflow[8:])
					wr.timestamp = binary.BigEndian.Uint64(overflow[16:])
					wr.blockID = vf.id
					wr.offset = binary.BigEndian.Uint32(overflow[24:])
					wr.length = binary.BigEndian.Uint32(overflow[28:])
					wix++
					if wix > maxwix {
						pendingChan <- wrs
						wrs = nil
					}
					fromDiskCount++
					overflow = overflow[:0]
				}
				for ; i+32 <= n; i += 32 {
					if wrs == nil {
						wrs = (<-freeChan)[:maxwix+1]
						wix = 0
					}
					wr := &wrs[wix]
					wr.keyA = binary.BigEndian.Uint64(buf[i:])
					wr.keyB = binary.BigEndian.Uint64(buf[i+8:])
					wr.timestamp = binary.BigEndian.Uint64(buf[i+16:])
					wr.blockID = vf.id
					wr.offset = binary.BigEndian.Uint32(buf[i+24:])
					wr.length = binary.BigEndian.Uint32(buf[i+28:])
					wix++
					if wix > maxwix {
						pendingChan <- wrs
						wrs = nil
					}
					fromDiskCount++
				}
				if i != n {
					overflow = overflow[:n-i]
					copy(overflow, buf[i:])
				}
			}
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				log.Printf("error reading %s: %s\n", names[i], err)
				break
			}
		}
		fp.Close()
		if !terminated {
			log.Printf("early end of file: %s\n", names[i])
		}
		if checksumFailures > 0 {
			log.Printf("%d checksum failures for %s\n", checksumFailures, names[i])
		}
	}
	if wix > 0 {
		pendingChan <- wrs[:wix]
	}
	for i := 0; i < cap(pendingChan); i++ {
		pendingChan <- nil
	}
	wg.Wait()
	if fromDiskCount > 0 {
		dur := time.Now().Sub(start)
		valueCount, valueLength, _ := vs.GatherStats(vs.cores, false)
		log.Printf("%d key locations loaded in %s, %.0f/s; %d caused change; %d resulting locations referencing %d bytes.\n", fromDiskCount, dur, float64(fromDiskCount)/(float64(dur)/float64(time.Second)), count, valueCount, valueLength)
	}
}

func (stats *valueStoreStats) String() string {
	if stats.debug {
		return brimtext.Align([][]string{
			[]string{"freeableVMChanCap", fmt.Sprintf("%d", stats.freeableVMChanCap)},
			[]string{"freeableVMChanIn", fmt.Sprintf("%d", stats.freeableVMChanIn)},
			[]string{"freeVMChanCap", fmt.Sprintf("%d", stats.freeVMChanCap)},
			[]string{"freeVMChanIn", fmt.Sprintf("%d", stats.freeVMChanIn)},
			[]string{"freeVWRChans", fmt.Sprintf("%d", stats.freeVWRChans)},
			[]string{"freeVWRChansCap", fmt.Sprintf("%d", stats.freeVWRChansCap)},
			[]string{"freeVWRChansIn", fmt.Sprintf("%d", stats.freeVWRChansIn)},
			[]string{"pendingVWRChans", fmt.Sprintf("%d", stats.pendingVWRChans)},
			[]string{"pendingVWRChansCap", fmt.Sprintf("%d", stats.pendingVWRChansCap)},
			[]string{"pendingVWRChansIn", fmt.Sprintf("%d", stats.pendingVWRChansIn)},
			[]string{"vfVMChanCap", fmt.Sprintf("%d", stats.vfVMChanCap)},
			[]string{"vfVMChanIn", fmt.Sprintf("%d", stats.vfVMChanIn)},
			[]string{"freeTOCBlockChanCap", fmt.Sprintf("%d", stats.freeTOCBlockChanCap)},
			[]string{"freeTOCBlockChanIn", fmt.Sprintf("%d", stats.freeTOCBlockChanIn)},
			[]string{"pendingTOCBlockChanCap", fmt.Sprintf("%d", stats.pendingTOCBlockChanCap)},
			[]string{"pendingTOCBlockChanIn", fmt.Sprintf("%d", stats.pendingTOCBlockChanIn)},
			[]string{"maxValueLocBlockID", fmt.Sprintf("%d", stats.maxValueLocBlockID)},
			[]string{"cores", fmt.Sprintf("%d", stats.cores)},
			[]string{"backgroundCores", fmt.Sprintf("%d", stats.backgroundCores)},
			[]string{"backgroundInterval", fmt.Sprintf("%d", stats.backgroundInterval)},
			[]string{"maxValueSize", fmt.Sprintf("%d", stats.maxValueSize)},
			[]string{"checksumInterval", fmt.Sprintf("%d", stats.checksumInterval)},
			[]string{"pageSize", fmt.Sprintf("%d", stats.pageSize)},
			[]string{"minValueAlloc", fmt.Sprintf("%d", stats.minValueAlloc)},
			[]string{"writePagesPerCore", fmt.Sprintf("%d", stats.writePagesPerCore)},
			[]string{"tombstoneAge", fmt.Sprintf("%d", stats.tombstoneAge)},
			[]string{"valuesFileSize", fmt.Sprintf("%d", stats.valuesFileSize)},
			[]string{"valuesFileReaders", fmt.Sprintf("%d", stats.valuesFileReaders)},
			[]string{"vlmCount", fmt.Sprintf("%d", stats.vlmCount)},
			[]string{"vlmLength", fmt.Sprintf("%d", stats.vlmLength)},
			[]string{"vlmDebugInfo", stats.vlmDebugInfo.String()},
		}, nil)
	} else {
		return brimtext.Align([][]string{
			[]string{"vlmCount", fmt.Sprintf("%d", stats.vlmCount)},
			[]string{"vlmLength", fmt.Sprintf("%d", stats.vlmLength)},
		}, nil)
	}
}

// BackgroundStart will start the execution of background tasks at configured
// intervals. Multiple calls to BackgroundStart will cause no harm and tasks
// can be temporarily suspended by calling BackgroundStop and BackgroundStart
// as desired.
func (vs *ValueStore) BackgroundStart() {
	if vs.backgroundNotifyChan == nil {
		vs.backgroundDoneChan = make(chan struct{}, 1)
		vs.backgroundNotifyChan = make(chan *backgroundNotification, 1)
		go vs.backgroundLauncher()
	}
}

// BackgroundStop will stop the execution of background tasks; use
// BackgroundStart to restart them if desired. This can be useful when
// importing large amounts of data where temporarily stopping the background
// tasks and just restarting them after the import can greatly increase the
// import speed.
func (vs *ValueStore) BackgroundStop() {
	if vs.backgroundNotifyChan != nil {
		vs.backgroundNotifyChan <- nil
		<-vs.backgroundDoneChan
		vs.backgroundDoneChan = nil
		vs.backgroundNotifyChan = nil
	}
}

// BackgroundNow will immediately (rather than waiting for the next background
// interval) execute any background tasks using up to the number of goroutines
// indicated (0 will use the configured OptBackgroundCores value). This
// function will not return until all background tasks have completed at least
// one pass.
func (vs *ValueStore) BackgroundNow(goroutines int) {
	if vs.backgroundNotifyChan == nil {
		vs.background(goroutines)
	} else {
		c := make(chan struct{}, 1)
		vs.backgroundNotifyChan <- &backgroundNotification{goroutines, c}
		<-c
	}
}

func (vs *ValueStore) backgroundLauncher() {
	interval := float64(vs.backgroundInterval) * float64(time.Second)
	nextRun := time.Now().Add(time.Duration(interval + interval*rand.NormFloat64()*0.1))
WORK:
	for {
		var notification *backgroundNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-vs.backgroundNotifyChan:
				if notification == nil {
					break WORK
				}
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-vs.backgroundNotifyChan:
				if notification == nil {
					break WORK
				}
			default:
			}
		}
		nextRun = time.Now().Add(time.Duration(interval + interval*rand.NormFloat64()*0.1))
		goroutines := vs.backgroundCores
		if notification != nil && notification.goroutines > 0 {
			goroutines = notification.goroutines
		}
		vs.background(goroutines)
		if notification != nil && notification.doneChan != nil {
			notification.doneChan <- struct{}{}
		}
	}
	vs.backgroundDoneChan <- struct{}{}
}

func (vs *ValueStore) background(goroutines int) {
	// TODO: Disabled for the moment
	if goroutines == 0 || goroutines != 0 {
		return
	}
	begin := time.Now()
	if goroutines < 1 {
		goroutines = vs.backgroundCores
	}
	if vs.backgroundIteration == math.MaxUint16 {
		vs.backgroundIteration = 0
	} else {
		vs.backgroundIteration++
	}
	bg := &backgroundRun{
		goroutines:      goroutines,
		funcChan:        make(chan func(), goroutines),
		iteration:       vs.backgroundIteration,
		tombstoneCutoff: uint64(time.Now().UnixNano()) - vs.tombstoneAge,
	}
	funcsDone := make(chan struct{}, 1)
	go func() {
		wg := &sync.WaitGroup{}
		for {
			f := <-bg.funcChan
			if f == nil {
				break
			}
			wg.Add(1)
			go func() {
				f()
				wg.Done()
			}()
		}
		wg.Wait()
		funcsDone <- struct{}{}
	}()
	p := 0
	ppower := 10
	pincrement := uint64(1) << uint64(64-ppower)
	pstart := uint64(0)
	pstop := pstart + (pincrement - 1)
	for {
		// Here I'm doing bloom filter scans for every partition when
		// eventually it should just do filters for partitions we're in the
		// ring for. Partitions we're not in the ring for (handoffs, old
		// data from ring changes, etc.) we should just send out what data
		// we have and the remove it locally.
		bg.funcChan <- func(p int, pstart uint64, pstop uint64) func() {
			return func() {
				vs.bloom(bg, p, pstart, pstop)
			}
		}(p, pstart, pstop)
		if pstop == math.MaxUint64 {
			break
		}
		p++
		pstart += pincrement
		pstop += pincrement
		if pstop == math.MaxUint64 {
			break
		}
	}
	bg.funcChan <- nil
	<-funcsDone
	fmt.Println(time.Now().Sub(begin), "to run background tasks, iteration", bg.iteration)
}

const _GLH_BLOOM_FILTER_N = 1000000
const _GLH_BLOOM_FILTER_P = 0.001

func (vs *ValueStore) bloom(bg *backgroundRun, p int, pstart uint64, pstop uint64) {
	count := vs.vlm.ScanCount(bg.tombstoneCutoff, pstart, pstop, _GLH_BLOOM_FILTER_N)
	for count > _GLH_BLOOM_FILTER_N {
		pstopNew := pstart + (pstop-pstart+1)/2 - 1
		go func(pstart uint64) {
			bg.funcChan <- func() {
				vs.bloom(bg, p, pstart, pstop)
			}
		}(pstopNew + 1)
		pstop = pstopNew
		count = vs.vlm.ScanCount(bg.tombstoneCutoff, pstart, pstop, _GLH_BLOOM_FILTER_N)
	}
	if count > 0 {
		ktbf := ktbloomfilter.NewKTBloomFilter(_GLH_BLOOM_FILTER_N, _GLH_BLOOM_FILTER_P, bg.iteration)
		vs.vlm.ScanCallback(
			pstart,
			pstop,
			func(keyA uint64, keyB uint64, timestamp uint64, blockID uint16, offset uint32, length uint32) bool {
				ktbf.Add(keyA, keyB, timestamp)
				return true
			},
		)
		if ktbf.HasData {
			// TODO
		}
	}
}
