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

	"github.com/gholt/brimtext"
	"github.com/gholt/brimutil"
	"github.com/spaolacci/murmur3"
)

var ErrValueNotFound error = errors.New("value not found")

// ValuesStoreOpts allows configuration of the ValuesStore, although normally
// the defaults are best.
//
// Note that due to implementation changes, many of these options may be
// deprecated in the future. Cores and MaxValueSize are the two options that
// are likely to always be supported.
type ValuesStoreOpts struct {
	// Cores controls the number of goroutines spawned to process data. The
	// actual number of CPU cores used can only truly be controlled by
	// GOMAXPROCS, this will just indicate how many goroutines can spawned for
	// a given task. For example, if GOMAXPROCS is 8 and Cores is 1, 8 cores
	// will likely still be in use as different tasks may each launch 1
	// goroutine.
	Cores int
	// MaxValueSize indicates the maximum length for values stored. Attempting
	// to store a value beyond this cap will result in an error returned.
	MaxValueSize int
	// TombstoneAge is the number of seconds to keep tombstones (deletion
	// markers).
	TombstoneAge int
	// MemTOCPageSize controls TOC buffer memory allocation. A TOC is a Table
	// Of Contents which has metadata about values stored. A TOC Page is a
	// block of memory where this metadata is buffered before being written to
	// disk. The implementation at the time of this writing will allocate
	// Cores*4 TOC Pages.
	MemTOCPageSize int
	// MemValuesPageSize controls value buffer memory allocation. A Values Page
	// is a block of memory where actual value content is buffered before being
	// written to disk. The implementation at the time of this writing will
	// allocate Cores*2 Values Pages.
	MemValuesPageSize int
	// ValuesLocMapPageSize controls the size of each chunk of memory allocated
	// by for value locations. The Values Loc Map is a map from key to value
	// location data (memory block and offset, disk file and offset) and other
	// metadata (timestamp, length, etc.). A Value Loc Map Page is a block of
	// memory where sections of location data is stored. The blocks are
	// arranged by the high bits of the keys and distributed within each block
	// by the low bits of the keys. The high bit coalescing is to speed
	// replication and other routines that work with ranges of keys and the low
	// bit distribution is to make even use of memory. This page size setting
	// indicates how large each memory block will be, but the number of memory
	// blocks depends on the key count. At the time of this writing, each key
	// requires 42 bytes of location data.
	ValuesLocMapPageSize int
	// ValuesLocMapSplitMultiplier indicates how full a Values Loc Map Page can
	// get before being split into two pages. Each page is a map (hash table),
	// so this indicates the load factor before splitting. For example, 1.0
	// would split once a page had as many items in it as its
	// ValuesLocMapPageSize would indicate. When a page is "overfull", each
	// item will, on average, point to another individually allocated item
	// (think of a slice of linked lists, each list having two items). Setting
	// this lower can decrease the individual allocations and pointer
	// traversing but at the expense of splitting and copying more often.
	ValuesLocMapSplitMultiplier float64
	// ValuesFileSize indicates the size of a values file on disk before
	// closing it and opening a new file. This also caps the size of the TOC
	// files (Table of Contents of what is contained within a values file), but
	// usually the TOC files are smaller than the values files unless the
	// average value size is very small or there are a lot of deletes.
	ValuesFileSize int
	// ValuesFileReaders controls how many times an individual values file is
	// opened for reading. A higher number will allow more concurrent reads,
	// but at the expense of open file descriptors.
	ValuesFileReaders int
	// ChecksumInterval controls how many on-disk bytes will be written before
	// emitting a 32-bit checksum for those bytes. Both TOC files and values
	// files have checksums at this interval. For example, if set to 65532 then
	// every 65532 bytes written and additional 4 byte checksum will be written
	// for those bytes, giving 65536 "chunks" on disk. This also controls the
	// block size written to disk at once since bytes are buffered to the
	// checksum interval before be checksummed and then written to disk.
	ChecksumInterval int
}

func NewValuesStoreOpts(envPrefix string) *ValuesStoreOpts {
	if envPrefix == "" {
		envPrefix = "BRIMSTORE_VALUESSTORE_"
	}
	opts := &ValuesStoreOpts{}
	if env := os.Getenv(envPrefix + "CORES"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.Cores = val
		}
	}
	if opts.Cores <= 0 {
		opts.Cores = runtime.GOMAXPROCS(0)
	}
	if env := os.Getenv(envPrefix + "MAX_VALUE_SIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.MaxValueSize = val
		}
	}
	if opts.MaxValueSize <= 0 {
		opts.MaxValueSize = 4 * 1024 * 1024
	}
	if env := os.Getenv(envPrefix + "TOMBSTONE_AGE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.TombstoneAge = val
		}
	}
	if opts.TombstoneAge <= 0 {
		opts.TombstoneAge = 4 * 60 * 60
	}
	if env := os.Getenv(envPrefix + "MEM_TOC_PAGE_SIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.MemTOCPageSize = val
		}
	}
	if opts.MemTOCPageSize <= 0 {
		opts.MemTOCPageSize = 8 * 1024 * 1024
	}
	if env := os.Getenv(envPrefix + "MEM_VALUES_PAGE_SIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.MemValuesPageSize = val
		}
	}
	if opts.MemValuesPageSize <= 0 {
		opts.MemValuesPageSize = 1 << brimutil.PowerOfTwoNeeded(uint64(opts.MaxValueSize+4))
		if opts.MemValuesPageSize < 8*1024*1024 {
			opts.MemValuesPageSize = 8 * 1024 * 1024
		}
	}
	if env := os.Getenv(envPrefix + "VALUES_LOC_MAP_PAGE_SIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.ValuesLocMapPageSize = val
		}
	}
	if opts.ValuesLocMapPageSize <= 0 {
		opts.ValuesLocMapPageSize = 8 * 1024 * 1024
	}
	if env := os.Getenv(envPrefix + "VALUES_LOC_MAP_SPLIT_MULTIPLIER"); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil {
			opts.ValuesLocMapSplitMultiplier = val
		}
	}
	if opts.ValuesLocMapSplitMultiplier <= 0 {
		opts.ValuesLocMapSplitMultiplier = 3.0
	}
	if env := os.Getenv(envPrefix + "VALUES_FILE_SIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.ValuesFileSize = val
		}
	}
	if opts.ValuesFileSize <= 0 {
		opts.ValuesFileSize = math.MaxUint32
	}
	if env := os.Getenv(envPrefix + "VALUES_FILE_READERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.ValuesFileReaders = val
		}
	}
	if opts.ValuesFileReaders <= 0 {
		opts.ValuesFileReaders = opts.Cores
	}
	if env := os.Getenv(envPrefix + "CHECKSUM_INTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.ChecksumInterval = val
		}
	}
	if opts.ChecksumInterval <= 0 {
		opts.ChecksumInterval = 65532
	}
	return opts
}

// ValuesStore: See NewValuesStore.
type ValuesStore struct {
	freeableVMChan        chan *valuesMem
	freeVMChan            chan *valuesMem
	freeVWRChans          []chan *valueWriteReq
	pendingVWRChans       []chan *valueWriteReq
	vfVMChan              chan *valuesMem
	freeTOCBlockChan      chan []byte
	pendingTOCBlockChan   chan []byte
	tocWriterDoneChan     chan struct{}
	backgroundNotifyChan  chan chan struct{}
	backgroundDoneChan    chan struct{}
	valuesLocBlocks       []valuesLocBlock
	atValuesLocBlocksIDer uint32
	vlm                   *valuesLocMap
	cores                 int
	maxValueSize          uint32
	tombstoneAge          uint64
	memTOCPageSize        uint32
	memValuesPageSize     uint32
	valuesFileSize        uint32
	valuesFileReaders     int
	checksumInterval      uint32
}

// NewValuesStore creates a ValuesStore for use in storing []byte values
// referenced by 128 bit keys; opts may be nil to use the defaults.
//
// Note that a lot of buffering and multiple cores can be in use and therefore
// Close should be called prior to the process exiting to ensure all processing
// is done and the buffers are flushed.
func NewValuesStore(opts *ValuesStoreOpts) *ValuesStore {
	if opts == nil {
		opts = NewValuesStoreOpts("")
	}
	cores := opts.Cores
	if cores < 1 {
		cores = 1
	}
	maxValueSize := opts.MaxValueSize
	if maxValueSize < 0 {
		maxValueSize = 0
	}
	if maxValueSize > math.MaxUint32 {
		maxValueSize = math.MaxUint32
	}
	tombstoneAge := uint64(opts.TombstoneAge)
	if tombstoneAge < 1 {
		tombstoneAge = 1
	}
	tombstoneAge *= uint64(time.Second)
	memTOCPageSize := opts.MemTOCPageSize
	if memTOCPageSize < 4096 {
		memTOCPageSize = 4096
	}
	memValuesPageSize := opts.MemValuesPageSize
	if memValuesPageSize < 4096 {
		memValuesPageSize = 4096
	}
	valuesFileSize := opts.ValuesFileSize
	if valuesFileSize <= 0 || valuesFileSize > math.MaxUint32 {
		valuesFileSize = math.MaxUint32
	}
	valuesFileReaders := opts.ValuesFileReaders
	if valuesFileReaders < 1 {
		valuesFileReaders = 1
	}
	checksumInterval := opts.ChecksumInterval
	if checksumInterval < 1024 {
		checksumInterval = 1024
	} else if checksumInterval >= 4294967296 {
		checksumInterval = 4294967295
	}
	if memTOCPageSize < checksumInterval/2+1 {
		memTOCPageSize = checksumInterval/2 + 1
	}
	if memTOCPageSize > math.MaxUint32 {
		memTOCPageSize = math.MaxUint32
	}
	if memValuesPageSize < checksumInterval/2+1 {
		memValuesPageSize = checksumInterval/2 + 1
	}
	if memValuesPageSize > math.MaxUint32 {
		memValuesPageSize = math.MaxUint32
	}
	vs := &ValuesStore{
		valuesLocBlocks:   make([]valuesLocBlock, 65536),
		vlm:               newValuesLocMap(opts),
		cores:             cores,
		maxValueSize:      uint32(maxValueSize),
		tombstoneAge:      tombstoneAge,
		memTOCPageSize:    uint32(memTOCPageSize),
		memValuesPageSize: uint32(memValuesPageSize),
		valuesFileSize:    uint32(valuesFileSize),
		checksumInterval:  uint32(checksumInterval),
		valuesFileReaders: valuesFileReaders,
	}
	vs.freeableVMChan = make(chan *valuesMem, vs.cores)
	vs.freeVMChan = make(chan *valuesMem, vs.cores*2)
	vs.freeVWRChans = make([]chan *valueWriteReq, vs.cores)
	vs.pendingVWRChans = make([]chan *valueWriteReq, vs.cores)
	vs.vfVMChan = make(chan *valuesMem, vs.cores)
	vs.freeTOCBlockChan = make(chan []byte, vs.cores*2)
	vs.pendingTOCBlockChan = make(chan []byte, vs.cores)
	vs.tocWriterDoneChan = make(chan struct{}, 1)
	vs.backgroundNotifyChan = make(chan chan struct{}, 1)
	vs.backgroundDoneChan = make(chan struct{}, 1)
	for i := 0; i < cap(vs.freeVMChan); i++ {
		vm := &valuesMem{
			vs:     vs,
			toc:    make([]byte, 0, vs.memTOCPageSize),
			values: make([]byte, 0, vs.memValuesPageSize),
		}
		vm.id = vs.addValuesLocBock(vm)
		vs.freeVMChan <- vm
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
		vs.freeTOCBlockChan <- make([]byte, 0, vs.memTOCPageSize)
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
	go vs.background()
	return vs
}

// MaxValueSize returns the maximum length of a value the ValuesStore can
// accept.
func (vs *ValuesStore) MaxValueSize() uint32 {
	return vs.maxValueSize
}

// Close shuts down all background processing and the ValuesStore will refuse
// any additional writes; reads may still occur.
func (vs *ValuesStore) Close() {
	for _, c := range vs.pendingVWRChans {
		c <- nil
	}
	<-vs.tocWriterDoneChan
	vs.backgroundNotifyChan <- nil
	<-vs.backgroundDoneChan
	for vs.vlm.isResizing() {
		time.Sleep(10 * time.Millisecond)
	}
}

// Lookup will return timestamp, length, err for keyA, keyB.
//
// Note that err == ErrValueNotFound with timestamp == 0 indicates keyA, keyB
// was not known at all whereas err == ErrValueNotFound with timestamp != 0
// (also timestamp & 1 == 1) indicates keyA, keyB was known and had a deletion
// marker (aka tombstone).
//
// This may be called even after Close.
func (vs *ValuesStore) Lookup(keyA uint64, keyB uint64) (uint64, uint32, error) {
	timestamp, id, _, length := vs.vlm.get(keyA, keyB)
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
func (vs *ValuesStore) Read(keyA uint64, keyB uint64, value []byte) (uint64, []byte, error) {
	timestamp, id, offset, length := vs.vlm.get(keyA, keyB)
	if id == 0 || timestamp&1 == 1 {
		return timestamp, value, ErrValueNotFound
	}
	return vs.valuesLocBlock(id).read(keyA, keyB, timestamp, offset, length, value)
}

// Write stores timestamp & 0xfffffffffffffffe (lowest bit zeroed), value for
// keyA, keyB or returns any error; a newer timestamp already in place is not
// reported as an error.
//
// This may no longer be called after Close.
func (vs *ValuesStore) Write(keyA uint64, keyB uint64, timestamp uint64, value []byte) (uint64, error) {
	i := int(keyA>>1) % len(vs.freeVWRChans)
	vwr := <-vs.freeVWRChans[i]
	vwr.keyA = keyA
	vwr.keyB = keyB
	vwr.timestamp = timestamp & 0xfffffffffffffffe
	vwr.value = value
	vs.pendingVWRChans[i] <- vwr
	err := <-vwr.errChan
	oldTimestamp := vwr.timestamp
	vwr.value = nil
	vs.freeVWRChans[i] <- vwr
	return oldTimestamp, err
}

// Delete stores timestamp | 1 for keyA, keyB or returns any error; a newer
// timestamp already in place is not reported as an error.
//
// This may no longer be called after Close.
func (vs *ValuesStore) Delete(keyA uint64, keyB uint64, timestamp uint64) (uint64, error) {
	i := int(keyA>>1) % len(vs.freeVWRChans)
	vwr := <-vs.freeVWRChans[i]
	vwr.keyA = keyA
	vwr.keyB = keyB
	vwr.timestamp = timestamp | 1
	vwr.value = nil
	vs.pendingVWRChans[i] <- vwr
	err := <-vwr.errChan
	oldTimestamp := vwr.timestamp
	vs.freeVWRChans[i] <- vwr
	return oldTimestamp, err
}

// BackgroundNow will trigger background tasks to run now instead of waiting
// for the next interval; this function will not return until the background
// tasks complete.
func (vs *ValuesStore) BackgroundNow() {
	c := make(chan struct{}, 1)
	vs.backgroundNotifyChan <- c
	<-c
}

// GatherStats returns overall information about the state of the ValuesStore.
//
// This may be called even after Close.
func (vs *ValuesStore) GatherStats(extended bool) *ValuesStoreStats {
	stats := &ValuesStoreStats{}
	if extended {
		stats.extended = extended
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
		stats.maxValuesLocBlockID = atomic.LoadUint32(&vs.atValuesLocBlocksIDer)
		stats.cores = vs.cores
		stats.maxValueSize = vs.maxValueSize
		stats.tombstoneAge = vs.tombstoneAge
		stats.memTOCPageSize = vs.memTOCPageSize
		stats.memValuesPageSize = vs.memValuesPageSize
		stats.valuesFileSize = vs.valuesFileSize
		stats.valuesFileReaders = vs.valuesFileReaders
		stats.checksumInterval = vs.checksumInterval
		stats.vlmStats = vs.vlm.gatherStats(true)
	} else {
		stats.vlmStats = vs.vlm.gatherStats(false)
	}
	return stats
}

func (vs *ValuesStore) valuesLocBlock(valuesLocBlockID uint16) valuesLocBlock {
	return vs.valuesLocBlocks[valuesLocBlockID]
}

func (vs *ValuesStore) addValuesLocBock(block valuesLocBlock) uint16 {
	id := atomic.AddUint32(&vs.atValuesLocBlocksIDer, 1)
	if id >= 65536 {
		panic("too many valuesLocBlocks")
	}
	vs.valuesLocBlocks[id] = block
	return uint16(id)
}

func (vs *ValuesStore) memClearer() {
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
		vf := vs.valuesLocBlock(vm.vfID)
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
			oldTimestamp := vs.vlm.set(keyA, keyB, timestamp, vm.vfID, vm.vfOffset+vmMemOffset, length, true)
			if oldTimestamp != timestamp {
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

func (vs *ValuesStore) memWriter(pendingVWRChan chan *valueWriteReq) {
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
		if vm != nil && (vmTOCOffset+32 > cap(vm.toc) || vmMemOffset+length > cap(vm.values)) {
			vs.vfVMChan <- vm
			vm = nil
		}
		if vm == nil {
			vm = <-vs.freeVMChan
			vmTOCOffset = 0
			vmMemOffset = 0
		}
		vm.discardLock.Lock()
		vm.values = vm.values[:vmMemOffset+length]
		vm.discardLock.Unlock()
		copy(vm.values[vmMemOffset:], vwr.value)
		oldTimestamp := vs.vlm.set(vwr.keyA, vwr.keyB, vwr.timestamp, vm.id, uint32(vmMemOffset), uint32(length), false)
		if oldTimestamp < vwr.timestamp {
			vm.toc = vm.toc[:vmTOCOffset+32]
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset:], vwr.keyA)
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+8:], vwr.keyB)
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+16:], vwr.timestamp)
			binary.BigEndian.PutUint32(vm.toc[vmTOCOffset+24:], uint32(vmMemOffset))
			binary.BigEndian.PutUint32(vm.toc[vmTOCOffset+28:], uint32(length))
			vmTOCOffset += 32
			vmMemOffset += length
		} else {
			vm.discardLock.Lock()
			vm.values = vm.values[:vmMemOffset]
			vm.discardLock.Unlock()
		}
		vwr.timestamp = oldTimestamp
		vwr.errChan <- nil
	}
}

func (vs *ValuesStore) vfWriter() {
	var vf *valuesFile
	memWritersLeft := vs.cores
	var tocLen uint64
	var valuesLen uint64
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
		if vf != nil && (tocLen+uint64(len(vm.toc)) >= uint64(vs.valuesFileSize) || valuesLen+uint64(len(vm.values)) > uint64(vs.valuesFileSize)) {
			vf.close()
			vf = nil
		}
		if vf == nil {
			vf = createValuesFile(vs)
			tocLen = 32
			valuesLen = 32
		}
		vf.write(vm)
		tocLen += uint64(len(vm.toc))
		valuesLen += uint64(len(vm.values))
	}
}

func (vs *ValuesStore) tocWriter() {
	var btsA uint64
	var writerA io.WriteCloser
	var offsetA uint64
	var btsB uint64
	var writerB io.WriteCloser
	var offsetB uint64
	head := []byte("BRIMSTORE VALUESTOC v0          ")
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

func (vs *ValuesStore) recovery() {
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
					if vs.vlm.set(wr.keyA, wr.keyB, wr.timestamp, wr.blockID, wr.offset, wr.length, false) < wr.timestamp {
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
					if !bytes.Equal(buf[:28], []byte("BRIMSTORE VALUESTOC v0      ")) {
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
		log.Printf("%d key locations loaded in %s, %.0f/s; %d caused change; %d resulting locations.\n", fromDiskCount, dur, float64(fromDiskCount)/(float64(dur)/float64(time.Second)), count, vs.GatherStats(false).ValueCount())
	}
}

func (vs *ValuesStore) background() {
	interval := float64(60 * time.Second)
	nextRun := time.Now().Add(time.Duration(interval + interval*rand.NormFloat64()*0.1))
WORK:
	for {
		var c chan struct{} = nil
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case c = <-vs.backgroundNotifyChan:
				if c == nil {
					break WORK
				}
			case <-time.After(sleep):
			}
		} else {
			select {
			case c = <-vs.backgroundNotifyChan:
				if c == nil {
					break WORK
				}
			default:
			}
		}
		nextRun = time.Now().Add(time.Duration(interval + interval*rand.NormFloat64()*0.1))
		vs.vlm.background(vs)
		if c != nil {
			c <- struct{}{}
		}
	}
	vs.backgroundDoneChan <- struct{}{}
}

type valueWriteReq struct {
	keyA      uint64
	keyB      uint64
	timestamp uint64
	value     []byte
	errChan   chan error
}

type valuesLocBlock interface {
	timestamp() int64
	read(keyA uint64, keyB uint64, timestamp uint64, offset uint32, length uint32, value []byte) (uint64, []byte, error)
}

type ValuesStoreStats struct {
	extended               bool
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
	maxValuesLocBlockID    uint32
	cores                  int
	maxValueSize           uint32
	tombstoneAge           uint64
	memTOCPageSize         uint32
	memValuesPageSize      uint32
	valuesFileSize         uint32
	valuesFileReaders      int
	checksumInterval       uint32
	vlmStats               *valuesLocMapStats
}

func (stats *ValuesStoreStats) String() string {
	if stats.extended {
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
			[]string{"maxValuesLocBlockID", fmt.Sprintf("%d", stats.maxValuesLocBlockID)},
			[]string{"cores", fmt.Sprintf("%d", stats.cores)},
			[]string{"maxValueSize", fmt.Sprintf("%d", stats.maxValueSize)},
			[]string{"tombstoneAge", fmt.Sprintf("%d", stats.tombstoneAge)},
			[]string{"memTOCPageSize", fmt.Sprintf("%d", stats.memTOCPageSize)},
			[]string{"memValuesPageSize", fmt.Sprintf("%d", stats.memValuesPageSize)},
			[]string{"valuesFileSize", fmt.Sprintf("%d", stats.valuesFileSize)},
			[]string{"valuesFileReaders", fmt.Sprintf("%d", stats.valuesFileReaders)},
			[]string{"checksumInterval", fmt.Sprintf("%d", stats.checksumInterval)},
			[]string{"vlm", stats.vlmStats.String()},
		}, nil)
	} else {
		return brimtext.Align([][]string{
			[]string{"vlm", stats.vlmStats.String()},
		}, nil)
	}
}

func (stats *ValuesStoreStats) ValueCount() uint64 {
	return stats.vlmStats.active
}

func (stats *ValuesStoreStats) ValuesLength() uint64 {
	return stats.vlmStats.length
}
