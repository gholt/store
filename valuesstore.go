package brimstore

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/brimutil"
	"github.com/spaolacci/murmur3"
)

var ErrValueNotFound error = fmt.Errorf("value not found")

type writeValue struct {
	KeyA        uint64
	KeyB        uint64
	Value       []byte
	Seq         uint64
	WrittenChan chan error
}

// ValuesStoreOpts allows configuration of the ValuesStore, although normally
// the defaults are best.
type ValuesStoreOpts struct {
	Cores                int
	MaxValueSize         int
	MemTOCPageSize       int
	MemValuesPageSize    int
	ValuesLocMapPageSize int
	ValuesFileSize       int
	ValuesFileReaders    int
	ChecksumInterval     int
}

func NewValuesStoreOpts() *ValuesStoreOpts {
	opts := &ValuesStoreOpts{}
	if env := os.Getenv("BRIMSTORE_VALUESSTORE_CORES"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.Cores = val
		}
	}
	if opts.Cores <= 0 {
		opts.Cores = runtime.GOMAXPROCS(0)
	}
	if env := os.Getenv("BRIMSTORE_VALUESSTORE_MAX_VALUE_SIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.MaxValueSize = val
		}
	}
	if opts.MaxValueSize <= 0 {
		opts.MaxValueSize = 4 * 1024 * 1024
	}
	if env := os.Getenv("BRIMSTORE_VALUESSTORE_MEMTOCPAGESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.MemTOCPageSize = val
		}
	}
	if opts.MemTOCPageSize <= 0 {
		opts.MemTOCPageSize = 1 << brimutil.PowerOfTwoNeeded(uint64(opts.MaxValueSize+4))
	}
	if env := os.Getenv("BRIMSTORE_VALUESSTORE_MEMVALUESPAGESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.MemValuesPageSize = val
		}
	}
	if opts.MemValuesPageSize <= 0 {
		opts.MemValuesPageSize = 1 << brimutil.PowerOfTwoNeeded(uint64(opts.MaxValueSize+4))
	}
	if env := os.Getenv("BRIMSTORE_VALUESSTORE_VALUESLOCMAP_PAGESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.ValuesLocMapPageSize = val
		}
	}
	if opts.ValuesLocMapPageSize <= 0 {
		opts.ValuesLocMapPageSize = 4 * 1024 * 1024
	}
	if env := os.Getenv("BRIMSTORE_VALUESSTORE_VALUESFILE_SIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.ValuesFileSize = val
		}
	}
	if opts.ValuesFileSize <= 0 {
		opts.ValuesFileSize = math.MaxUint32
	}
	if env := os.Getenv("BRIMSTORE_VALUESSTORE_VALUESFILE_READERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.ValuesFileReaders = val
		}
	}
	if opts.ValuesFileReaders <= 0 {
		opts.ValuesFileReaders = opts.Cores
		if opts.Cores > 8 {
			opts.ValuesFileReaders = 8
		}
	}
	if env := os.Getenv("BRIMSTORE_VALUESSTORE_CHECKSUMINTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.ChecksumInterval = val
		}
	}
	if opts.ChecksumInterval <= 0 {
		opts.ChecksumInterval = 65532
	}
	return opts
}

// ValuesStore will store []byte values referenced by 128 bit keys.
type ValuesStore struct {
	clearableMemBlockChan chan *memBlock
	clearedMemBlockChan   chan *memBlock
	writeValueChans       []chan *writeValue
	vfMBChan              chan *memBlock
	freeTOCBlockChan      chan []byte
	pendingTOCBlockChan   chan []byte
	memWriterDoneChans    []chan struct{}
	memClearerDoneChans   []chan struct{}
	vfDoneChan            chan struct{}
	tocWriterDoneChan     chan struct{}
	valuesLocBlocks       []valuesLocBlock
	atValuesLocBlocksIDer int32
	vlm                   *valuesLocMap
	cores                 int
	maxValueSize          int
	memTOCPageSize        int
	memValuesPageSize     int
	valuesFileSize        int
	valuesFileReaders     int
	checksumInterval      uint32
}

// NewValuesStore initializes a ValuesStore for use; opts may be nil to use the
// defaults.
func NewValuesStore(opts *ValuesStoreOpts) *ValuesStore {
	if opts == nil {
		opts = NewValuesStoreOpts()
	}
	cores := opts.Cores
	if cores < 1 {
		cores = 1
	}
	maxValueSize := opts.MaxValueSize
	if maxValueSize < 0 {
		maxValueSize = 0
	}
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
	vs := &ValuesStore{
		valuesLocBlocks:       make([]valuesLocBlock, 65536),
		atValuesLocBlocksIDer: _VALUESBLOCK_IDOFFSET - 1,
		vlm:               newValuesLocMap(opts),
		cores:             cores,
		maxValueSize:      maxValueSize,
		memTOCPageSize:    memTOCPageSize,
		memValuesPageSize: memValuesPageSize,
		valuesFileSize:    valuesFileSize,
		checksumInterval:  uint32(checksumInterval),
		valuesFileReaders: valuesFileReaders,
	}
	vs.clearableMemBlockChan = make(chan *memBlock, vs.cores)
	vs.clearedMemBlockChan = make(chan *memBlock, vs.cores)
	vs.writeValueChans = make([]chan *writeValue, vs.cores)
	vs.vfMBChan = make(chan *memBlock, vs.cores)
	vs.freeTOCBlockChan = make(chan []byte, vs.cores)
	vs.pendingTOCBlockChan = make(chan []byte, vs.cores)
	vs.memWriterDoneChans = make([]chan struct{}, vs.cores)
	vs.memClearerDoneChans = make([]chan struct{}, vs.cores)
	vs.vfDoneChan = make(chan struct{}, 1)
	vs.tocWriterDoneChan = make(chan struct{}, 1)
	for i := 0; i < cap(vs.clearableMemBlockChan); i++ {
		mb := &memBlock{
			vs:   vs,
			toc:  make([]byte, 0, vs.memTOCPageSize),
			data: make([]byte, 0, vs.memValuesPageSize),
		}
		mb.id = vs.addValuesLocBock(mb)
		vs.clearableMemBlockChan <- mb
	}
	for i := 0; i < cap(vs.clearedMemBlockChan); i++ {
		mb := &memBlock{
			vs:   vs,
			toc:  make([]byte, 0, vs.memTOCPageSize),
			data: make([]byte, 0, vs.memValuesPageSize),
		}
		mb.id = vs.addValuesLocBock(mb)
		vs.clearedMemBlockChan <- mb
	}
	for i := 0; i < len(vs.writeValueChans); i++ {
		vs.writeValueChans[i] = make(chan *writeValue, vs.cores)
	}
	for i := 0; i < cap(vs.vfMBChan); i++ {
		mb := &memBlock{
			vs:   vs,
			toc:  make([]byte, 0, vs.memTOCPageSize),
			data: make([]byte, 0, vs.memValuesPageSize),
		}
		mb.id = vs.addValuesLocBock(mb)
		vs.vfMBChan <- mb
	}
	for i := 0; i < cap(vs.freeTOCBlockChan); i++ {
		vs.freeTOCBlockChan <- make([]byte, 0, vs.memTOCPageSize)
	}
	for i := 0; i < cap(vs.pendingTOCBlockChan); i++ {
		vs.pendingTOCBlockChan <- make([]byte, 0, vs.memTOCPageSize)
	}
	for i := 0; i < len(vs.memWriterDoneChans); i++ {
		vs.memWriterDoneChans[i] = make(chan struct{}, 1)
	}
	for i := 0; i < len(vs.memClearerDoneChans); i++ {
		vs.memClearerDoneChans[i] = make(chan struct{}, 1)
	}
	go vs.tocWriter()
	go vs.vfWriter()
	for i := 0; i < len(vs.memClearerDoneChans); i++ {
		go vs.memClearer(vs.memClearerDoneChans[i])
	}
	for i := 0; i < len(vs.writeValueChans); i++ {
		go vs.memWriter(vs.writeValueChans[i], vs.memWriterDoneChans[i])
	}
	return vs
}

func (vs *ValuesStore) MaxValueSize() int {
	return vs.maxValueSize
}

func (vs *ValuesStore) Close() {
	for _, c := range vs.writeValueChans {
		c <- nil
	}
	<-vs.tocWriterDoneChan
	for vs.vlm.isResizing() {
		time.Sleep(10 * time.Millisecond)
	}
}

// ReadValue will return value, seq, err for keyA, keyB; if an incoming value
// is provided, the read value will be appended to it and the whole returned
// (useful to reuse an existing []byte).
func (vs *ValuesStore) ReadValue(keyA uint64, keyB uint64, value []byte) ([]byte, uint64, error) {
	id, offset, seq := vs.vlm.get(keyA, keyB)
	if id < _VALUESBLOCK_IDOFFSET {
		return value, 0, ErrValueNotFound
	}
	return vs.valuesLocBlock(id).readValue(keyA, keyB, value, seq, offset)
}

// WriteValue stores value, seq for keyA, keyB or returns any error; a newer
// seq already in place is not reported as an error.
func (vs *ValuesStore) WriteValue(keyA uint64, keyB uint64, value []byte, seq uint64) error {
	c := make(chan error, 1)
	vs.writeValueChans[int(keyA>>1)%len(vs.writeValueChans)] <- &writeValue{
		KeyA:        keyA,
		KeyB:        keyB,
		Value:       value,
		Seq:         seq,
		WrittenChan: c,
	}
	return <-c
}

func (vs *ValuesStore) valuesLocBlock(valuesLocBlockID uint16) valuesLocBlock {
	return vs.valuesLocBlocks[valuesLocBlockID]
}

func (vs *ValuesStore) addValuesLocBock(block valuesLocBlock) uint16 {
	id := atomic.AddInt32(&vs.atValuesLocBlocksIDer, 1)
	if id >= 65536 {
		panic("too many valuesLocBlocks")
	}
	vs.valuesLocBlocks[id] = block
	return uint16(id)
}

func (vs *ValuesStore) memClearer(memClearerDoneChan chan struct{}) {
	var tb []byte
	var tbTS int64
	var tbOffset int
	for {
		mb := <-vs.clearableMemBlockChan
		if mb == nil {
			if tb != nil {
				binary.BigEndian.PutUint32(tb, uint32(len(tb)-4))
				vs.pendingTOCBlockChan <- tb
				<-vs.freeTOCBlockChan
			}
			vs.pendingTOCBlockChan <- nil
			<-vs.freeTOCBlockChan
			break
		}
		vf := vs.valuesLocBlock(mb.vfID)
		if tb != nil && tbTS != vf.timestamp() {
			binary.BigEndian.PutUint32(tb, uint32(len(tb)-4))
			vs.pendingTOCBlockChan <- tb
			tb = nil
		}
		for mbTOCOffset := 0; mbTOCOffset < len(mb.toc); mbTOCOffset += 28 {
			mbDataOffset := binary.BigEndian.Uint32(mb.toc[mbTOCOffset:])
			a := binary.BigEndian.Uint64(mb.toc[mbTOCOffset+4:])
			b := binary.BigEndian.Uint64(mb.toc[mbTOCOffset+12:])
			q := binary.BigEndian.Uint64(mb.toc[mbTOCOffset+20:])
			vs.vlm.set(mb.vfID, mb.vfOffset+mbDataOffset, a, b, q)
			if tb != nil && tbOffset+28 > cap(tb) {
				binary.BigEndian.PutUint32(tb, uint32(len(tb)-4))
				vs.pendingTOCBlockChan <- tb
				tb = nil
			}
			if tb == nil {
				tb = <-vs.freeTOCBlockChan
				tbTS = vf.timestamp()
				tb = tb[:12]
				binary.BigEndian.PutUint64(tb[4:], uint64(tbTS))
				tbOffset = 12
			}
			tb = tb[:tbOffset+28]
			binary.BigEndian.PutUint32(tb[tbOffset:], mb.vfOffset+uint32(mbDataOffset))
			binary.BigEndian.PutUint64(tb[tbOffset+4:], a)
			binary.BigEndian.PutUint64(tb[tbOffset+12:], b)
			binary.BigEndian.PutUint64(tb[tbOffset+20:], q)
			tbOffset += 28
		}
		mb.discardLock.Lock()
		mb.vfID = 0
		mb.vfOffset = 0
		mb.toc = mb.toc[:0]
		mb.data = mb.data[:0]
		mb.discardLock.Unlock()
		vs.clearedMemBlockChan <- mb
	}
	memClearerDoneChan <- struct{}{}
}

func (vs *ValuesStore) memWriter(writeValueChan chan *writeValue, memWriterDoneChan chan struct{}) {
	var mb *memBlock
	var mbTOCOffset int
	var mbDataOffset int
	for {
		w := <-writeValueChan
		if w == nil {
			if mb != nil && len(mb.toc) > 0 {
				vs.vfMBChan <- mb
				<-vs.clearedMemBlockChan
			}
			vs.vfMBChan <- nil
			<-vs.clearedMemBlockChan
			break
		}
		vz := len(w.Value)
		if vz > vs.maxValueSize {
			w.WrittenChan <- fmt.Errorf("value length of %d > %d", vz, vs.maxValueSize)
			continue
		}
		if mb != nil && (mbTOCOffset+28 > cap(mb.toc) || mbDataOffset+4+vz > cap(mb.data)) {
			vs.vfMBChan <- mb
			mb = nil
		}
		if mb == nil {
			mb = <-vs.clearedMemBlockChan
			mbTOCOffset = 0
			mbDataOffset = 0
		}
		mb.toc = mb.toc[:mbTOCOffset+28]
		binary.BigEndian.PutUint32(mb.toc[mbTOCOffset:], uint32(mbDataOffset))
		binary.BigEndian.PutUint64(mb.toc[mbTOCOffset+4:], w.KeyA)
		binary.BigEndian.PutUint64(mb.toc[mbTOCOffset+12:], w.KeyB)
		binary.BigEndian.PutUint64(mb.toc[mbTOCOffset+20:], w.Seq)
		mbTOCOffset += 28
		mb.discardLock.Lock()
		mb.data = mb.data[:mbDataOffset+4+vz]
		mb.discardLock.Unlock()
		binary.BigEndian.PutUint32(mb.data[mbDataOffset:], uint32(vz))
		copy(mb.data[mbDataOffset+4:], w.Value)
		vs.vlm.set(mb.id, uint32(mbDataOffset), w.KeyA, w.KeyB, w.Seq)
		mbDataOffset += 4 + vz
		w.WrittenChan <- nil
	}
	memWriterDoneChan <- struct{}{}
}

func (vs *ValuesStore) vfWriter() {
	var vf *valuesFile
	memWritersLeft := vs.cores
	for {
		mb := <-vs.vfMBChan
		if mb == nil {
			memWritersLeft--
			if memWritersLeft < 1 {
				if vf != nil {
					vf.close()
				}
				for i := 0; i <= vs.cores; i++ {
					vf.vs.clearableMemBlockChan <- nil
				}
				break
			}
			continue
		}
		if vf != nil && int(atomic.LoadUint32(&vf.atOffset))+len(mb.data) > vs.valuesFileSize {
			vf.close()
			vf = nil
		}
		if vf == nil {
			vf = newValuesFile(vs)
		}
		vf.write(mb)
	}
	vs.vfDoneChan <- struct{}{}
}

func (vs *ValuesStore) tocWriter() {
	var tsA uint64
	var writerA io.WriteCloser
	var offsetA uint64
	var tsB uint64
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
					offsetB += 16
				}
				if writerA != nil {
					binary.BigEndian.PutUint64(term[4:], offsetA)
					if _, err := writerA.Write(term); err != nil {
						panic(err)
					}
					if err := writerA.Close(); err != nil {
						panic(err)
					}
					offsetA += 16
				}
				break
			}
			continue
		}
		if len(t) > 0 {
			ts := binary.BigEndian.Uint64(t[4:])
			switch ts {
			case tsA:
				if _, err := writerA.Write(t); err != nil {
					panic(err)
				}
				offsetA += uint64(len(t))
			case tsB:
				if _, err := writerB.Write(t); err != nil {
					panic(err)
				}
				offsetB += uint64(len(t))
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
					offsetB += 16
				}
				tsB = tsA
				writerB = writerA
				offsetB = offsetA
				tsA = ts
				fp, err := os.Create(fmt.Sprintf("%d.toc", ts))
				if err != nil {
					panic(err)
				}
				writerA = brimutil.NewMultiCoreChecksummedWriter(fp, int(vs.checksumInterval), murmur3.New32, vs.cores)
				if _, err := writerA.Write(head); err != nil {
					panic(err)
				}
				if _, err := writerA.Write(t); err != nil {
					panic(err)
				}
				offsetA = 32 + uint64(len(t))
			}
		}
		vs.freeTOCBlockChan <- t[:0]
	}
	vs.tocWriterDoneChan <- struct{}{}
}

type valuesLocBlock interface {
	timestamp() int64
	readValue(keyA uint64, keyB uint64, value []byte, seq uint64, offset uint32) ([]byte, uint64, error)
}

type memBlock struct {
	vs          *ValuesStore
	id          uint16
	vfID        uint16
	vfOffset    uint32
	toc         []byte
	data        []byte
	discardLock sync.RWMutex
}

func (mb *memBlock) timestamp() int64 {
	return math.MaxInt64
}

func (mb *memBlock) readValue(keyA uint64, keyB uint64, value []byte, seq uint64, offset uint32) ([]byte, uint64, error) {
	mb.discardLock.RLock()
	id, offset, seq := mb.vs.vlm.get(keyA, keyB)
	if id < _VALUESBLOCK_IDOFFSET {
		mb.discardLock.RUnlock()
		return value, seq, ErrValueNotFound
	}
	if id != mb.id {
		mb.discardLock.RUnlock()
		mb.vs.valuesLocBlock(id).readValue(keyA, keyB, value, seq, offset)
	}
	value = append(value, mb.data[offset+4:offset+4+binary.BigEndian.Uint32(mb.data[offset:])]...)
	mb.discardLock.RUnlock()
	return value, seq, nil
}
