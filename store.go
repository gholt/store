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

var ErrKeyNotFound error = fmt.Errorf("key not found")

type ReadValue struct {
	KeyHashA uint64
	KeyHashB uint64
	Value    []byte
	Seq      uint64
	ReadChan chan error
	offset   uint32
}

type WriteValue struct {
	KeyHashA    uint64
	KeyHashB    uint64
	Value       []byte
	Seq         uint64
	WrittenChan chan error
}

type StoreOpts struct {
	Cores                  int
	MaxValueSize           int
	MemTOCPageSize         int
	MemValuesPageSize      int
	KeyLocationMapPageSize int
	ChecksumInterval       int
	ReadersPerValuesFile   int
}

func NewStoreOpts() *StoreOpts {
	opts := &StoreOpts{}
	if env := os.Getenv("BRIMSTORE_CORES"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.Cores = val
		}
	}
	if opts.Cores <= 0 {
		opts.Cores = runtime.GOMAXPROCS(0)
	}
	if env := os.Getenv("BRIMSTORE_MAX_VALUE_SIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.MaxValueSize = val
		}
	}
	if opts.MaxValueSize <= 0 {
		opts.MaxValueSize = 4 * 1024 * 1024
	}
	if env := os.Getenv("BRIMSTORE_MEM_TOC_PAGE_SIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.MemTOCPageSize = val
		}
	}
	if opts.MemTOCPageSize <= 0 {
		opts.MemTOCPageSize = 1 << PowerOfTwoNeeded(uint64(opts.MaxValueSize+4))
	}
	if env := os.Getenv("BRIMSTORE_MEM_VALUES_PAGE_SIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.MemValuesPageSize = val
		}
	}
	if opts.MemValuesPageSize <= 0 {
		opts.MemValuesPageSize = 1 << PowerOfTwoNeeded(uint64(opts.MaxValueSize+4))
	}
	if env := os.Getenv("BRIMSTORE_KEY_LOCATION_MAP_PAGE_SIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.KeyLocationMapPageSize = val
		}
	}
	if opts.KeyLocationMapPageSize <= 0 {
		opts.KeyLocationMapPageSize = 4 * 1024 * 1024
	}
	if env := os.Getenv("BRIMSTORE_CHECKSUM_INTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.ChecksumInterval = val
		}
	}
	if opts.ChecksumInterval <= 0 {
		opts.ChecksumInterval = 65532
	}
	if env := os.Getenv("BRIMSTORE_READERS_PER_VALUES_FILE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			opts.ReadersPerValuesFile = val
		}
	}
	if opts.ReadersPerValuesFile <= 0 {
		opts.ReadersPerValuesFile = opts.Cores
		if opts.Cores > 8 {
			opts.ReadersPerValuesFile = 8
		}
	}
	return opts
}

type Store struct {
	clearableMemBlockChan    chan *memBlock
	clearedMemBlockChan      chan *memBlock
	writeValueChans          []chan *WriteValue
	diskWritableMemBlockChan chan *memBlock
	freeTOCBlockChan         chan []byte
	pendingTOCBlockChan      chan []byte
	memWriterDoneChans       []chan struct{}
	memClearerDoneChans      []chan struct{}
	diskWriterDoneChan       chan struct{}
	tocWriterDoneChan        chan struct{}
	keyLocationBlocks        []keyLocationBlock
	keyLocationBlocksIDer    int32
	keyLocationMap           *keyLocationMap
	cores                    int
	maxValueSize             int
	memTOCPageSize           int
	memValuesPageSize        int
	checksumInterval         int
	readersPerValuesFile     int
	diskWriterBytes          uint64
	tocWriterBytes           uint64
}

func NewStore(opts *StoreOpts) *Store {
	if opts == nil {
		opts = NewStoreOpts()
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
	fmt.Println(memValuesPageSize, "memValuesPageSize")
	checksumInterval := opts.ChecksumInterval
	if checksumInterval < 1024 {
		checksumInterval = 1024
	} else if checksumInterval >= 4294967296 {
		checksumInterval = 4294967295
	}
	readersPerValuesFile := opts.ReadersPerValuesFile
	if readersPerValuesFile < 1 {
		readersPerValuesFile = 1
	}
	s := &Store{
		keyLocationBlocks:     make([]keyLocationBlock, 65536),
		keyLocationBlocksIDer: KEY_LOCATION_BLOCK_ID_OFFSET - 1,
		keyLocationMap:        newKeyLocationMap(opts),
		cores:                 cores,
		maxValueSize:          maxValueSize,
		memTOCPageSize:        memTOCPageSize,
		memValuesPageSize:     memValuesPageSize,
		checksumInterval:      checksumInterval,
		readersPerValuesFile:  readersPerValuesFile,
	}
	return s
}

func (s *Store) MaxValueSize() int {
	return s.maxValueSize
}

func (s *Store) NewReadValue() *ReadValue {
	return &ReadValue{
		Value:    make([]byte, s.maxValueSize),
		ReadChan: make(chan error, 1),
	}
}

func (s *Store) Start() {
	s.clearableMemBlockChan = make(chan *memBlock, s.cores)
	s.clearedMemBlockChan = make(chan *memBlock, s.cores)
	s.writeValueChans = make([]chan *WriteValue, s.cores)
	s.diskWritableMemBlockChan = make(chan *memBlock, s.cores)
	s.freeTOCBlockChan = make(chan []byte, s.cores)
	s.pendingTOCBlockChan = make(chan []byte, s.cores)
	s.memWriterDoneChans = make([]chan struct{}, s.cores)
	s.memClearerDoneChans = make([]chan struct{}, s.cores)
	s.diskWriterDoneChan = make(chan struct{}, 1)
	s.tocWriterDoneChan = make(chan struct{}, 1)
	for i := 0; i < cap(s.clearableMemBlockChan); i++ {
		mb := &memBlock{
			store: s,
			toc:   make([]byte, 0, s.memTOCPageSize),
			data:  make([]byte, 0, s.memValuesPageSize),
		}
		mb.id = s.addKeyLocationBlock(mb)
		s.clearableMemBlockChan <- mb
	}
	for i := 0; i < cap(s.clearedMemBlockChan); i++ {
		mb := &memBlock{
			store: s,
			toc:   make([]byte, 0, s.memTOCPageSize),
			data:  make([]byte, 0, s.memValuesPageSize),
		}
		mb.id = s.addKeyLocationBlock(mb)
		s.clearedMemBlockChan <- mb
	}
	for i := 0; i < len(s.writeValueChans); i++ {
		s.writeValueChans[i] = make(chan *WriteValue, s.cores)
	}
	for i := 0; i < cap(s.diskWritableMemBlockChan); i++ {
		mb := &memBlock{
			store: s,
			toc:   make([]byte, 0, s.memTOCPageSize),
			data:  make([]byte, 0, s.memValuesPageSize),
		}
		mb.id = s.addKeyLocationBlock(mb)
		s.diskWritableMemBlockChan <- mb
	}
	for i := 0; i < cap(s.freeTOCBlockChan); i++ {
		s.freeTOCBlockChan <- make([]byte, 0, s.memTOCPageSize)
	}
	for i := 0; i < cap(s.pendingTOCBlockChan); i++ {
		s.pendingTOCBlockChan <- make([]byte, 0, s.memTOCPageSize)
	}
	for i := 0; i < len(s.memWriterDoneChans); i++ {
		s.memWriterDoneChans[i] = make(chan struct{}, 1)
	}
	for i := 0; i < len(s.memClearerDoneChans); i++ {
		s.memClearerDoneChans[i] = make(chan struct{}, 1)
	}
	go s.tocWriter()
	go s.diskWriter()
	for i := 0; i < len(s.memClearerDoneChans); i++ {
		go s.memClearer(s.memClearerDoneChans[i])
	}
	for i := 0; i < len(s.writeValueChans); i++ {
		go s.memWriter(s.writeValueChans[i], s.memWriterDoneChans[i])
	}
}

func (s *Store) BytesWritten() uint64 {
	return atomic.LoadUint64(&s.diskWriterBytes) + atomic.LoadUint64(&s.tocWriterBytes)
}

func (s *Store) Stop() uint64 {
	for _, c := range s.writeValueChans {
		c <- nil
	}
	<-s.tocWriterDoneChan
	for s.keyLocationMap.isResizing() {
		time.Sleep(10 * time.Millisecond)
	}
	return s.diskWriterBytes + s.tocWriterBytes
}

func (s *Store) Get(r *ReadValue) {
	var id uint16
	id, r.offset, r.Seq = s.keyLocationMap.get(r.KeyHashA, r.KeyHashB)
	if id < KEY_LOCATION_BLOCK_ID_OFFSET {
		r.ReadChan <- ErrKeyNotFound
	} else {
		s.keyLocationBlock(id).Get(r)
	}
}
func (s *Store) Put(w *WriteValue) {
	if w != nil {
		s.writeValueChans[int(w.KeyHashA>>1)%len(s.writeValueChans)] <- w
	}
}

func (s *Store) keyLocationBlock(keyLocationBlockID uint16) keyLocationBlock {
	return s.keyLocationBlocks[keyLocationBlockID]
}

func (s *Store) addKeyLocationBlock(block keyLocationBlock) uint16 {
	id := atomic.AddInt32(&s.keyLocationBlocksIDer, 1)
	if id >= 65536 {
		panic("too many keyLocationBlocks")
	}
	s.keyLocationBlocks[id] = block
	return uint16(id)
}

func (s *Store) memClearer(memClearerDoneChan chan struct{}) {
	var tb []byte
	var tbTimestamp int64
	var tbOffset int
	for {
		mb := <-s.clearableMemBlockChan
		if mb == nil {
			if tb != nil {
				binary.BigEndian.PutUint32(tb, uint32(len(tb)-4))
				s.pendingTOCBlockChan <- tb
				<-s.freeTOCBlockChan
			}
			s.pendingTOCBlockChan <- nil
			<-s.freeTOCBlockChan
			break
		}
		db := s.keyLocationBlock(mb.diskID)
		if tb != nil && tbTimestamp != db.Timestamp() {
			binary.BigEndian.PutUint32(tb, uint32(len(tb)-4))
			s.pendingTOCBlockChan <- tb
			tb = nil
		}
		for mbTOCOffset := 0; mbTOCOffset < len(mb.toc); mbTOCOffset += 28 {
			mbDataOffset := binary.BigEndian.Uint32(mb.toc[mbTOCOffset:])
			a := binary.BigEndian.Uint64(mb.toc[mbTOCOffset+4:])
			b := binary.BigEndian.Uint64(mb.toc[mbTOCOffset+12:])
			q := binary.BigEndian.Uint64(mb.toc[mbTOCOffset+20:])
			s.keyLocationMap.set(mb.diskID, mb.diskOffset+mbDataOffset, a, b, q)
			if tb != nil && tbOffset+28 > cap(tb) {
				binary.BigEndian.PutUint32(tb, uint32(len(tb)-4))
				s.pendingTOCBlockChan <- tb
				tb = nil
			}
			if tb == nil {
				tb = <-s.freeTOCBlockChan
				tbTimestamp = db.Timestamp()
				tb = tb[:12]
				binary.BigEndian.PutUint64(tb[4:], uint64(tbTimestamp))
				tbOffset = 12
			}
			tb = tb[:tbOffset+28]
			binary.BigEndian.PutUint32(tb[tbOffset:], mb.diskOffset+uint32(mbDataOffset))
			binary.BigEndian.PutUint64(tb[tbOffset+4:], a)
			binary.BigEndian.PutUint64(tb[tbOffset+12:], b)
			binary.BigEndian.PutUint64(tb[tbOffset+20:], q)
			tbOffset += 28
		}
		mb.discardLock.Lock()
		mb.diskID = 0
		mb.diskOffset = 0
		mb.toc = mb.toc[:0]
		mb.data = mb.data[:0]
		mb.discardLock.Unlock()
		s.clearedMemBlockChan <- mb
	}
	memClearerDoneChan <- struct{}{}
}

func (s *Store) memWriter(writeValueChan chan *WriteValue, memWriterDoneChan chan struct{}) {
	var mb *memBlock
	var mbTOCOffset int
	var mbDataOffset int
	for {
		w := <-writeValueChan
		if w == nil {
			if mb != nil && len(mb.toc) > 0 {
				s.diskWritableMemBlockChan <- mb
				<-s.clearedMemBlockChan
			}
			s.diskWritableMemBlockChan <- nil
			<-s.clearedMemBlockChan
			break
		}
		vz := len(w.Value)
		if vz > s.maxValueSize {
			w.WrittenChan <- fmt.Errorf("value length of %d > %d", vz, s.maxValueSize)
			continue
		}
		if mb != nil && (mbTOCOffset+28 > cap(mb.toc) || mbDataOffset+4+vz > cap(mb.data)) {
			s.diskWritableMemBlockChan <- mb
			mb = nil
		}
		if mb == nil {
			mb = <-s.clearedMemBlockChan
			mbTOCOffset = 0
			mbDataOffset = 0
		}
		mb.toc = mb.toc[:mbTOCOffset+28]
		binary.BigEndian.PutUint32(mb.toc[mbTOCOffset:], uint32(mbDataOffset))
		binary.BigEndian.PutUint64(mb.toc[mbTOCOffset+4:], w.KeyHashA)
		binary.BigEndian.PutUint64(mb.toc[mbTOCOffset+12:], w.KeyHashB)
		binary.BigEndian.PutUint64(mb.toc[mbTOCOffset+20:], w.Seq)
		mbTOCOffset += 28
		mb.discardLock.Lock()
		mb.data = mb.data[:mbDataOffset+4+vz]
		mb.discardLock.Unlock()
		binary.BigEndian.PutUint32(mb.data[mbDataOffset:], uint32(vz))
		copy(mb.data[mbDataOffset+4:], w.Value)
		s.keyLocationMap.set(mb.id, uint32(mbDataOffset), w.KeyHashA, w.KeyHashB, w.Seq)
		mbDataOffset += 4 + vz
		w.WrittenChan <- nil
	}
	memWriterDoneChan <- struct{}{}
}

func (s *Store) diskWriter() {
	var db *diskBlock
	memWritersLeft := s.cores
	for {
		mb := <-s.diskWritableMemBlockChan
		if mb == nil {
			memWritersLeft--
			if memWritersLeft < 1 {
				if db != nil {
					db.close()
				}
				for i := 0; i <= s.cores; i++ {
					db.store.clearableMemBlockChan <- nil
				}
				break
			}
			continue
		}
		if db != nil && db.offset()+len(mb.data) > math.MaxUint32 {
			db.close()
			db = nil
		}
		if db == nil {
			db = newDiskBlock(s)
		}
		db.write(mb)
	}
	s.diskWriterDoneChan <- struct{}{}
}

func (s *Store) tocWriter() {
	var timestampA uint64
	var writerA io.WriteCloser
	var offsetA uint64
	var timestampB uint64
	var writerB io.WriteCloser
	var offsetB uint64
	head := []byte("BRIMSTORE TOC v0                ")
	binary.BigEndian.PutUint32(head[28:], uint32(s.checksumInterval))
	term := make([]byte, 16)
	copy(term[12:], "TERM")
	memClearersLeft := s.cores
	for {
		t := <-s.pendingTOCBlockChan
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
					s.tocWriterBytes += offsetB + offsetB/uint64(s.checksumInterval*4)
					if offsetB%uint64(s.checksumInterval) != 0 {
						s.tocWriterBytes += 4
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
					offsetA += 16
					s.tocWriterBytes += offsetA + offsetA/uint64(s.checksumInterval)*4
					if offsetA%uint64(s.checksumInterval) != 0 {
						s.tocWriterBytes += 4
					}
				}
				break
			}
			continue
		}
		if len(t) > 0 {
			timestamp := binary.BigEndian.Uint64(t[4:])
			switch timestamp {
			case timestampA:
				if _, err := writerA.Write(t); err != nil {
					panic(err)
				}
				offsetA += uint64(len(t))
			case timestampB:
				if _, err := writerB.Write(t); err != nil {
					panic(err)
				}
				offsetB += uint64(len(t))
			default:
				// An assumption is made here: If the timestamp for this toc block
				// doesn't match the last two seen timestamps then we expect no
				// more toc blocks for the oldest timestamp and can close that toc
				// file.
				if writerB != nil {
					binary.BigEndian.PutUint64(term[4:], offsetB)
					if _, err := writerB.Write(term); err != nil {
						panic(err)
					}
					if err := writerB.Close(); err != nil {
						panic(err)
					}
					offsetB += 16
					s.tocWriterBytes += offsetB + offsetB/uint64(s.checksumInterval)*4
					if offsetB%uint64(s.checksumInterval) != 0 {
						s.tocWriterBytes += 4
					}
				}
				timestampB = timestampA
				writerB = writerA
				offsetB = offsetA
				timestampA = timestamp
				fp, err := os.Create(fmt.Sprintf("%d.toc", timestamp))
				if err != nil {
					panic(err)
				}
				writerA = brimutil.NewMultiCoreChecksummedWriter(fp, s.checksumInterval, murmur3.New32, s.cores)
				if _, err := writerA.Write(head); err != nil {
					panic(err)
				}
				if _, err := writerA.Write(t); err != nil {
					panic(err)
				}
				offsetA = 32 + uint64(len(t))
			}
		}
		s.freeTOCBlockChan <- t[:0]
	}
	s.tocWriterDoneChan <- struct{}{}
}

type keyLocationBlock interface {
	Timestamp() int64
	Get(r *ReadValue)
}

type memBlock struct {
	store       *Store
	id          uint16
	diskID      uint16
	diskOffset  uint32
	toc         []byte
	data        []byte
	discardLock sync.RWMutex
}

func (m *memBlock) Timestamp() int64 {
	return math.MaxInt64
}

func (m *memBlock) Get(r *ReadValue) {
	m.discardLock.RLock()
	var id uint16
	id, r.offset, r.Seq = m.store.keyLocationMap.get(r.KeyHashA, r.KeyHashB)
	if id < KEY_LOCATION_BLOCK_ID_OFFSET {
		m.discardLock.RUnlock()
		r.ReadChan <- ErrKeyNotFound
	} else if id != m.id {
		m.discardLock.RUnlock()
		m.store.keyLocationBlock(id).Get(r)
	} else {
		if r.offset+4 > uint32(len(m.data)) {
			fmt.Println("A", r.offset, len(m.data))
			m.discardLock.RUnlock()
			r.ReadChan <- ErrKeyNotFound
		} else {
			z := binary.BigEndian.Uint32(m.data[r.offset:])
			if r.offset+4+z > uint32(len(m.data)) {
				fmt.Println("B", r.offset, len(m.data))
				m.discardLock.RUnlock()
				r.ReadChan <- ErrKeyNotFound
			} else {
				r.Value = r.Value[:z]
				copy(r.Value, m.data[r.offset+4:])
				m.discardLock.RUnlock()
				r.ReadChan <- nil
			}
		}
	}
}
