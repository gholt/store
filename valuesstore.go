package brimstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gholt/brimutil"
	"github.com/spaolacci/murmur3"
)

var ErrValueNotFound error = errors.New("value not found")

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

// NewValuesStore creates a ValuesStore for use in storing []byte values
// referenced by 128 bit keys; opts may be nil to use the defaults.
//
// Note that a lot of buffering and multiple cores can be in use and Close
// should be called prior to the process exiting to ensure all processing is
// done and the buffers are flushed.
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
	if memTOCPageSize < checksumInterval/2+1 {
		memTOCPageSize = checksumInterval/2 + 1
	}
	if memValuesPageSize < checksumInterval/2+1 {
		memValuesPageSize = checksumInterval/2 + 1
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
	vs.freeableVMChan = make(chan *valuesMem, vs.cores)
	vs.freeVMChan = make(chan *valuesMem, vs.cores*2)
	vs.freeVWRChans = make([]chan *valueWriteReq, vs.cores)
	vs.pendingVWRChans = make([]chan *valueWriteReq, vs.cores)
	vs.vfVMChan = make(chan *valuesMem, vs.cores)
	vs.freeTOCBlockChan = make(chan []byte, vs.cores)
	vs.pendingTOCBlockChan = make(chan []byte, vs.cores)
	vs.tocWriterDoneChan = make(chan struct{}, 1)
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
		vs.freeVWRChans[i] = make(chan *valueWriteReq, vs.cores)
		for j := 0; j < vs.cores; j++ {
			vs.freeVWRChans[i] <- &valueWriteReq{errChan: make(chan error, 1)}
		}
	}
	for i := 0; i < len(vs.pendingVWRChans); i++ {
		vs.pendingVWRChans[i] = make(chan *valueWriteReq)
	}
	for i := 0; i < cap(vs.freeTOCBlockChan); i++ {
		vs.freeTOCBlockChan <- make([]byte, 0, vs.memTOCPageSize)
	}
	for i := 0; i < cap(vs.pendingTOCBlockChan); i++ {
		vs.pendingTOCBlockChan <- make([]byte, 0, vs.memTOCPageSize)
	}
	go vs.tocWriter()
	go vs.vfWriter()
	for i := 0; i < vs.cores; i++ {
		go vs.memClearer()
	}
	for i := 0; i < len(vs.pendingVWRChans); i++ {
		go vs.memWriter(vs.pendingVWRChans[i])
	}
	//fp, err := os.Open("1412518284291556122.toc")
	//if err != nil {
	//    panic(err)
	//}
	//buf := make([]byte, vs.checksumInterval + 4)
	//first := true
	//for {
	//    n, err := io.ReadFull(fp, buf)
	//    if n < 4 {
	//        if err != io.EOF || err != io.ErrUnexpectedEOF {
	//            fmt.Println(err)
	//        }
	//        break
	//    }
	//    n-=4
	//    c := murmur3.Sum32(buf[:n])
	//    if c == binary.BigEndian.Uint32(buf[n:]) {
	//        i := 0
	//        if first {
	//            i += 32
	//            first = false
	//        }
	//        for ;i+28 < n;i+=28 {
	//            offset := binary.BigEndian.Uint32(buf[i:])
	//            a := binary.BigEndian.Uint64(buf[i+4:])
	//            b := binary.BigEndian.Uint64(buf[i+12:])
	//            q := binary.BigEndian.Uint64(buf[i+20:])
	//            vs.vlm.set(10, offset, a, b, q)
	//        }
	//    } else {
	//        fmt.Println("checksum fail")
	//    }
	//    if err == io.EOF || err == io.ErrUnexpectedEOF {
	//        break
	//    }
	//    if err != nil {
	//        fmt.Println(err)
	//        break
	//    }
	//}
	//fp.Close()
	return vs
}

func (vs *ValuesStore) MaxValueSize() int {
	return vs.maxValueSize
}

func (vs *ValuesStore) Close() {
	for _, c := range vs.pendingVWRChans {
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
	i := int(keyA>>1) % len(vs.freeVWRChans)
	vwr := <-vs.freeVWRChans[i]
	vwr.keyA = keyA
	vwr.keyB = keyB
	vwr.value = value
	vwr.seq = seq
	vs.pendingVWRChans[i] <- vwr
	err := <-vwr.errChan
	vwr.value = nil
	vs.freeVWRChans[i] <- vwr
	return err
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

func (vs *ValuesStore) memClearer() {
	var tb []byte
	var tbTS int64
	var tbOffset int
	for {
		vm := <-vs.freeableVMChan
		if vm == nil {
			if tb != nil {
				binary.BigEndian.PutUint32(tb, uint32(len(tb)-4))
				vs.pendingTOCBlockChan <- tb
				<-vs.freeTOCBlockChan
			}
			vs.pendingTOCBlockChan <- nil
			<-vs.freeTOCBlockChan
			break
		}
		vf := vs.valuesLocBlock(vm.vfID)
		if tb != nil && tbTS != vf.timestamp() {
			vs.pendingTOCBlockChan <- tb
			tb = nil
		}
		for vmTOCOffset := 0; vmTOCOffset < len(vm.toc); vmTOCOffset += 28 {
			vmMemOffset := binary.BigEndian.Uint32(vm.toc[vmTOCOffset:])
			a := binary.BigEndian.Uint64(vm.toc[vmTOCOffset+4:])
			b := binary.BigEndian.Uint64(vm.toc[vmTOCOffset+12:])
			q := binary.BigEndian.Uint64(vm.toc[vmTOCOffset+20:])
			vs.vlm.set(vm.vfID, vm.vfOffset+vmMemOffset, a, b, q)
			if tb != nil && tbOffset+28 > cap(tb) {
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
			tb = tb[:tbOffset+28]
			binary.BigEndian.PutUint32(tb[tbOffset:], vm.vfOffset+uint32(vmMemOffset))
			binary.BigEndian.PutUint64(tb[tbOffset+4:], a)
			binary.BigEndian.PutUint64(tb[tbOffset+12:], b)
			binary.BigEndian.PutUint64(tb[tbOffset+20:], q)
			tbOffset += 28
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

func (vs *ValuesStore) memWriter(VWRChan chan *valueWriteReq) {
	var vm *valuesMem
	var vmTOCOffset int
	var vmMemOffset int
	for {
		vwr := <-VWRChan
		if vwr == nil {
			if vm != nil && len(vm.toc) > 0 {
				vs.vfVMChan <- vm
				<-vs.freeVMChan
			}
			vs.vfVMChan <- nil
			<-vs.freeVMChan
			break
		}
		vz := len(vwr.value)
		if vz > vs.maxValueSize {
			vwr.errChan <- fmt.Errorf("value length of %d > %d", vz, vs.maxValueSize)
			continue
		}
		if vm != nil && (vmTOCOffset+28 > cap(vm.toc) || vmMemOffset+4+vz > cap(vm.values)) {
			vs.vfVMChan <- vm
			vm = nil
		}
		if vm == nil {
			vm = <-vs.freeVMChan
			vmTOCOffset = 0
			vmMemOffset = 0
		}
		vm.toc = vm.toc[:vmTOCOffset+28]
		binary.BigEndian.PutUint32(vm.toc[vmTOCOffset:], uint32(vmMemOffset))
		binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+4:], vwr.keyA)
		binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+12:], vwr.keyB)
		binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+20:], vwr.seq)
		vmTOCOffset += 28
		vm.discardLock.Lock()
		vm.values = vm.values[:vmMemOffset+4+vz]
		vm.discardLock.Unlock()
		binary.BigEndian.PutUint32(vm.values[vmMemOffset:], uint32(vz))
		copy(vm.values[vmMemOffset+4:], vwr.value)
		vs.vlm.set(vm.id, uint32(vmMemOffset), vwr.keyA, vwr.keyB, vwr.seq)
		vmMemOffset += 4 + vz
		vwr.errChan <- nil
	}
}

func (vs *ValuesStore) vfWriter() {
	var vf *valuesFile
	memWritersLeft := vs.cores
	for {
		vm := <-vs.vfVMChan
		if vm == nil {
			memWritersLeft--
			if memWritersLeft < 1 {
				if vf != nil {
					vf.close()
				}
				for i := 0; i <= vs.cores; i++ {
					vs.freeableVMChan <- nil
				}
				break
			}
			continue
		}
		if vf != nil && int(atomic.LoadUint32(&vf.atOffset))+len(vm.values) > vs.valuesFileSize {
			vf.close()
			vf = nil
		}
		if vf == nil {
			vf = newValuesFile(vs)
		}
		vf.write(vm)
	}
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
			ts := binary.BigEndian.Uint64(t)
			switch ts {
			case tsA:
				if _, err := writerA.Write(t[8:]); err != nil {
					panic(err)
				}
				offsetA += uint64(len(t) - 8)
			case tsB:
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

type valueWriteReq struct {
	keyA    uint64
	keyB    uint64
	value   []byte
	seq     uint64
	errChan chan error
}

type valuesLocBlock interface {
	timestamp() int64
	readValue(keyA uint64, keyB uint64, value []byte, seq uint64, offset uint32) ([]byte, uint64, error)
}
