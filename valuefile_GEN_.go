package valuestore

import (
	"encoding/binary"
	"fmt"
	"io"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spaolacci/murmur3"
	"gopkg.in/gholt/brimutil.v1"
)

//    "VALUESTORETOC v0            ":28, checksumInterval:4
// or "VALUESTORE v0               ":28, checksumInterval:4
const _VALUE_FILE_HEADER_SIZE = 32

// keyA:8, keyB:8, timestamp:8, offset:4, length:4
const _VALUE_FILE_ENTRY_SIZE = 32

// 0:4, offsetWhereTrailerOccurs:4, "TERM":4
const _VALUE_FILE_TRAILER_SIZE = 16

type valueFile struct {
	vs                  *DefaultValueStore
	name                string
	id                  uint32
	bts                 int64
	writerFP            io.WriteCloser
	atOffset            uint32
	freeChan            chan *valueFileWriteBuf
	checksumChan        chan *valueFileWriteBuf
	writeChan           chan *valueFileWriteBuf
	doneChan            chan struct{}
	buf                 *valueFileWriteBuf
	freeableVMChanIndex int
	readerFPs           []brimutil.ChecksummedReader
	readerLocks         []sync.Mutex
	readerLens          [][]byte
}

type valueFileWriteBuf struct {
	seq    int
	buf    []byte
	offset uint32
	vms    []*valueMem
}

func newValueFile(vs *DefaultValueStore, bts int64, openReadSeeker func(name string) (io.ReadSeeker, error)) (*valueFile, error) {
	vf := &valueFile{vs: vs, bts: bts}
	vf.name = path.Join(vs.path, fmt.Sprintf("%019d.values", vf.bts))
	vf.readerFPs = make([]brimutil.ChecksummedReader, vs.fileReaders)
	vf.readerLocks = make([]sync.Mutex, len(vf.readerFPs))
	vf.readerLens = make([][]byte, len(vf.readerFPs))
	for i := 0; i < len(vf.readerFPs); i++ {
		fp, err := openReadSeeker(vf.name)
		if err != nil {
			return nil, err
		}
		vf.readerFPs[i] = brimutil.NewChecksummedReader(fp, int(vs.checksumInterval), murmur3.New32)
		vf.readerLens[i] = make([]byte, 4)
	}
	var err error
	vf.id, err = vs.addLocBlock(vf)
	if err != nil {
		vf.close()
		return nil, err
	}
	return vf, nil
}

func createValueFile(vs *DefaultValueStore, createWriteCloser func(name string) (io.WriteCloser, error), openReadSeeker func(name string) (io.ReadSeeker, error)) (*valueFile, error) {
	vf := &valueFile{vs: vs, bts: time.Now().UnixNano()}
	vf.name = path.Join(vs.path, fmt.Sprintf("%019d.values", vf.bts))
	fp, err := createWriteCloser(vf.name)
	if err != nil {
		return nil, err
	}
	vf.writerFP = fp
	vf.freeChan = make(chan *valueFileWriteBuf, vs.workers)
	for i := 0; i < vs.workers; i++ {
		vf.freeChan <- &valueFileWriteBuf{buf: make([]byte, vs.checksumInterval+4)}
	}
	vf.checksumChan = make(chan *valueFileWriteBuf, vs.workers)
	vf.writeChan = make(chan *valueFileWriteBuf, vs.workers)
	vf.doneChan = make(chan struct{})
	vf.buf = <-vf.freeChan
	head := []byte("VALUESTORE v0                   ")
	binary.BigEndian.PutUint32(head[28:], vs.checksumInterval)
	vf.buf.offset = uint32(copy(vf.buf.buf, head))
	atomic.StoreUint32(&vf.atOffset, vf.buf.offset)
	go vf.writer()
	for i := 0; i < vs.workers; i++ {
		go vf.checksummer()
	}
	vf.readerFPs = make([]brimutil.ChecksummedReader, vs.fileReaders)
	vf.readerLocks = make([]sync.Mutex, len(vf.readerFPs))
	vf.readerLens = make([][]byte, len(vf.readerFPs))
	for i := 0; i < len(vf.readerFPs); i++ {
		fp, err := openReadSeeker(vf.name)
		if err != nil {
			vf.writerFP.Close()
			for j := 0; j < i; j++ {
				vf.readerFPs[j].Close()
			}
			return nil, err
		}
		vf.readerFPs[i] = brimutil.NewChecksummedReader(fp, int(vs.checksumInterval), murmur3.New32)
		vf.readerLens[i] = make([]byte, 4)
	}
	vf.id, err = vs.addLocBlock(vf)
	if err != nil {
		return nil, err
	}
	return vf, nil
}

func (vf *valueFile) timestampnano() int64 {
	return vf.bts
}

// TODO: nameKey needs to go all throughout the code.
func (vf *valueFile) read(keyA uint64, keyB uint64, timestampbits uint64, offset uint32, length uint32, value []byte) (uint64, []byte, error) {
	// TODO: Add calling Verify occasionally on the readerFPs, maybe randomly
	// inside here or maybe randomly requested by the caller.
	if timestampbits&_TSB_DELETION != 0 {
		return timestampbits, value, ErrNotFound
	}
	i := int(keyA>>1) % len(vf.readerFPs)
	vf.readerLocks[i].Lock()
	vf.readerFPs[i].Seek(int64(offset), 0)
	end := len(value) + int(length)
	if end <= cap(value) {
		value = value[:end]
	} else {
		value2 := make([]byte, end)
		copy(value2, value)
		value = value2
	}
	if _, err := io.ReadFull(vf.readerFPs[i], value[len(value)-int(length):]); err != nil {
		vf.readerLocks[i].Unlock()
		return timestampbits, value, err
	}
	vf.readerLocks[i].Unlock()
	return timestampbits, value, nil
}

func (vf *valueFile) write(vm *valueMem) {
	if vm == nil {
		return
	}
	vm.vfID = vf.id
	vm.vfOffset = atomic.LoadUint32(&vf.atOffset)
	if len(vm.values) < 1 {
		vf.vs.freeableVMChans[vf.freeableVMChanIndex] <- vm
		vf.freeableVMChanIndex++
		if vf.freeableVMChanIndex >= len(vf.vs.freeableVMChans) {
			vf.freeableVMChanIndex = 0
		}
		return
	}
	left := len(vm.values)
	for left > 0 {
		n := copy(vf.buf.buf[vf.buf.offset:vf.vs.checksumInterval], vm.values[len(vm.values)-left:])
		atomic.AddUint32(&vf.atOffset, uint32(n))
		vf.buf.offset += uint32(n)
		if vf.buf.offset >= vf.vs.checksumInterval {
			s := vf.buf.seq
			vf.checksumChan <- vf.buf
			vf.buf = <-vf.freeChan
			vf.buf.seq = s + 1
		}
		left -= n
	}
	if vf.buf.offset == 0 {
		vf.vs.freeableVMChans[vf.freeableVMChanIndex] <- vm
		vf.freeableVMChanIndex++
		if vf.freeableVMChanIndex >= len(vf.vs.freeableVMChans) {
			vf.freeableVMChanIndex = 0
		}
	} else {
		vf.buf.vms = append(vf.buf.vms, vm)
	}
}

func (vf *valueFile) close() error {
	var reterr error
	close(vf.checksumChan)
	for i := 0; i < cap(vf.checksumChan); i++ {
		<-vf.doneChan
	}
	vf.writeChan <- nil
	<-vf.doneChan
	term := make([]byte, 16)
	binary.BigEndian.PutUint64(term[4:], uint64(atomic.LoadUint32(&vf.atOffset)))
	copy(term[12:], "TERM")
	left := len(term)
	for left > 0 {
		n := copy(vf.buf.buf[vf.buf.offset:vf.vs.checksumInterval], term[len(term)-left:])
		vf.buf.offset += uint32(n)
		binary.BigEndian.PutUint32(vf.buf.buf[vf.buf.offset:], murmur3.Sum32(vf.buf.buf[:vf.buf.offset]))
		if _, err := vf.writerFP.Write(vf.buf.buf[:vf.buf.offset+4]); err != nil {
			if reterr == nil {
				reterr = err
			}
			break
		}
		vf.buf.offset = 0
		left -= n
	}
	if err := vf.writerFP.Close(); err != nil {
		if reterr == nil {
			reterr = err
		}
	}
	for _, vm := range vf.buf.vms {
		vf.vs.freeableVMChans[vf.freeableVMChanIndex] <- vm
		vf.freeableVMChanIndex++
		if vf.freeableVMChanIndex >= len(vf.vs.freeableVMChans) {
			vf.freeableVMChanIndex = 0
		}
	}
	vf.writerFP = nil
	vf.freeChan = nil
	vf.checksumChan = nil
	vf.writeChan = nil
	vf.doneChan = nil
	vf.buf = nil
	for i, fp := range vf.readerFPs {
		// This will let any ongoing reads complete.
		vf.readerLocks[i].Lock()
		fp.Close()
		// This will release any pending reads, which will get errors
		// immediately. Essentially, there is a race between compaction
		// accomplishing its goal of rewriting all entries of a file to a new
		// file, and readers of those entries beginning to use the new entry
		// locations. It's a small window and the resulting errors should be
		// fairly few and easily recoverable on a re-read.
		vf.readerLocks[i].Unlock()
	}
	return reterr
}

func (vf *valueFile) checksummer() {
	for {
		buf := <-vf.checksumChan
		if buf == nil {
			break
		}
		binary.BigEndian.PutUint32(buf.buf[vf.vs.checksumInterval:], murmur3.Sum32(buf.buf[:vf.vs.checksumInterval]))
		vf.writeChan <- buf
	}
	vf.doneChan <- struct{}{}
}

func (vf *valueFile) writer() {
	var seq int
	lastWasNil := false
	for {
		buf := <-vf.writeChan
		if buf == nil {
			if lastWasNil {
				break
			}
			lastWasNil = true
			vf.writeChan <- nil
			continue
		}
		lastWasNil = false
		if buf.seq != seq {
			vf.writeChan <- buf
			continue
		}
		if _, err := vf.writerFP.Write(buf.buf); err != nil {
			vf.vs.logCritical("%s %s\n", vf.name, err)
			break
		}
		if len(buf.vms) > 0 {
			for _, vm := range buf.vms {
				vf.vs.freeableVMChans[vf.freeableVMChanIndex] <- vm
				vf.freeableVMChanIndex++
				if vf.freeableVMChanIndex >= len(vf.vs.freeableVMChans) {
					vf.freeableVMChanIndex = 0
				}
			}
			buf.vms = buf.vms[:0]
		}
		buf.offset = 0
		vf.freeChan <- buf
		seq++
	}
	vf.doneChan <- struct{}{}
}
