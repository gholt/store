package store

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

//    "GROUPSTORETOC v0            ":28, checksumInterval:4
// or "GROUPSTORE v0               ":28, checksumInterval:4
const _GROUP_FILE_HEADER_SIZE = 32

// keyA:8, keyB:8, nameKeyA:8, nameKeyB:8, timestamp:8, offset:4, length:4
const _GROUP_FILE_ENTRY_SIZE = 48

// 0:4, offsetWhereTrailerOccurs:4, "TERM":4
const _GROUP_FILE_TRAILER_SIZE = 16

type groupStoreFile struct {
	store                     *DefaultGroupStore
	name                      string
	id                        uint32
	bts                       int64
	writerFP                  io.WriteCloser
	atOffset                  uint32
	freeChan                  chan *groupStoreFileWriteBuf
	checksumChan              chan *groupStoreFileWriteBuf
	writeChan                 chan *groupStoreFileWriteBuf
	doneChan                  chan struct{}
	buf                       *groupStoreFileWriteBuf
	freeableMemBlockChanIndex int
	readerFPs                 []brimutil.ChecksummedReader
	readerLocks               []sync.Mutex
	readerLens                [][]byte
}

type groupStoreFileWriteBuf struct {
	seq       int
	buf       []byte
	offset    uint32
	memBlocks []*groupMemBlock
}

func newGroupFile(store *DefaultGroupStore, bts int64, openReadSeeker func(name string) (io.ReadSeeker, error)) (*groupStoreFile, error) {
	fl := &groupStoreFile{store: store, bts: bts}
	fl.name = path.Join(store.path, fmt.Sprintf("%019d.group", fl.bts))
	fl.readerFPs = make([]brimutil.ChecksummedReader, store.fileReaders)
	fl.readerLocks = make([]sync.Mutex, len(fl.readerFPs))
	fl.readerLens = make([][]byte, len(fl.readerFPs))
	for i := 0; i < len(fl.readerFPs); i++ {
		fp, err := openReadSeeker(fl.name)
		if err != nil {
			return nil, err
		}
		fl.readerFPs[i] = brimutil.NewChecksummedReader(fp, int(store.checksumInterval), murmur3.New32)
		fl.readerLens[i] = make([]byte, 4)
	}
	var err error
	fl.id, err = store.addLocBlock(fl)
	if err != nil {
		fl.close()
		return nil, err
	}
	return fl, nil
}

func createGroupFile(store *DefaultGroupStore, createWriteCloser func(name string) (io.WriteCloser, error), openReadSeeker func(name string) (io.ReadSeeker, error)) (*groupStoreFile, error) {
	fl := &groupStoreFile{store: store, bts: time.Now().UnixNano()}
	fl.name = path.Join(store.path, fmt.Sprintf("%019d.group", fl.bts))
	fp, err := createWriteCloser(fl.name)
	if err != nil {
		return nil, err
	}
	fl.writerFP = fp
	fl.freeChan = make(chan *groupStoreFileWriteBuf, store.workers)
	for i := 0; i < store.workers; i++ {
		fl.freeChan <- &groupStoreFileWriteBuf{buf: make([]byte, store.checksumInterval+4)}
	}
	fl.checksumChan = make(chan *groupStoreFileWriteBuf, store.workers)
	fl.writeChan = make(chan *groupStoreFileWriteBuf, store.workers)
	fl.doneChan = make(chan struct{})
	fl.buf = <-fl.freeChan
	head := []byte("GROUPSTORE v0                   ")
	binary.BigEndian.PutUint32(head[28:], store.checksumInterval)
	fl.buf.offset = uint32(copy(fl.buf.buf, head))
	atomic.StoreUint32(&fl.atOffset, fl.buf.offset)
	go fl.writer()
	for i := 0; i < store.workers; i++ {
		go fl.checksummer()
	}
	fl.readerFPs = make([]brimutil.ChecksummedReader, store.fileReaders)
	fl.readerLocks = make([]sync.Mutex, len(fl.readerFPs))
	fl.readerLens = make([][]byte, len(fl.readerFPs))
	for i := 0; i < len(fl.readerFPs); i++ {
		fp, err := openReadSeeker(fl.name)
		if err != nil {
			fl.writerFP.Close()
			for j := 0; j < i; j++ {
				fl.readerFPs[j].Close()
			}
			return nil, err
		}
		fl.readerFPs[i] = brimutil.NewChecksummedReader(fp, int(store.checksumInterval), murmur3.New32)
		fl.readerLens[i] = make([]byte, 4)
	}
	fl.id, err = store.addLocBlock(fl)
	if err != nil {
		return nil, err
	}
	return fl, nil
}

func (fl *groupStoreFile) timestampnano() int64 {
	return fl.bts
}

func (fl *groupStoreFile) read(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, timestampbits uint64, offset uint32, length uint32, value []byte) (uint64, []byte, error) {
	// TODO: Add calling Verify occasionally on the readerFPs, maybe randomly
	// inside here or maybe randomly requested by the caller.
	if timestampbits&_TSB_DELETION != 0 {
		return timestampbits, value, ErrNotFound
	}
	i := int(keyA>>1) % len(fl.readerFPs)
	fl.readerLocks[i].Lock()
	fl.readerFPs[i].Seek(int64(offset), 0)
	end := len(value) + int(length)
	if end <= cap(value) {
		value = value[:end]
	} else {
		value2 := make([]byte, end)
		copy(value2, value)
		value = value2
	}
	if _, err := io.ReadFull(fl.readerFPs[i], value[len(value)-int(length):]); err != nil {
		fl.readerLocks[i].Unlock()
		return timestampbits, value, err
	}
	fl.readerLocks[i].Unlock()
	return timestampbits, value, nil
}

func (fl *groupStoreFile) write(memBlock *groupMemBlock) {
	if memBlock == nil {
		return
	}
	memBlock.fileID = fl.id
	memBlock.fileOffset = atomic.LoadUint32(&fl.atOffset)
	if len(memBlock.values) < 1 {
		fl.store.freeableMemBlockChans[fl.freeableMemBlockChanIndex] <- memBlock
		fl.freeableMemBlockChanIndex++
		if fl.freeableMemBlockChanIndex >= len(fl.store.freeableMemBlockChans) {
			fl.freeableMemBlockChanIndex = 0
		}
		return
	}
	left := len(memBlock.values)
	for left > 0 {
		n := copy(fl.buf.buf[fl.buf.offset:fl.store.checksumInterval], memBlock.values[len(memBlock.values)-left:])
		atomic.AddUint32(&fl.atOffset, uint32(n))
		fl.buf.offset += uint32(n)
		if fl.buf.offset >= fl.store.checksumInterval {
			s := fl.buf.seq
			fl.checksumChan <- fl.buf
			fl.buf = <-fl.freeChan
			fl.buf.seq = s + 1
		}
		left -= n
	}
	if fl.buf.offset == 0 {
		fl.store.freeableMemBlockChans[fl.freeableMemBlockChanIndex] <- memBlock
		fl.freeableMemBlockChanIndex++
		if fl.freeableMemBlockChanIndex >= len(fl.store.freeableMemBlockChans) {
			fl.freeableMemBlockChanIndex = 0
		}
	} else {
		fl.buf.memBlocks = append(fl.buf.memBlocks, memBlock)
	}
}

func (fl *groupStoreFile) closeWriting() error {
	if fl.checksumChan == nil {
		return nil
	}
	var reterr error
	close(fl.checksumChan)
	for i := 0; i < cap(fl.checksumChan); i++ {
		<-fl.doneChan
	}
	fl.writeChan <- nil
	<-fl.doneChan
	term := make([]byte, 16)
	binary.BigEndian.PutUint64(term[4:], uint64(atomic.LoadUint32(&fl.atOffset)))
	copy(term[12:], "TERM")
	left := len(term)
	for left > 0 {
		n := copy(fl.buf.buf[fl.buf.offset:fl.store.checksumInterval], term[len(term)-left:])
		fl.buf.offset += uint32(n)
		binary.BigEndian.PutUint32(fl.buf.buf[fl.buf.offset:], murmur3.Sum32(fl.buf.buf[:fl.buf.offset]))
		if _, err := fl.writerFP.Write(fl.buf.buf[:fl.buf.offset+4]); err != nil {
			if reterr == nil {
				reterr = err
			}
			break
		}
		fl.buf.offset = 0
		left -= n
	}
	if err := fl.writerFP.Close(); err != nil {
		if reterr == nil {
			reterr = err
		}
	}
	for _, memBlock := range fl.buf.memBlocks {
		fl.store.freeableMemBlockChans[fl.freeableMemBlockChanIndex] <- memBlock
		fl.freeableMemBlockChanIndex++
		if fl.freeableMemBlockChanIndex >= len(fl.store.freeableMemBlockChans) {
			fl.freeableMemBlockChanIndex = 0
		}
	}
	fl.writerFP = nil
	fl.freeChan = nil
	fl.checksumChan = nil
	fl.writeChan = nil
	fl.doneChan = nil
	fl.buf = nil
	return reterr
}

func (fl *groupStoreFile) close() error {
	reterr := fl.closeWriting()
	for i, fp := range fl.readerFPs {
		// This will let any ongoing reads complete.
		fl.readerLocks[i].Lock()
		if err := fp.Close(); err != nil {
			if reterr == nil {
				reterr = err
			}
		}
		// This will release any pending reads, which will get errors
		// immediately. Essentially, there is a race between compaction
		// accomplishing its goal of rewriting all entries of a file to a new
		// file, and readers of those entries beginning to use the new entry
		// locations. It's a small window and the resulting errors should be
		// fairly few and easily recoverable on a re-read.
		fl.readerLocks[i].Unlock()
	}
	return reterr
}

func (fl *groupStoreFile) checksummer() {
	for {
		buf := <-fl.checksumChan
		if buf == nil {
			break
		}
		binary.BigEndian.PutUint32(buf.buf[fl.store.checksumInterval:], murmur3.Sum32(buf.buf[:fl.store.checksumInterval]))
		fl.writeChan <- buf
	}
	fl.doneChan <- struct{}{}
}

func (fl *groupStoreFile) writer() {
	var seq int
	lastWasNil := false
	for {
		buf := <-fl.writeChan
		if buf == nil {
			if lastWasNil {
				break
			}
			lastWasNil = true
			fl.writeChan <- nil
			continue
		}
		lastWasNil = false
		if buf.seq != seq {
			fl.writeChan <- buf
			continue
		}
		if _, err := fl.writerFP.Write(buf.buf); err != nil {
			fl.store.logCritical("%s %s\n", fl.name, err)
			break
		}
		if len(buf.memBlocks) > 0 {
			for _, memBlock := range buf.memBlocks {
				fl.store.freeableMemBlockChans[fl.freeableMemBlockChanIndex] <- memBlock
				fl.freeableMemBlockChanIndex++
				if fl.freeableMemBlockChanIndex >= len(fl.store.freeableMemBlockChans) {
					fl.freeableMemBlockChanIndex = 0
				}
			}
			buf.memBlocks = buf.memBlocks[:0]
		}
		buf.offset = 0
		fl.freeChan <- buf
		seq++
	}
	fl.doneChan <- struct{}{}
}
