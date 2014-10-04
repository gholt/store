package brimstore

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/gholt/brimutil"
	"github.com/spaolacci/murmur3"
)

type diskBlock struct {
	store          *Store
	id             uint16
	timestamp      int64
	writerFP       io.WriteCloser
	off            int
	freeChan       chan *dbBuf
	checksumChan   chan *dbBuf
	writeChan      chan *dbBuf
	doneChan       chan struct{}
	buf            *dbBuf
	readValueChans []chan *ReadValue
}

type dbBuf struct {
	seq int
	buf []byte
	off int
	mbs []*memBlock
}

func newDiskBlock(s *Store) *diskBlock {
	db := &diskBlock{store: s, timestamp: time.Now().UnixNano()}
	fp, err := os.Create(fmt.Sprintf("%d.values", db.timestamp))
	if err != nil {
		panic(err)
	}
	db.writerFP = fp
	db.freeChan = make(chan *dbBuf, s.cores)
	for i := 0; i < s.cores; i++ {
		db.freeChan <- &dbBuf{buf: make([]byte, s.checksumInterval+4)}
	}
	db.checksumChan = make(chan *dbBuf, s.cores)
	db.writeChan = make(chan *dbBuf, s.cores)
	db.doneChan = make(chan struct{})
	db.buf = <-db.freeChan
	head := []byte("BRIMSTORE VALUES v0             ")
	binary.BigEndian.PutUint32(head[28:], uint32(s.checksumInterval))
	db.buf.off = copy(db.buf.buf, head)
	db.off = db.buf.off
	go db.writer()
	for i := 0; i < s.cores; i++ {
		go db.checksummer()
	}
	db.readValueChans = make([]chan *ReadValue, s.readersPerValuesFile)
	for i := 0; i < len(db.readValueChans); i++ {
		fp, err := os.Open(fmt.Sprintf("%d.values", db.timestamp))
		if err != nil {
			panic(err)
		}
		db.readValueChans[i] = make(chan *ReadValue, s.cores)
		go reader(brimutil.NewChecksummedReader(fp, s.checksumInterval, murmur3.New32), db.readValueChans[i])
	}
	db.id = s.addKeyLocationBlock(db)
	return db
}

func (db *diskBlock) Timestamp() int64 {
	return db.timestamp
}

func (db *diskBlock) Get(r *ReadValue) {
	db.readValueChans[int(r.KeyHashA>>1)%len(db.readValueChans)] <- r
}

func (db *diskBlock) offset() int {
	return db.off
}

func (db *diskBlock) write(mb *memBlock) {
	if mb == nil {
		return
	}
	if len(mb.data) < 1 {
		db.store.clearableMemBlockChan <- mb
		return
	}
	mb.diskID = db.id
	mb.diskOffset = uint32(db.off)
	left := len(mb.data)
	for left > 0 {
		n := copy(db.buf.buf[db.buf.off:db.store.checksumInterval], mb.data[len(mb.data)-left:])
		db.off += n
		db.buf.off += n
		if db.buf.off >= db.store.checksumInterval {
			s := db.buf.seq
			db.checksumChan <- db.buf
			db.buf = <-db.freeChan
			db.buf.seq = s + 1
		}
		left -= n
	}
	if db.buf.off == 0 {
		db.store.clearableMemBlockChan <- mb
	} else {
		db.buf.mbs = append(db.buf.mbs, mb)
	}
}

func reader(cr brimutil.ChecksummedReader, c chan *ReadValue) {
	zb := make([]byte, 4)
	for {
		r := <-c
		cr.Seek(int64(r.offset), 0)
		if _, err := io.ReadFull(cr, zb); err != nil {
			r.ReadChan <- err
		} else {
			z := binary.BigEndian.Uint32(zb)
			r.Value = r.Value[:z]
			if _, err := io.ReadFull(cr, r.Value); err != nil {
				r.ReadChan <- err
			} else {
				r.ReadChan <- nil
			}
		}
	}
}

func (db *diskBlock) close() {
	close(db.checksumChan)
	for i := 0; i < cap(db.checksumChan); i++ {
		<-db.doneChan
	}
	db.writeChan <- nil
	<-db.doneChan
	term := make([]byte, 16)
	binary.BigEndian.PutUint64(term[4:], uint64(db.off))
	copy(term[12:], "TERM")
	left := len(term)
	for left > 0 {
		n := copy(db.buf.buf[db.buf.off:db.store.checksumInterval], term[len(term)-left:])
		db.buf.off += n
		binary.BigEndian.PutUint32(db.buf.buf[db.buf.off:], murmur3.Sum32(db.buf.buf[:db.buf.off]))
		if _, err := db.writerFP.Write(db.buf.buf[:db.buf.off+4]); err != nil {
			panic(err)
		}
		db.store.diskWriterBytes += uint64(db.buf.off) + 4
		db.buf.off = 0
		left -= n
	}
	if err := db.writerFP.Close(); err != nil {
		panic(err)
	}
	for _, bmb := range db.buf.mbs {
		db.store.clearableMemBlockChan <- bmb
	}
	db.writerFP = nil
	db.freeChan = nil
	db.checksumChan = nil
	db.writeChan = nil
	db.doneChan = nil
	db.buf = nil
}

func (db *diskBlock) checksummer() {
	for {
		buf := <-db.checksumChan
		if buf == nil {
			break
		}
		binary.BigEndian.PutUint32(buf.buf[db.store.checksumInterval:], murmur3.Sum32(buf.buf[:db.store.checksumInterval]))
		db.writeChan <- buf
	}
	db.doneChan <- struct{}{}
}

func (db *diskBlock) writer() {
	var seq int
	lastWasNil := false
	for {
		buf := <-db.writeChan
		if buf == nil {
			if lastWasNil {
				break
			}
			lastWasNil = true
			db.writeChan <- nil
			continue
		}
		lastWasNil = false
		if buf.seq != seq {
			db.writeChan <- buf
			continue
		}
		if _, err := db.writerFP.Write(buf.buf); err != nil {
			panic(err)
		}
		db.store.diskWriterBytes += uint64(db.store.checksumInterval) + 4
		if len(buf.mbs) > 0 {
			for _, bmb := range buf.mbs {
				db.store.clearableMemBlockChan <- bmb
			}
			buf.mbs = buf.mbs[:0]
		}
		buf.off = 0
		db.freeChan <- buf
		seq++
	}
	db.doneChan <- struct{}{}
}
