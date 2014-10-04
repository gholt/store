package valuesstore

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/gholt/brimutil"
	"github.com/spaolacci/murmur3"
)

type valuesFile struct {
	vs             *ValuesStore
	id             uint16
	ts             int64
	writerFP       io.WriteCloser
	atOffset       uint32
	freeChan       chan *wbuf
	checksumChan   chan *wbuf
	writeChan      chan *wbuf
	doneChan       chan struct{}
	buf            *wbuf
	readValueChans []chan *ReadValue
}

type wbuf struct {
	seq    int
	buf    []byte
	offset uint32
	mbs    []*memBlock
}

func newValuesFile(vs *ValuesStore) *valuesFile {
	vf := &valuesFile{vs: vs, ts: time.Now().UnixNano()}
	fp, err := os.Create(fmt.Sprintf("%d.values", vf.ts))
	if err != nil {
		panic(err)
	}
	vf.writerFP = fp
	vf.freeChan = make(chan *wbuf, vs.cores)
	for i := 0; i < vs.cores; i++ {
		vf.freeChan <- &wbuf{buf: make([]byte, vs.checksumInterval+4)}
	}
	vf.checksumChan = make(chan *wbuf, vs.cores)
	vf.writeChan = make(chan *wbuf, vs.cores)
	vf.doneChan = make(chan struct{})
	vf.buf = <-vf.freeChan
	head := []byte("BRIMSTORE VALUES v0             ")
	binary.BigEndian.PutUint32(head[28:], vs.checksumInterval)
	vf.buf.offset = uint32(copy(vf.buf.buf, head))
	atomic.StoreUint32(&vf.atOffset, vf.buf.offset)
	go vf.writer()
	for i := 0; i < vs.cores; i++ {
		go vf.checksummer()
	}
	vf.readValueChans = make([]chan *ReadValue, vs.readersPerValuesFile)
	for i := 0; i < len(vf.readValueChans); i++ {
		fp, err := os.Open(fmt.Sprintf("%d.values", vf.ts))
		if err != nil {
			panic(err)
		}
		vf.readValueChans[i] = make(chan *ReadValue, vs.cores)
		go reader(brimutil.NewChecksummedReader(fp, int(vs.checksumInterval), murmur3.New32), vf.readValueChans[i])
	}
	vf.id = vs.addValuesLocBock(vf)
	return vf
}

func (vf *valuesFile) timestamp() int64 {
	return vf.ts
}

func (vf *valuesFile) readValue(r *ReadValue) {
	vf.readValueChans[int(r.KeyA>>1)%len(vf.readValueChans)] <- r
}

func (vf *valuesFile) write(mb *memBlock) {
	if mb == nil {
		return
	}
	if len(mb.data) < 1 {
		vf.vs.clearableMemBlockChan <- mb
		return
	}
	mb.vfID = vf.id
	mb.vfOffset = atomic.LoadUint32(&vf.atOffset)
	left := len(mb.data)
	for left > 0 {
		n := copy(vf.buf.buf[vf.buf.offset:vf.vs.checksumInterval], mb.data[len(mb.data)-left:])
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
		vf.vs.clearableMemBlockChan <- mb
	} else {
		vf.buf.mbs = append(vf.buf.mbs, mb)
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

func (vf *valuesFile) close() {
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
			panic(err)
		}
		vf.buf.offset = 0
		left -= n
	}
	if err := vf.writerFP.Close(); err != nil {
		panic(err)
	}
	for _, bmb := range vf.buf.mbs {
		vf.vs.clearableMemBlockChan <- bmb
	}
	vf.writerFP = nil
	vf.freeChan = nil
	vf.checksumChan = nil
	vf.writeChan = nil
	vf.doneChan = nil
	vf.buf = nil
}

func (vf *valuesFile) checksummer() {
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

func (vf *valuesFile) writer() {
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
			panic(err)
		}
		if len(buf.mbs) > 0 {
			for _, bmb := range buf.mbs {
				vf.vs.clearableMemBlockChan <- bmb
			}
			buf.mbs = buf.mbs[:0]
		}
		buf.offset = 0
		vf.freeChan <- buf
		seq++
	}
	vf.doneChan <- struct{}{}
}
