package valuestore

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/gholt/experimental-ring"
)

const _GLH_IN_BULK_SET_MSGS = 128
const _GLH_IN_BULK_SET_HANDLERS = 40
const _GLH_OUT_BULK_SET_MSGS = 128
const _GLH_OUT_BULK_SET_MSG_SIZE = 16 * 1024 * 1024
const _GLH_IN_BULK_SET_MSG_TIMEOUT = 300

type bulkSetMsg struct {
	vs     *DefaultValueStore
	header []byte
	body   []byte
}

func (vs *DefaultValueStore) inBulkSet() {
	for {
		bsm := <-vs.inBulkSetChan
		var bsam *bulkSetAckMsg
		if bsm.nodeID() != 0 {
			bsam = vs.newOutBulkSetAckMsg()
		}
		body := bsm.body
		var err error
		rid := vs.ring.ID()
		sppower := 64 - vs.ring.PartitionPower()
		for len(body) > 0 {
			keyA := binary.BigEndian.Uint64(body)
			keyB := binary.BigEndian.Uint64(body[8:])
			timestampbits := binary.BigEndian.Uint64(body[16:])
			l := binary.BigEndian.Uint32(body[24:])
			_, err = vs.write(keyA, keyB, timestampbits, body[28:28+l])
			if bsam != nil && err == nil {
				if rid != vs.ring.ID() {
					rid = vs.ring.ID()
					sppower = 64 - vs.ring.PartitionPower()
				}
				if vs.ring.Responsible(uint32(keyA >> sppower)) {
					bsam.add(keyA, keyB, timestampbits)
				}
			}
			body = body[28+l:]
		}
		if bsam != nil {
			if !vs.ring.MsgToNode(bsm.nodeID(), bsam) {
				bsam.Done()
			}
		}
		vs.freeInBulkSetChan <- bsm
	}
}

func (vs *DefaultValueStore) newInBulkSetMsg(r io.Reader, l uint64) (uint64, error) {
	var bsm *bulkSetMsg
	select {
	case bsm = <-vs.freeInBulkSetChan:
	case <-time.After(_GLH_IN_BULK_SET_MSG_TIMEOUT * time.Second):
		var n uint64
		var sn int
		var err error
		for n < l {
			sn, err = r.Read(toss)
			n += uint64(sn)
			if err != nil {
				return n, err
			}
		}
		return n, nil
	}
	if l < 8 {
		var n uint64
		var sn int
		var err error
		for n < l {
			sn, err = r.Read(toss)
			n += uint64(sn)
			if err != nil {
				return n, err
			}
		}
		return n, nil
	}
	var n int
	var sn int
	var err error
	for n != 8 {
		sn, err = r.Read(bsm.header[n:])
		n += sn
		if err != nil {
			return uint64(n), err
		}
	}
	l -= 8
	if l > uint64(cap(bsm.body)) {
		bsm.body = make([]byte, l)
	}
	bsm.body = bsm.body[:l]
	n = 0
	for n != len(bsm.body) {
		sn, err = r.Read(bsm.body[n:])
		n += sn
		if err != nil {
			return uint64(n), err
		}
	}
	vs.inBulkSetChan <- bsm
	return l, nil
}

func (vs *DefaultValueStore) newOutBulkSetMsg() *bulkSetMsg {
	bsm := <-vs.outBulkSetChan
	binary.BigEndian.PutUint64(bsm.header, 0)
	bsm.body = bsm.body[:0]
	return bsm
}

func (bsm *bulkSetMsg) MsgType() ring.MsgType {
	return ring.MSG_BULK_SET
}

func (bsm *bulkSetMsg) MsgLength() uint64 {
	return uint64(8 + len(bsm.body))
}

func (bsm *bulkSetMsg) WriteContent(w io.Writer) (uint64, error) {
	n, err := w.Write(bsm.header)
	if err != nil {
		return uint64(n), err
	}
	n, err = w.Write(bsm.body)
	return uint64(8 + n), err
}

func (bsm *bulkSetMsg) Done() {
	bsm.vs.outBulkSetChan <- bsm
}

func (bsm *bulkSetMsg) nodeID() uint64 {
	return binary.BigEndian.Uint64(bsm.header)
}

func (bsm *bulkSetMsg) add(keyA uint64, keyB uint64, timestampbits uint64, value []byte) bool {
	o := len(bsm.body)
	if o+len(value)+28 >= cap(bsm.body) {
		return false
	}
	bsm.body = bsm.body[:o+len(value)+28]
	binary.BigEndian.PutUint64(bsm.body[o:], keyA)
	binary.BigEndian.PutUint64(bsm.body[o+8:], keyB)
	binary.BigEndian.PutUint64(bsm.body[o+16:], timestampbits)
	binary.BigEndian.PutUint32(bsm.body[o+24:], uint32(len(value)))
	copy(bsm.body[o+28:], value)
	return true
}
