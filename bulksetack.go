package valuestore

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/gholt/experimental-ring"
)

const _GLH_IN_BULK_SET_ACK_MSGS = 128
const _GLH_IN_BULK_SET_ACK_HANDLERS = 40
const _GLH_OUT_BULK_SET_ACK_MSGS = 128
const _GLH_OUT_BULK_SET_ACK_MSG_SIZE = 16 * 1024 * 1024
const _GLH_IN_BULK_SET_ACK_MSG_TIMEOUT = 300

type bulkSetAckMsg struct {
	vs   *DefaultValueStore
	body []byte
}

func (vs *DefaultValueStore) inBulkSetAck() {
	for {
		bsam := <-vs.inBulkSetAckChan
		rid := vs.ring.ID()
		sppower := 64 - vs.ring.PartitionPower()
		b := bsam.body
		l := len(b)
		for o := 0; o < l; o += 24 {
			if rid != vs.ring.ID() {
				rid = vs.ring.ID()
				sppower = 64 - vs.ring.PartitionPower()
			}
			keyA := binary.BigEndian.Uint64(b[o:])
			if !vs.ring.Responsible(uint32(keyA >> sppower)) {
				vs.write(keyA, binary.BigEndian.Uint64(b[o+8:]), binary.BigEndian.Uint64(b[o+16:])|_TSB_LOCAL_REMOVAL, nil)
			}
		}
		vs.freeInBulkSetAckChan <- bsam
	}
}

func (vs *DefaultValueStore) newInBulkSetAckMsg(r io.Reader, l uint64) (uint64, error) {
	var bsam *bulkSetAckMsg
	select {
	case bsam = <-vs.freeInBulkSetAckChan:
	case <-time.After(_GLH_IN_BULK_SET_ACK_MSG_TIMEOUT * time.Second):
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
	if l > uint64(cap(bsam.body)) {
		bsam.body = make([]byte, l)
	}
	bsam.body = bsam.body[:l]
	n = 0
	for n != len(bsam.body) {
		sn, err = r.Read(bsam.body[n:])
		n += sn
		if err != nil {
			return uint64(n), err
		}
	}
	vs.inBulkSetAckChan <- bsam
	return l, nil
}

func (vs *DefaultValueStore) newOutBulkSetAckMsg() *bulkSetAckMsg {
	bsam := <-vs.outBulkSetAckChan
	bsam.body = bsam.body[:0]
	return bsam
}

func (bsam *bulkSetAckMsg) MsgType() ring.MsgType {
	return ring.MSG_BULK_SET_ACK
}

func (bsam *bulkSetAckMsg) MsgLength() uint64 {
	return uint64(len(bsam.body))
}

func (bsam *bulkSetAckMsg) WriteContent(w io.Writer) (uint64, error) {
	n, err := w.Write(bsam.body)
	return uint64(n), err
}

func (bsam *bulkSetAckMsg) Done() {
	bsam.vs.outBulkSetAckChan <- bsam
}

func (bsam *bulkSetAckMsg) add(keyA uint64, keyB uint64, timestampbits uint64) bool {
	o := len(bsam.body)
	if o+24 >= cap(bsam.body) {
		return false
	}
	bsam.body = bsam.body[:o+24]
	binary.BigEndian.PutUint64(bsam.body[o:], keyA)
	binary.BigEndian.PutUint64(bsam.body[o+8:], keyB)
	binary.BigEndian.PutUint64(bsam.body[o+16:], timestampbits)
	return true
}
