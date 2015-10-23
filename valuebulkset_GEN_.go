package valuestore

import (
	"encoding/binary"
	"io"
	"sync/atomic"
	"time"
)

// bsm: senderNodeID:8 entries:n
// bsm entry: keyA:8, keyB:8, timestampbits:8, length:4, value:n

const _VALUE_BULK_SET_MSG_TYPE = 0x44f58445991a4aa1

const _VALUE_BULK_SET_MSG_HEADER_LENGTH = 8
const _VALUE_BULK_SET_MSG_ENTRY_HEADER_LENGTH = 28
const _VALUE_BULK_SET_MSG_MIN_ENTRY_LENGTH = 28

type valueBulkSetState struct {
	msgCap               int
	inMsgChan            chan *valueBulkSetMsg
	inFreeMsgChan        chan *valueBulkSetMsg
	inResponseMsgTimeout time.Duration
	outFreeMsgChan       chan *valueBulkSetMsg
	inBulkSetDoneChans   []chan struct{}
}

type valueBulkSetMsg struct {
	vs     *DefaultValueStore
	header []byte
	body   []byte
}

func (vs *DefaultValueStore) bulkSetConfig(cfg *ValueStoreConfig) {
	if vs.msgRing != nil {
		vs.msgRing.SetMsgHandler(_VALUE_BULK_SET_MSG_TYPE, vs.newInBulkSetMsg)
		vs.bulkSetState.inMsgChan = make(chan *valueBulkSetMsg, cfg.InBulkSetMsgs)
		vs.bulkSetState.inFreeMsgChan = make(chan *valueBulkSetMsg, cfg.InBulkSetMsgs)
		for i := 0; i < cap(vs.bulkSetState.inFreeMsgChan); i++ {
			vs.bulkSetState.inFreeMsgChan <- &valueBulkSetMsg{
				vs:     vs,
				header: make([]byte, _VALUE_BULK_SET_MSG_HEADER_LENGTH),
				body:   make([]byte, cfg.BulkSetMsgCap),
			}
		}
		vs.bulkSetState.inBulkSetDoneChans = make([]chan struct{}, cfg.InBulkSetWorkers)
		for i := 0; i < len(vs.bulkSetState.inBulkSetDoneChans); i++ {
			vs.bulkSetState.inBulkSetDoneChans[i] = make(chan struct{}, 1)
		}
		vs.bulkSetState.msgCap = cfg.BulkSetMsgCap
		vs.bulkSetState.outFreeMsgChan = make(chan *valueBulkSetMsg, cfg.OutBulkSetMsgs)
		for i := 0; i < cap(vs.bulkSetState.outFreeMsgChan); i++ {
			vs.bulkSetState.outFreeMsgChan <- &valueBulkSetMsg{
				vs:     vs,
				header: make([]byte, _VALUE_BULK_SET_MSG_HEADER_LENGTH),
				body:   make([]byte, cfg.BulkSetMsgCap),
			}
		}
		vs.bulkSetState.inResponseMsgTimeout = time.Duration(cfg.InBulkSetResponseMsgTimeout) * time.Millisecond
	}
}

func (vs *DefaultValueStore) bulkSetLaunch() {
	for i := 0; i < len(vs.bulkSetState.inBulkSetDoneChans); i++ {
		go vs.inBulkSet(vs.bulkSetState.inBulkSetDoneChans[i])
	}
}

// newInBulkSetMsg reads bulk-set messages from the MsgRing and puts them on
// the inMsgChan for the inBulkSet workers to work on.
func (vs *DefaultValueStore) newInBulkSetMsg(r io.Reader, l uint64) (uint64, error) {
	var bsm *valueBulkSetMsg
	select {
	case bsm = <-vs.bulkSetState.inFreeMsgChan:
	default:
		// If there isn't a free valueBulkSetMsg, just read and discard the incoming
		// bulk-set message.
		left := l
		var sn int
		var err error
		for left > 0 {
			t := toss
			if left < uint64(len(t)) {
				t = t[:left]
			}
			sn, err = r.Read(t)
			left -= uint64(sn)
			if err != nil {
				atomic.AddInt32(&vs.inBulkSetInvalids, 1)
				return l - left, err
			}
		}
		atomic.AddInt32(&vs.inBulkSetDrops, 1)
		return l, nil
	}
	// If the message is obviously too short, just throw it away.
	if l < _VALUE_BULK_SET_MSG_HEADER_LENGTH+_VALUE_BULK_SET_MSG_MIN_ENTRY_LENGTH {
		vs.bulkSetState.inFreeMsgChan <- bsm
		left := l
		var sn int
		var err error
		for left > 0 {
			t := toss
			if left < uint64(len(t)) {
				t = t[:left]
			}
			sn, err = r.Read(t)
			left -= uint64(sn)
			if err != nil {
				atomic.AddInt32(&vs.inBulkSetInvalids, 1)
				return l - left, err
			}
		}
		atomic.AddInt32(&vs.inBulkSetInvalids, 1)
		return l, nil
	}
	var n int
	var sn int
	var err error
	for n != len(bsm.header) {
		sn, err = r.Read(bsm.header[n:])
		n += sn
		if err != nil {
			vs.bulkSetState.inFreeMsgChan <- bsm
			atomic.AddInt32(&vs.inBulkSetInvalids, 1)
			return uint64(n), err
		}
	}
	l -= uint64(len(bsm.header))
	// TODO: I think we should cap the body size to vs.bulkSetState.msgCap but
	// that also means that the inBulkSet worker will need to handle the likely
	// trailing truncated entry. Once all this is done, the overall cluster
	// should work even if the caps are set differently from node to node
	// (definitely not recommended though), as the bulk-set messages would
	// eventually start falling under the minimum cap as the front-end data is
	// tranferred and acknowledged. Anyway, I think this is needed in case
	// someone accidentally screws up the cap on one node, making it way too
	// big. Rather just have that one node abuse/run-out-of memory instead of
	// it causing every other node it sends bulk-set messages to also have
	// memory issues.
	if l > uint64(cap(bsm.body)) {
		bsm.body = make([]byte, l)
	}
	bsm.body = bsm.body[:l]
	n = 0
	for n != len(bsm.body) {
		sn, err = r.Read(bsm.body[n:])
		n += sn
		if err != nil {
			vs.bulkSetState.inFreeMsgChan <- bsm
			atomic.AddInt32(&vs.inBulkSetInvalids, 1)
			return uint64(len(bsm.header)) + uint64(n), err
		}
	}
	vs.bulkSetState.inMsgChan <- bsm
	atomic.AddInt32(&vs.inBulkSets, 1)
	return uint64(len(bsm.header)) + l, nil
}

// inBulkSet actually processes incoming bulk-set messages; there may be more
// than one of these workers.
func (vs *DefaultValueStore) inBulkSet(doneChan chan struct{}) {
	for {
		bsm := <-vs.bulkSetState.inMsgChan
		if bsm == nil {
			break
		}
		body := bsm.body
		var err error
		ring := vs.msgRing.Ring()
		var rightwardPartitionShift uint64
		var bsam *valueBulkSetAckMsg
		var rtimestampbits uint64
		if ring != nil {
			rightwardPartitionShift = 64 - uint64(ring.PartitionBitCount())
			// Only ack if there is someone to ack to, which should always be
			// the case but just in case.
			if bsm.nodeID() != 0 {
				bsam = vs.newOutBulkSetAckMsg()
			}
		}
		for len(body) > _VALUE_BULK_SET_MSG_ENTRY_HEADER_LENGTH {
			keyA := binary.BigEndian.Uint64(body)
			keyB := binary.BigEndian.Uint64(body[8:])
			timestampbits := binary.BigEndian.Uint64(body[16:])
			l := binary.BigEndian.Uint32(body[24:])
			atomic.AddInt32(&vs.inBulkSetWrites, 1)
			// Attempt to store everything received...
			// Note that deletions are acted upon as internal requests (work
			// even if writes are disabled due to disk fullness) and new data
			// writes are not.
			rtimestampbits, err = vs.write(keyA, keyB, timestampbits, body[_VALUE_BULK_SET_MSG_ENTRY_HEADER_LENGTH:_VALUE_BULK_SET_MSG_ENTRY_HEADER_LENGTH+l], timestampbits&_TSB_DELETION != 0)
			if err != nil {
				atomic.AddInt32(&vs.inBulkSetWriteErrors, 1)
			} else if rtimestampbits != timestampbits {
				atomic.AddInt32(&vs.inBulkSetWritesOverridden, 1)
			}
			// But only ack on success, there is someone to ack to, and the
			// local node is responsible for the data.
			if err == nil && bsam != nil && ring != nil && ring.Responsible(uint32(keyA>>rightwardPartitionShift)) {
				bsam.add(keyA, keyB, timestampbits)
			}
			body = body[_VALUE_BULK_SET_MSG_ENTRY_HEADER_LENGTH+l:]
		}
		if bsam != nil {
			atomic.AddInt32(&vs.outBulkSetAcks, 1)
			vs.msgRing.MsgToNode(bsam, bsm.nodeID(), vs.bulkSetState.inResponseMsgTimeout)
		}
		vs.bulkSetState.inFreeMsgChan <- bsm
	}
	doneChan <- struct{}{}
}

// newOutBulkSetMsg gives an initialized valueBulkSetMsg for filling out and
// eventually sending using the MsgRing. The MsgRing (or someone else if the
// message doesn't end up with the MsgRing) will call valueBulkSetMsg.Free()
// eventually and the valueBulkSetMsg will be requeued for reuse later. There is a
// fixed number of outgoing valueBulkSetMsg instances that can exist at any given
// time, capping memory usage. Once the limit is reached, this method will
// block until a valueBulkSetMsg is available to return.
func (vs *DefaultValueStore) newOutBulkSetMsg() *valueBulkSetMsg {
	bsm := <-vs.bulkSetState.outFreeMsgChan
	if vs.msgRing != nil {
		if r := vs.msgRing.Ring(); r != nil {
			if n := r.LocalNode(); n != nil {
				binary.BigEndian.PutUint64(bsm.header, n.ID())
			}
		}
	}
	bsm.body = bsm.body[:0]
	return bsm
}

func (bsm *valueBulkSetMsg) MsgType() uint64 {
	return _VALUE_BULK_SET_MSG_TYPE
}

func (bsm *valueBulkSetMsg) MsgLength() uint64 {
	return uint64(len(bsm.header) + len(bsm.body))
}

func (bsm *valueBulkSetMsg) WriteContent(w io.Writer) (uint64, error) {
	n, err := w.Write(bsm.header)
	if err != nil {
		return uint64(n), err
	}
	n, err = w.Write(bsm.body)
	return uint64(len(bsm.header)) + uint64(n), err
}

func (bsm *valueBulkSetMsg) Free() {
	bsm.vs.bulkSetState.outFreeMsgChan <- bsm
}

func (bsm *valueBulkSetMsg) nodeID() uint64 {
	return binary.BigEndian.Uint64(bsm.header)
}

func (bsm *valueBulkSetMsg) add(keyA uint64, keyB uint64, timestampbits uint64, value []byte) bool {
	// CONSIDER: I'd rather not have "useless" checks every place wasting
	// cycles when the caller should have already validated the input; but here
	// len(value) must not exceed math.MaxUint32.
	o := len(bsm.body)
	if o+_VALUE_BULK_SET_MSG_ENTRY_HEADER_LENGTH+len(value) >= cap(bsm.body) {
		return false
	}
	bsm.body = bsm.body[:o+_VALUE_BULK_SET_MSG_ENTRY_HEADER_LENGTH+len(value)]
	binary.BigEndian.PutUint64(bsm.body[o:], keyA)
	binary.BigEndian.PutUint64(bsm.body[o+8:], keyB)
	binary.BigEndian.PutUint64(bsm.body[o+16:], timestampbits)
	binary.BigEndian.PutUint32(bsm.body[o+24:], uint32(len(value)))
	copy(bsm.body[o+_VALUE_BULK_SET_MSG_ENTRY_HEADER_LENGTH:], value)
	return true
}
