package valuestore

import (
	"encoding/binary"
	"io"
	"time"
)

// bsm: senderNodeID:8 entries:n
// bsm entry: keyA:8, keyB:8, timestampbits:8, length:4, value:n
const _BULK_SET_MSG_TYPE = 0x44f58445991a4aa1
const _BULK_SET_MSG_HEADER_LENGTH = 8
const _BULK_SET_MSG_ENTRY_HEADER_LENGTH = 28
const _BULK_SET_MSG_MIN_ENTRY_LENGTH = 28

type bulkSetState struct {
	msgCap         int
	inMsgChan      chan *bulkSetMsg
	inFreeMsgChan  chan *bulkSetMsg
	inMsgTimeout   time.Duration
	outFreeMsgChan chan *bulkSetMsg
}

type bulkSetMsg struct {
	vs     *DefaultValueStore
	header []byte
	body   []byte
}

func (vs *DefaultValueStore) bulkSetInit(cfg *Config) {
	if vs.msgRing != nil {
		vs.msgRing.SetMsgHandler(_BULK_SET_MSG_TYPE, vs.newInBulkSetMsg)
		vs.bulkSetState.inMsgChan = make(chan *bulkSetMsg, cfg.InBulkSetMsgs)
		vs.bulkSetState.inFreeMsgChan = make(chan *bulkSetMsg, cfg.InBulkSetMsgs)
		for i := 0; i < cap(vs.bulkSetState.inFreeMsgChan); i++ {
			vs.bulkSetState.inFreeMsgChan <- &bulkSetMsg{
				vs:     vs,
				header: make([]byte, _BULK_SET_MSG_HEADER_LENGTH),
				body:   make([]byte, cfg.BulkSetMsgCap),
			}
		}
		for i := 0; i < cfg.InBulkSetWorkers; i++ {
			go vs.inBulkSet()
		}
		vs.bulkSetState.msgCap = cfg.BulkSetMsgCap
		vs.bulkSetState.outFreeMsgChan = make(chan *bulkSetMsg, cfg.OutBulkSetMsgs)
		for i := 0; i < cap(vs.bulkSetState.outFreeMsgChan); i++ {
			vs.bulkSetState.outFreeMsgChan <- &bulkSetMsg{
				vs:     vs,
				header: make([]byte, _BULK_SET_MSG_HEADER_LENGTH),
				body:   make([]byte, cfg.BulkSetMsgCap),
			}
		}
		vs.bulkSetState.inMsgTimeout = time.Duration(cfg.InBulkSetMsgTimeout) * time.Second
	}
}

// newInBulkSetMsg reads bulk-set messages from the MsgRing and puts them on
// the inMsgChan for the inBulkSet workers to work on.
func (vs *DefaultValueStore) newInBulkSetMsg(r io.Reader, l uint64) (uint64, error) {
	var bsm *bulkSetMsg
	select {
	case bsm = <-vs.bulkSetState.inFreeMsgChan:
		// If there isn't a free bulkSetMsg after some time, give up and just
		// read and discard the incoming bulk-set message.
	case <-time.After(vs.bulkSetState.inMsgTimeout):
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
				return l - left, err
			}
		}
		return l, nil
	}
	// If the message is obviously too short, just throw it away.
	if l < _BULK_SET_MSG_HEADER_LENGTH+_BULK_SET_MSG_MIN_ENTRY_LENGTH {
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
				return l - left, err
			}
		}
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
			return uint64(len(bsm.header)) + uint64(n), err
		}
	}
	vs.bulkSetState.inMsgChan <- bsm
	return uint64(len(bsm.header)) + l, nil
}

// inBulkSet actually processes incoming bulk-set messages; there may be more
// than one of these workers.
func (vs *DefaultValueStore) inBulkSet() {
	for {
		bsm := <-vs.bulkSetState.inMsgChan
		if bsm == nil {
			break
		}
		body := bsm.body
		var err error
		ring := vs.msgRing.Ring()
		var rightwardPartitionShift uint64
		var bsam *bulkSetAckMsg
		if ring != nil {
			rightwardPartitionShift = 64 - uint64(ring.PartitionBitCount())
			// Only ack if there is someone to ack to, which should always be
			// the case but just in case.
			if bsm.nodeID() != 0 {
				bsam = vs.newOutBulkSetAckMsg()
			}
		}
		for len(body) > _BULK_SET_MSG_ENTRY_HEADER_LENGTH {
			keyA := binary.BigEndian.Uint64(body)
			keyB := binary.BigEndian.Uint64(body[8:])
			timestampbits := binary.BigEndian.Uint64(body[16:])
			l := binary.BigEndian.Uint32(body[24:])
			// Attempt to store everything received...
			_, err = vs.write(keyA, keyB, timestampbits, body[_BULK_SET_MSG_ENTRY_HEADER_LENGTH:_BULK_SET_MSG_ENTRY_HEADER_LENGTH+l])
			// But only ack on success, there is someone to ack to, and the
			// local node is responsible for the data.
			if err == nil && bsam != nil && ring != nil && ring.Responsible(uint32(keyA>>rightwardPartitionShift)) {
				bsam.add(keyA, keyB, timestampbits)
			}
			body = body[_BULK_SET_MSG_ENTRY_HEADER_LENGTH+l:]
		}
		if bsam != nil {
			vs.msgRing.MsgToNode(bsm.nodeID(), bsam)
		}
		vs.bulkSetState.inFreeMsgChan <- bsm
	}
}

// newOutBulkSetMsg gives an initialized bulkSetMsg for filling out and
// eventually sending using the MsgRing. The MsgRing (or someone else if the
// message doesn't end up with the MsgRing) will call bulkSetMsg.Done()
// eventually and the bulkSetMsg will be requeued for reuse later. There is a
// fixed number of outgoing bulkSetMsg instances that can exist at any given
// time, capping memory usage. Once the limit is reached, this method will
// block until a bulkSetMsg is available to return.
func (vs *DefaultValueStore) newOutBulkSetMsg() *bulkSetMsg {
	bsm := <-vs.bulkSetState.outFreeMsgChan
	binary.BigEndian.PutUint64(bsm.header, 0)
	bsm.body = bsm.body[:0]
	return bsm
}

func (bsm *bulkSetMsg) MsgType() uint64 {
	return _BULK_SET_MSG_TYPE
}

func (bsm *bulkSetMsg) MsgLength() uint64 {
	return uint64(len(bsm.header) + len(bsm.body))
}

func (bsm *bulkSetMsg) WriteContent(w io.Writer) (uint64, error) {
	n, err := w.Write(bsm.header)
	if err != nil {
		return uint64(n), err
	}
	n, err = w.Write(bsm.body)
	return uint64(len(bsm.header)) + uint64(n), err
}

func (bsm *bulkSetMsg) Done() {
	bsm.vs.bulkSetState.outFreeMsgChan <- bsm
}

func (bsm *bulkSetMsg) nodeID() uint64 {
	return binary.BigEndian.Uint64(bsm.header)
}

func (bsm *bulkSetMsg) add(keyA uint64, keyB uint64, timestampbits uint64, value []byte) bool {
	// CONSIDER: I'd rather not have "useless" checks every place wasting
	// cycles when the caller should have already validated the input; but here
	// len(value) must not exceed math.MaxUint32.
	o := len(bsm.body)
	if o+_BULK_SET_MSG_ENTRY_HEADER_LENGTH+len(value) >= cap(bsm.body) {
		return false
	}
	bsm.body = bsm.body[:o+_BULK_SET_MSG_ENTRY_HEADER_LENGTH+len(value)]
	binary.BigEndian.PutUint64(bsm.body[o:], keyA)
	binary.BigEndian.PutUint64(bsm.body[o+8:], keyB)
	binary.BigEndian.PutUint64(bsm.body[o+16:], timestampbits)
	binary.BigEndian.PutUint32(bsm.body[o+24:], uint32(len(value)))
	copy(bsm.body[o+_BULK_SET_MSG_ENTRY_HEADER_LENGTH:], value)
	return true
}
