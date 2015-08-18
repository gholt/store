package valuestore

import (
	"encoding/binary"
	"io"
	"time"
)

// bsam: entries:n
// bsam entry: keyA:8, keyB:8, timestampbits:8
const _BULK_SET_ACK_MSG_TYPE = 0x39589f4746844e3b
const _BULK_SET_ACK_MSG_ENTRY_LENGTH = 24

type bulkSetAckState struct {
	inMsgChan             chan *bulkSetAckMsg
	inFreeMsgChan         chan *bulkSetAckMsg
	outFreeMsgChan        chan *bulkSetAckMsg
	inMsgTimeout          time.Duration
	inBulkSetAckDoneChans []chan struct{}
}

type bulkSetAckMsg struct {
	vs   *DefaultValueStore
	body []byte
}

func (vs *DefaultValueStore) bulkSetAckInit(cfg *Config) {
	if vs.msgRing != nil {
		vs.msgRing.SetMsgHandler(_BULK_SET_ACK_MSG_TYPE, vs.newInBulkSetAckMsg)
		vs.bulkSetAckState.inMsgChan = make(chan *bulkSetAckMsg, cfg.InBulkSetAckMsgs)
		vs.bulkSetAckState.inFreeMsgChan = make(chan *bulkSetAckMsg, cfg.InBulkSetAckMsgs)
		for i := 0; i < cap(vs.bulkSetAckState.inFreeMsgChan); i++ {
			vs.bulkSetAckState.inFreeMsgChan <- &bulkSetAckMsg{
				vs:   vs,
				body: make([]byte, cfg.BulkSetAckMsgCap),
			}
		}
		vs.bulkSetAckState.inBulkSetAckDoneChans = make([]chan struct{}, cfg.InBulkSetAckWorkers)
		for i := 0; i < cfg.InBulkSetAckWorkers; i++ {
			vs.bulkSetAckState.inBulkSetAckDoneChans[i] = make(chan struct{}, 1)
			go vs.inBulkSetAck(vs.bulkSetAckState.inBulkSetAckDoneChans[i])
		}
		vs.bulkSetAckState.outFreeMsgChan = make(chan *bulkSetAckMsg, cfg.OutBulkSetAckMsgs)
		for i := 0; i < cap(vs.bulkSetAckState.outFreeMsgChan); i++ {
			vs.bulkSetAckState.outFreeMsgChan <- &bulkSetAckMsg{
				vs:   vs,
				body: make([]byte, cfg.BulkSetAckMsgCap),
			}
		}
		vs.bulkSetAckState.inMsgTimeout = time.Duration(cfg.InBulkSetAckMsgTimeout) * time.Second
	}
}

// newInBulkSetAckMsg reads bulk-set-ack messages from the MsgRing and puts
// them on the inMsgChan for the inBulkSetAck workers to work on.
func (vs *DefaultValueStore) newInBulkSetAckMsg(r io.Reader, l uint64) (uint64, error) {
	var bsam *bulkSetAckMsg
	select {
	case bsam = <-vs.bulkSetAckState.inFreeMsgChan:
		// If there isn't a free bulkSetAckMsg after some time, give up and
		// just read and discard the incoming bulk-set-ack message.
	case <-time.After(vs.bulkSetAckState.inMsgTimeout):
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
	// TODO: Need to read up the actual msg cap and toss rest.
	if l > uint64(cap(bsam.body)) {
		bsam.body = make([]byte, l)
	}
	bsam.body = bsam.body[:l]
	n = 0
	for n != len(bsam.body) {
		sn, err = r.Read(bsam.body[n:])
		n += sn
		if err != nil {
			vs.bulkSetAckState.inFreeMsgChan <- bsam
			return uint64(n), err
		}
	}
	vs.bulkSetAckState.inMsgChan <- bsam
	return l, nil
}

// inBulkSetAck actually processes incoming bulk-set-ack messages; there may be
// more than one of these workers.
func (vs *DefaultValueStore) inBulkSetAck(doneChan chan struct{}) {
	for {
		bsam := <-vs.bulkSetAckState.inMsgChan
		if bsam == nil {
			break
		}
		ring := vs.msgRing.Ring()
		var rightwardPartitionShift uint64
		if ring != nil {
			rightwardPartitionShift = 64 - uint64(ring.PartitionBitCount())
		}
		b := bsam.body
		// div mul just ensures any trailing bytes are dropped
		l := len(b) / _BULK_SET_ACK_MSG_ENTRY_LENGTH * _BULK_SET_ACK_MSG_ENTRY_LENGTH
		for o := 0; o < l; o += _BULK_SET_ACK_MSG_ENTRY_LENGTH {
			keyA := binary.BigEndian.Uint64(b[o:])
			if ring != nil && !ring.Responsible(uint32(keyA>>rightwardPartitionShift)) {
				vs.write(keyA, binary.BigEndian.Uint64(b[o+8:]), binary.BigEndian.Uint64(b[o+16:])|_TSB_LOCAL_REMOVAL, nil)
			}
		}
		vs.bulkSetAckState.inFreeMsgChan <- bsam
	}
	doneChan <- struct{}{}
}

// newOutBulkSetAckMsg gives an initialized bulkSetAckMsg for filling out and
// eventually sending using the MsgRing. The MsgRing (or someone else if the
// message doesn't end up with the MsgRing) will call bulkSetAckMsg.Done()
// eventually and the bulkSetAckMsg will be requeued for reuse later. There is
// a fixed number of outgoing bulkSetAckMsg instances that can exist at any
// given time, capping memory usage. Once the limit is reached, this method
// will block until a bulkSetAckMsg is available to return.
func (vs *DefaultValueStore) newOutBulkSetAckMsg() *bulkSetAckMsg {
	bsam := <-vs.bulkSetAckState.outFreeMsgChan
	bsam.body = bsam.body[:0]
	return bsam
}

func (bsam *bulkSetAckMsg) MsgType() uint64 {
	return _BULK_SET_ACK_MSG_TYPE
}

func (bsam *bulkSetAckMsg) MsgLength() uint64 {
	return uint64(len(bsam.body))
}

func (bsam *bulkSetAckMsg) WriteContent(w io.Writer) (uint64, error) {
	n, err := w.Write(bsam.body)
	return uint64(n), err
}

func (bsam *bulkSetAckMsg) Done() {
	bsam.vs.bulkSetAckState.outFreeMsgChan <- bsam
}

func (bsam *bulkSetAckMsg) add(keyA uint64, keyB uint64, timestampbits uint64) bool {
	o := len(bsam.body)
	if o+_BULK_SET_ACK_MSG_ENTRY_LENGTH >= cap(bsam.body) {
		return false
	}
	bsam.body = bsam.body[:o+_BULK_SET_ACK_MSG_ENTRY_LENGTH]
	binary.BigEndian.PutUint64(bsam.body[o:], keyA)
	binary.BigEndian.PutUint64(bsam.body[o+8:], keyB)
	binary.BigEndian.PutUint64(bsam.body[o+16:], timestampbits)
	return true
}
