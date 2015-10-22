package valuestore

import (
	"bytes"
	"io"
	"testing"

	"github.com/gholt/ring"
)

func TestGroupBulkSetAckRead(t *testing.T) {
	vs, err := NewGroupStore(&GroupStoreConfig{MsgRing: &msgRingPlaceholder{}})
	if err != nil {
		t.Fatal("")
	}
	for i := 0; i < len(vs.bulkSetAckState.inBulkSetAckDoneChans); i++ {
		vs.bulkSetAckState.inMsgChan <- nil
	}
	for _, doneChan := range vs.bulkSetAckState.inBulkSetAckDoneChans {
		<-doneChan
	}
	n, err := vs.newInBulkSetAckMsg(bytes.NewBuffer(make([]byte, 100)), 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Fatal(n)
	}
	<-vs.bulkSetAckState.inMsgChan
	// Once again, but with an error in the body.
	n, err = vs.newInBulkSetAckMsg(bytes.NewBuffer(make([]byte, 10)), 100)
	if err != io.EOF {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatal(n)
	}
	select {
	case bsam := <-vs.bulkSetAckState.inMsgChan:
		t.Fatal(bsam)
	default:
	}
}

func TestGroupBulkSetAckReadLowSendCap(t *testing.T) {
	vs, err := NewGroupStore(&GroupStoreConfig{MsgRing: &msgRingPlaceholder{}, BulkSetAckMsgCap: 1})
	if err != nil {
		t.Fatal("")
	}
	for i := 0; i < len(vs.bulkSetAckState.inBulkSetAckDoneChans); i++ {
		vs.bulkSetAckState.inMsgChan <- nil
	}
	for _, doneChan := range vs.bulkSetAckState.inBulkSetAckDoneChans {
		<-doneChan
	}
	n, err := vs.newInBulkSetAckMsg(bytes.NewBuffer(make([]byte, 100)), 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Fatal(n)
	}
	<-vs.bulkSetAckState.inMsgChan
}

func TestGroupBulkSetAckMsgIncoming(t *testing.T) {
	b := ring.NewBuilder(64)
	n, err := b.AddNode(true, 1, nil, nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	r := b.Ring()
	r.SetLocalNode(n.ID() + 1) // so we're not responsible for anything
	m := &msgRingPlaceholder{ring: r}
	vs, err := NewGroupStore(&GroupStoreConfig{
		MsgRing:             m,
		InBulkSetAckWorkers: 1,
		InBulkSetAckMsgs:    1,
	})
	if err != nil {
		t.Fatal("")
	}
	vs.EnableAll()
	defer vs.DisableAll()
	ts, err := vs.write(1, 2, 0x300, []byte("testing"), true)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0 {
		t.Fatal(ts)
	}
	// just double check the item is there
	ts2, v, err := vs.read(1, 2, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ts2 != 0x300 {
		t.Fatal(ts2)
	}
	if string(v) != "testing" {
		t.Fatal(string(v))
	}
	bsam := <-vs.bulkSetAckState.inFreeMsgChan
	bsam.body = bsam.body[:0]
	if !bsam.add(1, 2, 0x300) {
		t.Fatal("")
	}
	vs.bulkSetAckState.inMsgChan <- bsam
	// only one of these, so if we get it back we know the previous data was
	// processed
	<-vs.bulkSetAckState.inFreeMsgChan
	// Make sure the item is gone
	ts2, v, err = vs.read(1, 2, 0, 0, nil)
	if err != ErrNotFound {
		t.Fatal(err)
	}
	if ts2 != 0x300|_TSB_LOCAL_REMOVAL {
		t.Fatal(ts2)
	}
	if string(v) != "" {
		t.Fatal(string(v))
	}
}

func TestGroupBulkSetAckMsgIncomingNoRing(t *testing.T) {
	m := &msgRingPlaceholder{}
	vs, err := NewGroupStore(&GroupStoreConfig{
		MsgRing:             m,
		InBulkSetAckWorkers: 1,
		InBulkSetAckMsgs:    1,
	})
	if err != nil {
		t.Fatal("")
	}
	vs.EnableAll()
	defer vs.DisableAll()
	ts, err := vs.write(1, 2, 0x300, []byte("testing"), true)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0 {
		t.Fatal(ts)
	}
	// just double check the item is there
	ts2, v, err := vs.read(1, 2, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ts2 != 0x300 {
		t.Fatal(ts2)
	}
	if string(v) != "testing" {
		t.Fatal(string(v))
	}
	bsam := <-vs.bulkSetAckState.inFreeMsgChan
	bsam.body = bsam.body[:0]
	if !bsam.add(1, 2, 0x300) {
		t.Fatal("")
	}
	vs.bulkSetAckState.inMsgChan <- bsam
	// only one of these, so if we get it back we know the previous data was
	// processed
	<-vs.bulkSetAckState.inFreeMsgChan
	// Make sure the item is not gone since we don't know if we're responsible
	// or not since we don't have a ring
	ts2, v, err = vs.read(1, 2, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ts2 != 0x300 {
		t.Fatal(ts2)
	}
	if string(v) != "testing" {
		t.Fatal(string(v))
	}
}

func TestGroupBulkSetAckMsgOut(t *testing.T) {
	vs, err := NewGroupStore(&GroupStoreConfig{MsgRing: &msgRingPlaceholder{}})
	if err != nil {
		t.Fatal("")
	}
	bsam := vs.newOutBulkSetAckMsg()
	if bsam.MsgType() != _GROUP_BULK_SET_ACK_MSG_TYPE {
		t.Fatal(bsam.MsgType())
	}
	if bsam.MsgLength() != 0 {
		t.Fatal(bsam.MsgLength())
	}
	buf := bytes.NewBuffer(nil)
	n, err := bsam.WriteContent(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal(n)
	}
	if !bytes.Equal(buf.Bytes(), []byte{}) {
		t.Fatal(buf.Bytes())
	}
	bsam.Free()
	bsam = vs.newOutBulkSetAckMsg()
	bsam.add(1, 2, 0x300)
	bsam.add(4, 5, 0x600)
	if bsam.MsgType() != _GROUP_BULK_SET_ACK_MSG_TYPE {
		t.Fatal(bsam.MsgType())
	}
	if bsam.MsgLength() != _GROUP_BULK_SET_ACK_MSG_ENTRY_LENGTH+_GROUP_BULK_SET_ACK_MSG_ENTRY_LENGTH {
		t.Fatal(bsam.MsgLength())
	}
	buf = bytes.NewBuffer(nil)
	n, err = bsam.WriteContent(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != _GROUP_BULK_SET_ACK_MSG_ENTRY_LENGTH+_GROUP_BULK_SET_ACK_MSG_ENTRY_LENGTH {
		t.Fatal(n)
	}
	if !bytes.Equal(buf.Bytes(), []byte{
		0, 0, 0, 0, 0, 0, 0, 1, // keyA
		0, 0, 0, 0, 0, 0, 0, 2, // keyB
		0, 0, 0, 0, 0, 0, 3, 0, // timestamp
		0, 0, 0, 0, 0, 0, 0, 4, // keyA
		0, 0, 0, 0, 0, 0, 0, 5, // keyB
		0, 0, 0, 0, 0, 0, 6, 0, // timestamp
	}) {
		t.Fatal(buf.Bytes())
	}
	bsam.Free()
}

func TestGroupBulkSetAckMsgOutWriteError(t *testing.T) {
	vs, err := NewGroupStore(&GroupStoreConfig{MsgRing: &msgRingPlaceholder{}})
	if err != nil {
		t.Fatal("")
	}
	bsam := vs.newOutBulkSetAckMsg()
	bsam.add(1, 2, 0x300)
	_, err = bsam.WriteContent(&testErrorWriter{})
	if err == nil {
		t.Fatal(err)
	}
	bsam.Free()
}

func TestGroupBulkSetAckMsgOutHitCap(t *testing.T) {
	vs, err := NewGroupStore(&GroupStoreConfig{MsgRing: &msgRingPlaceholder{}, BulkSetAckMsgCap: _GROUP_BULK_SET_ACK_MSG_ENTRY_LENGTH + 3})
	if err != nil {
		t.Fatal("")
	}
	bsam := vs.newOutBulkSetAckMsg()
	if !bsam.add(1, 2, 0x300) {
		t.Fatal("")
	}
	if bsam.add(4, 5, 0x600) {
		t.Fatal("")
	}
}
