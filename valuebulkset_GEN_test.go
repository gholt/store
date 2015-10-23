package valuestore

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/gholt/ring"
)

func TestValueBulkSetReadObviouslyTooShort(t *testing.T) {
	cfg := lowMemValueStoreConfig()
	cfg.MsgRing = &msgRingPlaceholder{}
	vs, err := NewValueStore(cfg)
	if err != nil {
		t.Fatal("")
	}
	for i := 0; i < len(vs.bulkSetState.inBulkSetDoneChans); i++ {
		vs.bulkSetState.inMsgChan <- nil
	}
	for _, doneChan := range vs.bulkSetState.inBulkSetDoneChans {
		<-doneChan
	}
	n, err := vs.newInBulkSetMsg(bytes.NewBuffer(make([]byte, 1)), 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal(n)
	}
	select {
	case bsm := <-vs.bulkSetState.inMsgChan:
		t.Fatal(bsm)
	default:
	}
	// Once again, way too short but with an error too.
	_, err = vs.newInBulkSetMsg(bytes.NewBuffer(make([]byte, 1)), 2)
	if err != io.EOF {
		t.Fatal(err)
	}
	select {
	case bsm := <-vs.bulkSetState.inMsgChan:
		t.Fatal(bsm)
	default:
	}
}

func TestValueBulkSetRead(t *testing.T) {
	cfg := lowMemValueStoreConfig()
	cfg.MsgRing = &msgRingPlaceholder{}
	vs, err := NewValueStore(cfg)
	if err != nil {
		t.Fatal("")
	}
	for i := 0; i < len(vs.bulkSetState.inBulkSetDoneChans); i++ {
		vs.bulkSetState.inMsgChan <- nil
	}
	for _, doneChan := range vs.bulkSetState.inBulkSetDoneChans {
		<-doneChan
	}
	n, err := vs.newInBulkSetMsg(bytes.NewBuffer(make([]byte, 100)), 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Fatal(n)
	}
	<-vs.bulkSetState.inMsgChan
	// Again, but with an error in the header.
	n, err = vs.newInBulkSetMsg(bytes.NewBuffer(make([]byte, _VALUE_BULK_SET_MSG_HEADER_LENGTH-1)), 100)
	if err != io.EOF {
		t.Fatal(err)
	}
	if n != _VALUE_BULK_SET_MSG_HEADER_LENGTH-1 {
		t.Fatal(n)
	}
	select {
	case bsm := <-vs.bulkSetState.inMsgChan:
		t.Fatal(bsm)
	default:
	}
	// Once again, but with an error in the body.
	n, err = vs.newInBulkSetMsg(bytes.NewBuffer(make([]byte, 10)), 100)
	if err != io.EOF {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatal(n)
	}
	select {
	case bsm := <-vs.bulkSetState.inMsgChan:
		t.Fatal(bsm)
	default:
	}
}

func TestValueBulkSetReadLowSendCap(t *testing.T) {
	cfg := lowMemValueStoreConfig()
	cfg.MsgRing = &msgRingPlaceholder{}
	cfg.BulkSetMsgCap = _VALUE_BULK_SET_MSG_HEADER_LENGTH + 1
	vs, err := NewValueStore(cfg)
	if err != nil {
		t.Fatal("")
	}
	for i := 0; i < len(vs.bulkSetState.inBulkSetDoneChans); i++ {
		vs.bulkSetState.inMsgChan <- nil
	}
	for _, doneChan := range vs.bulkSetState.inBulkSetDoneChans {
		<-doneChan
	}
	for len(vs.bulkSetState.inMsgChan) > 0 {
		time.Sleep(time.Millisecond)
	}
	n, err := vs.newInBulkSetMsg(bytes.NewBuffer(make([]byte, 100)), 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Fatal(n)
	}
	<-vs.bulkSetState.inMsgChan
}

func TestValueBulkSetMsgWithoutAck(t *testing.T) {
	b := ring.NewBuilder(64)
	n, err := b.AddNode(true, 1, nil, nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	r := b.Ring()
	r.SetLocalNode(n.ID())
	m := &msgRingPlaceholder{ring: r}
	cfg := lowMemValueStoreConfig()
	cfg.MsgRing = m
	cfg.InBulkSetWorkers = 1
	cfg.InBulkSetMsgs = 1
	vs, err := NewValueStore(cfg)
	if err != nil {
		t.Fatal("")
	}
	vs.EnableAll()
	defer vs.DisableAll()
	bsm := <-vs.bulkSetState.inFreeMsgChan
	bsm.body = bsm.body[:0]
	if !bsm.add(1, 2, 0x300, []byte("testing")) {
		t.Fatal("")
	}
	vs.bulkSetState.inMsgChan <- bsm
	// only one of these, so if we get it back we know the previous data was
	// processed
	<-vs.bulkSetState.inFreeMsgChan
	ts, v, err := vs.Read(1, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 3 { // the bottom 8 bits are discarded for the public Read
		t.Fatal(ts)
	}
	if string(v) != "testing" {
		t.Fatal(string(v))
	}
	m.lock.Lock()
	v2 := len(m.msgToNodeIDs)
	m.lock.Unlock()
	if v2 != 0 {
		t.Fatal(v2)
	}
}

func TestValueBulkSetMsgWithAck(t *testing.T) {
	b := ring.NewBuilder(64)
	n, err := b.AddNode(true, 1, nil, nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	r := b.Ring()
	r.SetLocalNode(n.ID())
	m := &msgRingPlaceholder{ring: r}
	cfg := lowMemValueStoreConfig()
	cfg.MsgRing = m
	cfg.InBulkSetWorkers = 1
	cfg.InBulkSetMsgs = 1
	vs, err := NewValueStore(cfg)
	if err != nil {
		t.Fatal("")
	}
	vs.EnableAll()
	defer vs.DisableAll()
	bsm := <-vs.bulkSetState.inFreeMsgChan
	binary.BigEndian.PutUint64(bsm.header, 123)
	bsm.body = bsm.body[:0]
	if !bsm.add(1, 2, 0x300, []byte("testing")) {
		t.Fatal("")
	}
	vs.bulkSetState.inMsgChan <- bsm
	// only one of these, so if we get it back we know the previous data was
	// processed
	<-vs.bulkSetState.inFreeMsgChan
	ts, v, err := vs.Read(1, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 3 { // the bottom 8 bits are discarded for the public Read
		t.Fatal(ts)
	}
	if string(v) != "testing" {
		t.Fatal(string(v))
	}
	m.lock.Lock()
	v2 := len(m.msgToNodeIDs)
	m.lock.Unlock()
	if v2 != 1 {
		t.Fatal(v2)
	}
	m.lock.Lock()
	v3 := m.msgToNodeIDs[0]
	m.lock.Unlock()
	if v3 != 123 {
		t.Fatal(v3)
	}
}

func TestValueBulkSetMsgWithoutRing(t *testing.T) {
	m := &msgRingPlaceholder{}
	cfg := lowMemValueStoreConfig()
	cfg.MsgRing = m
	cfg.InBulkSetWorkers = 1
	cfg.InBulkSetMsgs = 1
	vs, err := NewValueStore(cfg)
	if err != nil {
		t.Fatal("")
	}
	vs.EnableAll()
	defer vs.DisableAll()
	bsm := <-vs.bulkSetState.inFreeMsgChan
	binary.BigEndian.PutUint64(bsm.header, 123)
	bsm.body = bsm.body[:0]
	if !bsm.add(1, 2, 0x300, []byte("testing")) {
		t.Fatal("")
	}
	vs.bulkSetState.inMsgChan <- bsm
	// only one of these, so if we get it back we know the previous data was
	// processed
	<-vs.bulkSetState.inFreeMsgChan
	ts, v, err := vs.Read(1, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 3 { // the bottom 8 bits are discarded for the public Read
		t.Fatal(ts)
	}
	if string(v) != "testing" {
		t.Fatal(string(v))
	}
	m.lock.Lock()
	v2 := len(m.msgToNodeIDs)
	m.lock.Unlock()
	if v2 != 0 {
		t.Fatal(v2)
	}
}

func TestValueBulkSetMsgOut(t *testing.T) {
	cfg := lowMemValueStoreConfig()
	cfg.MsgRing = &msgRingPlaceholder{}
	vs, err := NewValueStore(cfg)
	if err != nil {
		t.Fatal("")
	}
	bsm := vs.newOutBulkSetMsg()
	if bsm.MsgType() != _VALUE_BULK_SET_MSG_TYPE {
		t.Fatal(bsm.MsgType())
	}
	if bsm.MsgLength() != _VALUE_BULK_SET_MSG_HEADER_LENGTH {
		t.Fatal(bsm.MsgLength())
	}
	buf := bytes.NewBuffer(nil)
	n, err := bsm.WriteContent(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != _VALUE_BULK_SET_MSG_HEADER_LENGTH {
		t.Fatal(n)
	}
	if !bytes.Equal(buf.Bytes(), []byte{0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Fatal(buf.Bytes())
	}
	bsm.Free()
	bsm = vs.newOutBulkSetMsg()
	binary.BigEndian.PutUint64(bsm.header, 12345)
	bsm.add(1, 2, 0x300, nil)
	bsm.add(4, 5, 0x600, []byte("testing"))
	if bsm.MsgType() != _VALUE_BULK_SET_MSG_TYPE {
		t.Fatal(bsm.MsgType())
	}
	if bsm.MsgLength() != _VALUE_BULK_SET_MSG_HEADER_LENGTH+_VALUE_BULK_SET_MSG_ENTRY_HEADER_LENGTH+0+_VALUE_BULK_SET_MSG_ENTRY_HEADER_LENGTH+7 {
		t.Fatal(bsm.MsgLength())
	}
	buf = bytes.NewBuffer(nil)
	n, err = bsm.WriteContent(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != _VALUE_BULK_SET_MSG_HEADER_LENGTH+_VALUE_BULK_SET_MSG_ENTRY_HEADER_LENGTH+0+_VALUE_BULK_SET_MSG_ENTRY_HEADER_LENGTH+7 {
		t.Fatal(n)
	}
	if !bytes.Equal(buf.Bytes(), []byte{
		0, 0, 0, 0, 0, 0, 48, 57, // header
		0, 0, 0, 0, 0, 0, 0, 1, // keyA
		0, 0, 0, 0, 0, 0, 0, 2, // keyB
		0, 0, 0, 0, 0, 0, 3, 0, // timestamp
		0, 0, 0, 0, // length
		0, 0, 0, 0, 0, 0, 0, 4, // keyA
		0, 0, 0, 0, 0, 0, 0, 5, // keyB
		0, 0, 0, 0, 0, 0, 6, 0, // timestamp
		0, 0, 0, 7, // length
		116, 101, 115, 116, 105, 110, 103, // "testing"
	}) {
		t.Fatal(buf.Bytes())
	}
	bsm.Free()
}

func TestValueBulkSetMsgOutDefaultsToFromLocalNode(t *testing.T) {
	b := ring.NewBuilder(64)
	n, err := b.AddNode(true, 1, nil, nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	r := b.Ring()
	r.SetLocalNode(n.ID())
	cfg := lowMemValueStoreConfig()
	cfg.MsgRing = &msgRingPlaceholder{ring: r}
	vs, err := NewValueStore(cfg)
	if err != nil {
		t.Fatal("")
	}
	bsm := vs.newOutBulkSetMsg()
	if binary.BigEndian.Uint64(bsm.header) != n.ID() {
		t.Fatal(bsm)
	}
}

func TestValueBulkSetMsgOutWriteError(t *testing.T) {
	cfg := lowMemValueStoreConfig()
	cfg.MsgRing = &msgRingPlaceholder{}
	vs, err := NewValueStore(cfg)
	if err != nil {
		t.Fatal("")
	}
	bsm := vs.newOutBulkSetMsg()
	_, err = bsm.WriteContent(&testErrorWriter{})
	if err == nil {
		t.Fatal(err)
	}
	bsm.Free()
}

func TestValueBulkSetMsgOutHitCap(t *testing.T) {
	cfg := lowMemValueStoreConfig()
	cfg.MsgRing = &msgRingPlaceholder{}
	cfg.BulkSetMsgCap = _VALUE_BULK_SET_MSG_HEADER_LENGTH + _VALUE_BULK_SET_MSG_ENTRY_HEADER_LENGTH + 3
	vs, err := NewValueStore(cfg)
	if err != nil {
		t.Fatal("")
	}
	bsm := vs.newOutBulkSetMsg()
	if !bsm.add(1, 2, 0x300, []byte("1")) {
		t.Fatal("")
	}
	if bsm.add(4, 5, 0x600, []byte("12345678901234567890")) {
		t.Fatal("")
	}
}
