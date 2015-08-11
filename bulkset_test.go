package valuestore

import (
	"bytes"
	"io"
	"testing"

	"github.com/gholt/ring"
)

type testBulkSetMsgRing struct {
}

func (m *testBulkSetMsgRing) Ring() ring.Ring {
	return nil
}

func (m *testBulkSetMsgRing) MaxMsgLength() uint64 {
	return 65536
}

func (m *testBulkSetMsgRing) SetMsgHandler(msgType uint64, handler ring.MsgUnmarshaller) {
}

func (m *testBulkSetMsgRing) MsgToNode(nodeID uint64, msg ring.Msg) {
}

func (m *testBulkSetMsgRing) MsgToOtherReplicas(ringVersion int64, partition uint32, msg ring.Msg) {
}

func TestBulkSetInTimeout(t *testing.T) {
	vs := New(&Config{
		MsgRing:             &testBulkSetMsgRing{},
		InBulkSetMsgTimeout: 1,
	})
	// This means that the subsystem can never get a free bulkSetMsg since we
	// never feed this replacement channel.
	vs.bulkSetState.inFreeMsgChan = make(chan *bulkSetMsg, 1)
	n, err := vs.newInBulkSetMsg(bytes.NewBuffer(make([]byte, 100)), 100)
	// Validates we got no error and read all the bytes; meaning the message
	// was read and tossed after the timeout in getting a free bulkSetMsg.
	if err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Fatal(n)
	}
	// Try again to make sure it can handle Reader errors.
	n, err = vs.newInBulkSetMsg(bytes.NewBuffer(make([]byte, 10)), 100)
	if err != io.EOF {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatal(n)
	}
}

func TestBulkSetReadObviouslyTooShort(t *testing.T) {
	vs := New(&Config{MsgRing: &testBulkSetMsgRing{}})
	// Just in case the test fails, we don't want the subsystem to try to
	// interpret our junk bytes.
	vs.bulkSetState.inMsgChan = make(chan *bulkSetMsg, 1)
	n, err := vs.newInBulkSetMsg(bytes.NewBuffer(make([]byte, 1)), 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal(n)
	}
	select {
	case <-vs.bulkSetState.inMsgChan:
		t.Fatal("")
	default:
	}
	// Once again, way too short but with an error too.
	_, err = vs.newInBulkSetMsg(bytes.NewBuffer(make([]byte, 1)), 2)
	if err != io.EOF {
		t.Fatal(err)
	}
	select {
	case <-vs.bulkSetState.inMsgChan:
		t.Fatal("")
	default:
	}
}

func TestBulkSetReadGood(t *testing.T) {
	vs := New(&Config{MsgRing: &testBulkSetMsgRing{}})
	// We don't want the subsystem to try to interpret our junk bytes.
	vs.bulkSetState.inMsgChan = make(chan *bulkSetMsg, 1)
	n, err := vs.newInBulkSetMsg(bytes.NewBuffer(make([]byte, 100)), 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Fatal(n)
	}
	select {
	case <-vs.bulkSetState.inMsgChan:
	default:
		t.Fatal("")
	}
	// Again, but with an error in the header.
	n, err = vs.newInBulkSetMsg(bytes.NewBuffer(make([]byte, _BULK_SET_MSG_HEADER_LENGTH-1)), 100)
	if err != io.EOF {
		t.Fatal(err)
	}
	if n != _BULK_SET_MSG_HEADER_LENGTH-1 {
		t.Fatal(n)
	}
	select {
	case <-vs.bulkSetState.inMsgChan:
		t.Fatal("")
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
	case <-vs.bulkSetState.inMsgChan:
		t.Fatal("")
	default:
	}
}
