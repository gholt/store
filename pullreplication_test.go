package valuestore

import (
	"sync"
	"testing"
	"time"

	"github.com/gholt/ring"
)

type msgRingPullReplicationTester struct {
	ring               ring.Ring
	lock               sync.Mutex
	msgToNodeIDs       []uint64
	headerToPartitions [][]byte
	bodyToPartitions   [][]byte
}

func (m *msgRingPullReplicationTester) Ring() ring.Ring {
	return m.ring
}

func (m *msgRingPullReplicationTester) MaxMsgLength() uint64 {
	return 65536
}

func (m *msgRingPullReplicationTester) SetMsgHandler(msgType uint64, handler ring.MsgUnmarshaller) {
}

func (m *msgRingPullReplicationTester) MsgToNode(msg ring.Msg, nodeID uint64, timeout time.Duration) {
	m.lock.Lock()
	m.msgToNodeIDs = append(m.msgToNodeIDs, nodeID)
	m.lock.Unlock()
	msg.Free()
}

func (m *msgRingPullReplicationTester) MsgToOtherReplicas(msg ring.Msg, partition uint32, timeout time.Duration) {
	prm, ok := msg.(*pullReplicationMsg)
	if ok {
		m.lock.Lock()
		h := make([]byte, len(prm.header))
		copy(h, prm.header)
		m.headerToPartitions = append(m.headerToPartitions, h)
		b := make([]byte, len(prm.body))
		copy(b, prm.body)
		m.bodyToPartitions = append(m.bodyToPartitions, b)
		m.lock.Unlock()
	}
	msg.Free()
}

func TestPullReplicationSimple(t *testing.T) {
	b := ring.NewBuilder(64)
	b.SetReplicaCount(2)
	n, err := b.AddNode(true, 1, nil, nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.AddNode(true, 1, nil, nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	r := b.Ring()
	r.SetLocalNode(n.ID())
	m := &msgRingPullReplicationTester{ring: r}
	vs, err := New(&Config{MsgRing: m})
	if err != nil {
		t.Fatal("")
	}
	vs.EnableAll()
	defer vs.DisableAll()
	_, err = vs.write(1, 2, 0x300, []byte("testing"), false)
	if err != nil {
		t.Fatal(err)
	}
	vs.OutPullReplicationPass()
	m.lock.Lock()
	v := len(m.headerToPartitions)
	m.lock.Unlock()
	if v == 0 {
		t.Fatal(v)
	}
	mayHave := false
	m.lock.Lock()
	for i := 0; i < len(m.headerToPartitions); i++ {
		prm := &pullReplicationMsg{vs: vs, header: m.headerToPartitions[i], body: m.bodyToPartitions[i]}
		bf := prm.ktBloomFilter()
		if bf.mayHave(1, 2, 0x300) {
			mayHave = true
		}
	}
	m.lock.Unlock()
	if !mayHave {
		t.Fatal("")
	}
}
