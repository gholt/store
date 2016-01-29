package store

import (
	"sync"
	"testing"
	"time"

	"github.com/gholt/ring"
)

type msgRingValuePullReplicationTester struct {
	ring               ring.Ring
	lock               sync.Mutex
	msgToNodeIDs       []uint64
	headerToPartitions [][]byte
	bodyToPartitions   [][]byte
}

func (m *msgRingValuePullReplicationTester) Ring() ring.Ring {
	return m.ring
}

func (m *msgRingValuePullReplicationTester) MaxMsgLength() uint64 {
	return 65536
}

func (m *msgRingValuePullReplicationTester) SetMsgHandler(msgType uint64, handler ring.MsgUnmarshaller) {
}

func (m *msgRingValuePullReplicationTester) MsgToNode(msg ring.Msg, nodeID uint64, timeout time.Duration) {
	m.lock.Lock()
	m.msgToNodeIDs = append(m.msgToNodeIDs, nodeID)
	m.lock.Unlock()
	msg.Free()
}

func (m *msgRingValuePullReplicationTester) MsgToOtherReplicas(msg ring.Msg, partition uint32, timeout time.Duration) {
	prm, ok := msg.(*valuePullReplicationMsg)
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

func TestValuePullReplicationSimple(t *testing.T) {
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
	m := &msgRingValuePullReplicationTester{ring: r}
	cfg := newTestValueStoreConfig()
	cfg.MsgRing = m
	store, _ := newTestValueStore(cfg)
	if err := store.Startup(); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown()
	_, err = store.write(1, 2, 0x500, []byte("testing"), false)
	if err != nil {
		t.Fatal(err)
	}
	store.OutPullReplicationPass()
	m.lock.Lock()
	v := len(m.headerToPartitions)
	m.lock.Unlock()
	if v == 0 {
		t.Fatal(v)
	}
	mayHave := false
	m.lock.Lock()
	for i := 0; i < len(m.headerToPartitions); i++ {
		prm := &valuePullReplicationMsg{store: store, header: m.headerToPartitions[i], body: m.bodyToPartitions[i]}
		bf := prm.ktBloomFilter()
		if bf.mayHave(1, 2, 0x500) {
			mayHave = true
		}
	}
	m.lock.Unlock()
	if !mayHave {
		t.Fatal("")
	}
}
