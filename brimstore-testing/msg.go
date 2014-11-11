package main

import (
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/brimstore"
)

type FlushWriter interface {
	io.Writer
	Flush() error
}

type msgMap struct {
	lock    sync.RWMutex
	mapping map[brimstore.MsgType]brimstore.MsgUnmarshaller
}

func newMsgMap() *msgMap {
	return &msgMap{mapping: make(map[brimstore.MsgType]brimstore.MsgUnmarshaller)}
}

func (mm *msgMap) set(t brimstore.MsgType, f brimstore.MsgUnmarshaller) brimstore.MsgUnmarshaller {
	mm.lock.Lock()
	p := mm.mapping[t]
	mm.mapping[t] = f
	mm.lock.Unlock()
	return p
}

func (mm *msgMap) get(t brimstore.MsgType) brimstore.MsgUnmarshaller {
	mm.lock.RLock()
	f := mm.mapping[t]
	mm.lock.RUnlock()
	return f
}

type pipeMsgConn struct {
	conn            net.Conn
	lock            sync.RWMutex
	msgMap          *msgMap
	logError        *log.Logger
	logWarning      *log.Logger
	typeBytes       int
	lengthBytes     int
	writeChan       chan brimstore.Msg
	writingDoneChan chan struct{}
	sendDrops       uint32
}

func NewPipeMsgConn(c net.Conn) *pipeMsgConn {
	mc := &pipeMsgConn{
		conn:            c,
		msgMap:          newMsgMap(),
		logError:        log.New(os.Stderr, "", log.LstdFlags),
		logWarning:      log.New(os.Stderr, "", log.LstdFlags),
		typeBytes:       1,
		lengthBytes:     3,
		writeChan:       make(chan brimstore.Msg, 40),
		writingDoneChan: make(chan struct{}, 1),
	}
	return mc
}

func (mc *pipeMsgConn) Start() {
	go mc.reading()
	go mc.writing()
}

const _GLH_SEND_MSG_TIMEOUT = 1

func (mc *pipeMsgConn) SetHandler(t brimstore.MsgType, h brimstore.MsgUnmarshaller) {
	mc.msgMap.set(t, h)
}

func (mc *pipeMsgConn) SendToNode(nodeID uint64, m brimstore.Msg) bool {
	select {
	case mc.writeChan <- m:
		return true
	case <-time.After(_GLH_SEND_MSG_TIMEOUT * time.Second):
		atomic.AddUint32(&mc.sendDrops, 1)
		return false
	}
}

func (mc *pipeMsgConn) SendToOtherReplicas(ringID uint64, partition uint32, m brimstore.Msg) bool {
	// TODO: If ringID has changed, partition invalid, etc. return false
	select {
	case mc.writeChan <- m:
		return true
	case <-time.After(_GLH_SEND_MSG_TIMEOUT * time.Second):
		atomic.AddUint32(&mc.sendDrops, 1)
		return false
	}
}

func (mc *pipeMsgConn) reading() {
	b := make([]byte, mc.typeBytes+mc.lengthBytes)
	d := make([]byte, 65536)
	for {
		var n int
		var sn int
		var err error
		for n != len(b) {
			if err != nil {
				if n != 0 || err != io.EOF {
					mc.logError.Print("error reading msg", err)
				}
				return
			}
			sn, err = mc.conn.Read(b[n:])
			n += sn
		}
		if err != nil {
			mc.logError.Print("error reading msg content", err)
			return
		}
		var t brimstore.MsgType
		for i := 0; i < mc.typeBytes; i++ {
			t = (t << 8) | brimstore.MsgType(b[i])
		}
		var l uint64
		for i := 0; i < mc.lengthBytes; i++ {
			l = (l << 8) | uint64(b[mc.typeBytes+i])
		}
		f := mc.msgMap.get(t)
		if f != nil {
			_, err = f(mc.conn, l)
			if err != nil {
				mc.logError.Print("error reading msg content", err)
				return
			}
		} else {
			mc.logWarning.Printf("unknown msg type %d", t)
			for l > 0 {
				if err != nil {
					mc.logError.Print("err reading msg content", err)
					return
				}
				if l >= uint64(len(d)) {
					sn, err = mc.conn.Read(d)
				} else {
					sn, err = mc.conn.Read(d[:l])
				}
				l -= uint64(sn)
			}
		}
	}
}

func (mc *pipeMsgConn) writing() {
	b := make([]byte, mc.typeBytes+mc.lengthBytes)
	for {
		m := <-mc.writeChan
		if m == nil {
			break
		}
		t := m.MsgType()
		for i := mc.typeBytes - 1; i >= 0; i-- {
			b[i] = byte(t)
			t >>= 8
		}
		l := m.MsgLength()
		for i := mc.lengthBytes - 1; i >= 0; i-- {
			b[mc.typeBytes+i] = byte(l)
			l >>= 8
		}
		_, err := mc.conn.Write(b)
		if err != nil {
			mc.logError.Print("err writing msg", err)
			break
		}
		_, err = m.WriteContent(mc.conn)
		if err != nil {
			mc.logError.Print("err writing msg content", err)
			break
		}
		m.Done()
	}
	mc.writingDoneChan <- struct{}{}
}
