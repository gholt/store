package valuestore

import (
	"math"
	"sync"
)

type valuesMem struct {
	vs          *DefaultValueStore
	id          uint32
	vfID        uint32
	vfOffset    uint32
	toc         []byte
	values      []byte
	discardLock sync.RWMutex
}

var flushValuesMem *valuesMem = &valuesMem{}

func (vm *valuesMem) timestamp() int64 {
	return math.MaxInt64
}

func (vm *valuesMem) read(keyA uint64, keyB uint64, timestamp uint64, offset uint32, length uint32, value []byte) (uint64, []byte, error) {
	vm.discardLock.RLock()
	timestamp, id, offset, length := vm.vs.vlm.Get(keyA, keyB)
	if id == 0 || timestamp&1 == 1 {
		vm.discardLock.RUnlock()
		return timestamp, value, ErrNotFound
	}
	if id != vm.id {
		vm.discardLock.RUnlock()
		vm.vs.valueLocBlock(id).read(keyA, keyB, timestamp, offset, length, value)
	}
	value = append(value, vm.values[offset:offset+length]...)
	vm.discardLock.RUnlock()
	return timestamp, value, nil
}
