package brimstore

import (
	"math"
	"sync"
)

type valuesMem struct {
	vs          *ValuesStore
	id          uint16
	vfID        uint16
	vfOffset    uint32
	toc         []byte
	values      []byte
	discardLock sync.RWMutex
}

func (vm *valuesMem) timestamp() int64 {
	return math.MaxInt64
}

func (vm *valuesMem) readValue(keyA uint64, keyB uint64, value []byte, seq uint64, offset uint32, length uint32) ([]byte, uint64, error) {
	vm.discardLock.RLock()
	id, offset, length, seq := vm.vs.vlm.get(keyA, keyB)
	if id < _VALUESBLOCK_IDOFFSET {
		vm.discardLock.RUnlock()
		return value, seq, ErrValueNotFound
	}
	if id != vm.id {
		vm.discardLock.RUnlock()
		vm.vs.valuesLocBlock(id).readValue(keyA, keyB, value, seq, offset, length)
	}
	value = append(value, vm.values[offset:offset+length]...)
	vm.discardLock.RUnlock()
	return value, seq, nil
}
