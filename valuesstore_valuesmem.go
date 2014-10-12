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

func (vm *valuesMem) read(keyA uint64, keyB uint64, seq uint64, offset uint32, length uint32, value []byte) (uint64, []byte, error) {
	vm.discardLock.RLock()
	seq, id, offset, length := vm.vs.vlm.get(keyA, keyB)
	if id < _VALUESBLOCK_IDOFFSET {
		vm.discardLock.RUnlock()
		return seq, value, ErrValueNotFound
	}
	if id != vm.id {
		vm.discardLock.RUnlock()
		vm.vs.valuesLocBlock(id).read(keyA, keyB, seq, offset, length, value)
	}
	value = append(value, vm.values[offset:offset+length]...)
	vm.discardLock.RUnlock()
	return seq, value, nil
}
