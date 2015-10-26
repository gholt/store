package valuestore

import (
	"math"
	"sync"
)

type groupMem struct {
	store       *DefaultGroupStore
	id          uint32
	vfID        uint32
	vfOffset    uint32
	toc         []byte
	values      []byte
	discardLock sync.RWMutex
}

func (vm *groupMem) timestampnano() int64 {
	return math.MaxInt64
}

func (vm *groupMem) read(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, timestampbits uint64, offset uint32, length uint32, value []byte) (uint64, []byte, error) {
	vm.discardLock.RLock()
	timestampbits, id, offset, length := vm.store.vlm.Get(keyA, keyB, nameKeyA, nameKeyB)
	if id == 0 || timestampbits&_TSB_DELETION != 0 {
		vm.discardLock.RUnlock()
		return timestampbits, value, ErrNotFound
	}
	if id != vm.id {
		vm.discardLock.RUnlock()
		return vm.store.locBlock(id).read(keyA, keyB, nameKeyA, nameKeyB, timestampbits, offset, length, value)
	}
	value = append(value, vm.values[offset:offset+length]...)
	vm.discardLock.RUnlock()
	return timestampbits, value, nil
}

func (vm *groupMem) close() error {
	return nil
}
