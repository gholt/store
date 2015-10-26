package valuestore

import (
	"math"
	"sync"
)

type valueMem struct {
	store       *DefaultValueStore
	id          uint32
	vfID        uint32
	vfOffset    uint32
	toc         []byte
	values      []byte
	discardLock sync.RWMutex
}

func (vm *valueMem) timestampnano() int64 {
	return math.MaxInt64
}

func (vm *valueMem) read(keyA uint64, keyB uint64, timestampbits uint64, offset uint32, length uint32, value []byte) (uint64, []byte, error) {
	vm.discardLock.RLock()
	timestampbits, id, offset, length := vm.store.vlm.Get(keyA, keyB)
	if id == 0 || timestampbits&_TSB_DELETION != 0 {
		vm.discardLock.RUnlock()
		return timestampbits, value, ErrNotFound
	}
	if id != vm.id {
		vm.discardLock.RUnlock()
		return vm.store.locBlock(id).read(keyA, keyB, timestampbits, offset, length, value)
	}
	value = append(value, vm.values[offset:offset+length]...)
	vm.discardLock.RUnlock()
	return timestampbits, value, nil
}

func (vm *valueMem) close() error {
	return nil
}
