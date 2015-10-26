package valuestore

import (
	"math"
	"testing"
)

func TestGroupValuesMemRead(t *testing.T) {
	store, err := NewGroupStore(lowMemGroupStoreConfig())
	if err != nil {
		t.Fatal("")
	}
	vm1 := &groupMem{id: 1, store: store, values: []byte("0123456789abcdef")}
	vm2 := &groupMem{id: 2, store: store, values: []byte("fedcba9876543210")}
	store.locBlocks = []groupLocBlock{nil, vm1, vm2}
	tsn := vm1.timestampnano()
	if tsn != math.MaxInt64 {
		t.Fatal(tsn)
	}
	ts, v, err := vm1.read(1, 2, 0, 0, 0x100, 5, 6, nil)
	if err != ErrNotFound {
		t.Fatal(err)
	}
	vm1.store.vlm.Set(1, 2, 0, 0, 0x100, vm1.id, 5, 6, false)
	ts, v, err = vm1.read(1, 2, 0, 0, 0x100, 5, 6, nil)
	if err != nil {
		a, b, c, d := vm1.store.vlm.Get(1, 2, 0, 0)
		t.Fatal(err, a, b, c, d)
	}
	if ts != 0x100 {
		t.Fatal(ts)
	}
	if string(v) != "56789a" {
		t.Fatal(string(v))
	}
	vm1.store.vlm.Set(1, 2, 0, 0, 0x100|_TSB_DELETION, vm1.id, 5, 6, false)
	ts, v, err = vm1.read(1, 2, 0, 0, 0x100, 5, 6, nil)
	if err != ErrNotFound {
		t.Fatal(err)
	}
	vm1.store.vlm.Set(1, 2, 0, 0, 0x200, vm2.id, 5, 6, false)
	ts, v, err = vm1.read(1, 2, 0, 0, 0x100, 5, 6, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0x200 {
		t.Fatal(ts)
	}
	if string(v) != "a98765" {
		t.Fatal(string(v))
	}
}
