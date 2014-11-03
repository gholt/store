package valuelocmap

import (
	"testing"
)

func TestSetNewKeyOldTimestampIs0AndNewKeySaved(t *testing.T) {
	vlm := NewValueLocMap()
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp := uint64(2)
	blockID := uint32(1)
	offset := uint32(0)
	length := uint32(0)
	oldTimestamp := vlm.Set(keyA, keyB, timestamp, blockID, offset, length, false)
	if oldTimestamp != 0 {
		t.Fatal(oldTimestamp)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != timestamp {
		t.Fatal(timestampGet, timestamp)
	}
	if blockIDGet != blockID {
		t.Fatal(blockIDGet, blockID)
	}
	if offsetGet != offset {
		t.Fatal(offsetGet, offset)
	}
	if lengthGet != length {
		t.Fatal(lengthGet, length)
	}
}

func TestSetOverwriteKeyOldTimestampIsOldAndOverwriteWins(t *testing.T) {
	vlm := NewValueLocMap()
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp1 := uint64(2)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	vlm.Set(keyA, keyB, timestamp1, blockID1, offset1, length1, false)
	timestamp2 := timestamp1 + 2
	blockID2 := blockID1 + 1
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp := vlm.Set(keyA, keyB, timestamp2, blockID2, offset2, length2, false)
	if oldTimestamp != timestamp1 {
		t.Fatal(oldTimestamp, timestamp1)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != timestamp2 {
		t.Fatal(timestampGet, timestamp2)
	}
	if blockIDGet != blockID2 {
		t.Fatal(blockIDGet, blockID2)
	}
	if offsetGet != offset2 {
		t.Fatal(offsetGet, offset2)
	}
	if lengthGet != length2 {
		t.Fatal(lengthGet, length2)
	}
}

func TestSetOldOverwriteKeyOldTimestampIsPreviousAndPreviousWins(t *testing.T) {
	vlm := NewValueLocMap()
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp1 := uint64(4)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	vlm.Set(keyA, keyB, timestamp1, blockID1, offset1, length1, false)
	timestamp2 := timestamp1 - 2
	blockID2 := blockID1 + 1
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp := vlm.Set(keyA, keyB, timestamp2, blockID2, offset2, length2, false)
	if oldTimestamp != timestamp1 {
		t.Fatal(oldTimestamp, timestamp1)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != timestamp1 {
		t.Fatal(timestampGet, timestamp1)
	}
	if blockIDGet != blockID1 {
		t.Fatal(blockIDGet, blockID1)
	}
	if offsetGet != offset1 {
		t.Fatal(offsetGet, offset1)
	}
	if lengthGet != length1 {
		t.Fatal(lengthGet, length1)
	}
}

func TestSetOverwriteKeyOldTimestampIsSameAndOverwriteIgnored(t *testing.T) {
	vlm := NewValueLocMap()
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp1 := uint64(2)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	vlm.Set(keyA, keyB, timestamp1, blockID1, offset1, length1, false)
	timestamp2 := timestamp1
	blockID2 := blockID1 + 1
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp := vlm.Set(keyA, keyB, timestamp2, blockID2, offset2, length2, false)
	if oldTimestamp != timestamp1 {
		t.Fatal(oldTimestamp, timestamp1)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != timestamp1 {
		t.Fatal(timestampGet, timestamp1)
	}
	if blockIDGet != blockID1 {
		t.Fatal(blockIDGet, blockID1)
	}
	if offsetGet != offset1 {
		t.Fatal(offsetGet, offset1)
	}
	if lengthGet != length1 {
		t.Fatal(lengthGet, length1)
	}
}

func TestSetOverwriteKeyOldTimestampIsSameAndOverwriteWins(t *testing.T) {
	vlm := NewValueLocMap()
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp1 := uint64(2)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	vlm.Set(keyA, keyB, timestamp1, blockID1, offset1, length1, false)
	timestamp2 := timestamp1
	blockID2 := blockID1 + 1
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp := vlm.Set(keyA, keyB, timestamp2, blockID2, offset2, length2, true)
	if oldTimestamp != timestamp1 {
		t.Fatal(oldTimestamp, timestamp1)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != timestamp2 {
		t.Fatal(timestampGet, timestamp2)
	}
	if blockIDGet != blockID2 {
		t.Fatal(blockIDGet, blockID2)
	}
	if offsetGet != offset2 {
		t.Fatal(offsetGet, offset2)
	}
	if lengthGet != length2 {
		t.Fatal(lengthGet, length2)
	}
}

func TestSetOverflowingKeys(t *testing.T) {
	vlm := NewValueLocMap(OptRoots(1), OptPageSize(1))
	keyA1 := uint64(0)
	keyB1 := uint64(0)
	timestamp1 := uint64(2)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	oldTimestamp := vlm.Set(keyA1, keyB1, timestamp1, blockID1, offset1, length1, false)
	if oldTimestamp != 0 {
		t.Fatal(oldTimestamp)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA1, keyB1)
	if timestampGet != timestamp1 {
		t.Fatal(timestampGet, timestamp1)
	}
	if blockIDGet != blockID1 {
		t.Fatal(blockIDGet, blockID1)
	}
	if offsetGet != offset1 {
		t.Fatal(offsetGet, offset1)
	}
	if lengthGet != length1 {
		t.Fatal(lengthGet, length1)
	}
	keyA2 := uint64(0)
	keyB2 := uint64(2)
	timestamp2 := timestamp1 + 2
	blockID2 := blockID1 + 1
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp = vlm.Set(keyA2, keyB2, timestamp2, blockID2, offset2, length2, false)
	if oldTimestamp != 0 {
		t.Fatal(oldTimestamp)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet = vlm.Get(keyA2, keyB2)
	if timestampGet != timestamp2 {
		t.Fatal(timestampGet, timestamp2)
	}
	if blockIDGet != blockID2 {
		t.Fatal(blockIDGet, blockID2)
	}
	if offsetGet != offset2 {
		t.Fatal(offsetGet, offset2)
	}
	if lengthGet != length2 {
		t.Fatal(lengthGet, length2)
	}
}

func TestSetNewKeyBlockID0OldTimestampIs0AndNoEffect(t *testing.T) {
	vlm := NewValueLocMap()
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp := uint64(2)
	blockID := uint32(0)
	offset := uint32(4)
	length := uint32(5)
	oldTimestamp := vlm.Set(keyA, keyB, timestamp, blockID, offset, length, false)
	if oldTimestamp != 0 {
		t.Fatal(oldTimestamp)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != 0 {
		t.Fatal(timestampGet, 0)
	}
	if blockIDGet != 0 {
		t.Fatal(blockIDGet, 0)
	}
	if offsetGet != 0 {
		t.Fatal(offsetGet, 0)
	}
	if lengthGet != 0 {
		t.Fatal(lengthGet, 0)
	}
}

func TestSetOverwriteKeyBlockID0OldTimestampIsOldAndOverwriteWins(t *testing.T) {
	vlm := NewValueLocMap()
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp1 := uint64(2)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	vlm.Set(keyA, keyB, timestamp1, blockID1, offset1, length1, false)
	timestamp2 := timestamp1 + 2
	blockID2 := uint32(0)
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp := vlm.Set(keyA, keyB, timestamp2, blockID2, offset2, length2, false)
	if oldTimestamp != timestamp1 {
		t.Fatal(oldTimestamp, timestamp1)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != 0 {
		t.Fatal(timestampGet, 0)
	}
	if blockIDGet != 0 {
		t.Fatal(blockIDGet, 0)
	}
	if offsetGet != 0 {
		t.Fatal(offsetGet, 0)
	}
	if lengthGet != 0 {
		t.Fatal(lengthGet, 0)
	}
}

func TestSetOldOverwriteKeyBlockID0OldTimestampIsPreviousAndPreviousWins(t *testing.T) {
	vlm := NewValueLocMap()
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp1 := uint64(4)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	vlm.Set(keyA, keyB, timestamp1, blockID1, offset1, length1, false)
	timestamp2 := timestamp1 - 2
	blockID2 := uint32(0)
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp := vlm.Set(keyA, keyB, timestamp2, blockID2, offset2, length2, false)
	if oldTimestamp != timestamp1 {
		t.Fatal(oldTimestamp, timestamp1)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != timestamp1 {
		t.Fatal(timestampGet, timestamp1)
	}
	if blockIDGet != blockID1 {
		t.Fatal(blockIDGet, blockID1)
	}
	if offsetGet != offset1 {
		t.Fatal(offsetGet, offset1)
	}
	if lengthGet != length1 {
		t.Fatal(lengthGet, length1)
	}
}

func TestSetOverwriteKeyBlockID0OldTimestampIsSameAndOverwriteIgnored(t *testing.T) {
	vlm := NewValueLocMap()
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp1 := uint64(2)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	vlm.Set(keyA, keyB, timestamp1, blockID1, offset1, length1, false)
	timestamp2 := timestamp1
	blockID2 := uint32(0)
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp := vlm.Set(keyA, keyB, timestamp2, blockID2, offset2, length2, false)
	if oldTimestamp != timestamp1 {
		t.Fatal(oldTimestamp, timestamp1)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != timestamp1 {
		t.Fatal(timestampGet, timestamp1)
	}
	if blockIDGet != blockID1 {
		t.Fatal(blockIDGet, blockID1)
	}
	if offsetGet != offset1 {
		t.Fatal(offsetGet, offset1)
	}
	if lengthGet != length1 {
		t.Fatal(lengthGet, length1)
	}
}

func TestSetOverwriteKeyBlockID0OldTimestampIsSameAndOverwriteWins(t *testing.T) {
	vlm := NewValueLocMap()
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp1 := uint64(2)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	vlm.Set(keyA, keyB, timestamp1, blockID1, offset1, length1, false)
	timestamp2 := timestamp1
	blockID2 := uint32(0)
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp := vlm.Set(keyA, keyB, timestamp2, blockID2, offset2, length2, true)
	if oldTimestamp != timestamp1 {
		t.Fatal(oldTimestamp, timestamp1)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != 0 {
		t.Fatal(timestampGet, 0)
	}
	if blockIDGet != 0 {
		t.Fatal(blockIDGet, 0)
	}
	if offsetGet != 0 {
		t.Fatal(offsetGet, 0)
	}
	if lengthGet != 0 {
		t.Fatal(lengthGet, 0)
	}
}
