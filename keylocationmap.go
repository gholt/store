package brimstore

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

type keyLocationMap struct {
	a          *keyLocationMapSection
	b          *keyLocationMapSection
	bucketMask uint64
	lockMask   uint64
}

const (
	KEY_LOCATION_BLOCK_UNUSED    = iota
	KEY_LOCATION_BLOCK_TOMBSTONE = iota
	KEY_LOCATION_BLOCK_ID_OFFSET = iota
)

func newKeyLocationMap() *keyLocationMap {
	bucketCount := 1 << PowerOfTwoNeeded(128*1024*1024/uint64(unsafe.Sizeof(keyLocation{})))
	lockCount := 1 << PowerOfTwoNeeded(uint64(runtime.GOMAXPROCS(0)*runtime.GOMAXPROCS(0)))
	return &keyLocationMap{
		a:          newKeyLocationMapSection(bucketCount, lockCount),
		b:          newKeyLocationMapSection(bucketCount, lockCount),
		bucketMask: uint64(bucketCount) - 1,
		lockMask:   uint64(lockCount) - 1,
	}
}

func (klm *keyLocationMap) get(keyHashA uint64, keyHashB uint64) (uint16, uint32, uint64) {
	sectionMask := uint64(1) << 63
	bucketIndex := int(keyHashB & klm.bucketMask)
	lockIndex := int(keyHashB & klm.lockMask)
	if keyHashA&sectionMask == 0 {
		return klm.a.get(sectionMask, bucketIndex, lockIndex, keyHashA, keyHashB)
	} else {
		return klm.b.get(sectionMask, bucketIndex, lockIndex, keyHashA, keyHashB)
	}
}

func (klm *keyLocationMap) set(keyLocationBlockID uint16, offset uint32, keyHashA uint64, keyHashB uint64, seq uint64) {
	sectionMask := uint64(1) << 63
	bucketIndex := int(keyHashB & klm.bucketMask)
	lockIndex := int(keyHashB & klm.lockMask)
	if keyHashA&sectionMask == 0 {
		klm.a.set(sectionMask, bucketIndex, lockIndex, keyLocationBlockID, offset, keyHashA, keyHashB, seq)
	} else {
		klm.b.set(sectionMask, bucketIndex, lockIndex, keyLocationBlockID, offset, keyHashA, keyHashB, seq)
	}
}

func (klm *keyLocationMap) isResizing() bool {
	return klm.a.isResizing() || klm.b.isResizing()
}

type keyLocationMapSection struct {
	storageA   *keyLocationMapSectionStorage
	storageB   *keyLocationMapSectionStorage
	resizing   bool
	resizeLock sync.Mutex
	a          *keyLocationMapSection
	b          *keyLocationMapSection
}

func newKeyLocationMapSection(bucketCount int, lockCount int) *keyLocationMapSection {
	return &keyLocationMapSection{storageA: newKeyLocationMapSectionStorage(bucketCount, lockCount)}
}

func (klms *keyLocationMapSection) get(sectionMask uint64, bucketIndex int, lockIndex int, keyHashA uint64, keyHashB uint64) (uint16, uint32, uint64) {
	storageA := klms.storageA
	storageB := klms.storageB
	if storageA == nil {
		sectionMask >>= 1
		if keyHashA&sectionMask == 0 {
			return klms.a.get(sectionMask, bucketIndex, lockIndex, keyHashA, keyHashB)
		} else {
			return klms.b.get(sectionMask, bucketIndex, lockIndex, keyHashA, keyHashB)
		}
	} else if storageB == nil {
		return klms.getSingle(storageA, bucketIndex, lockIndex, keyHashA, keyHashB)
	} else {
		sectionMask >>= 1
		if keyHashA&sectionMask == 0 {
			return klms.getSingle(storageA, bucketIndex, lockIndex, keyHashA, keyHashB)
		} else {
			return klms.getWithFallback(storageB, storageA, bucketIndex, lockIndex, keyHashA, keyHashB)
		}
	}
}

func (klms *keyLocationMapSection) set(sectionMask uint64, bucketIndex int, lockIndex int, keyLocationBlockID uint16, offset uint32, keyHashA uint64, keyHashB uint64, seq uint64) {
	storageA := klms.storageA
	storageB := klms.storageB
	if storageA == nil {
		sectionMask >>= 1
		if keyHashA&sectionMask == 0 {
			klms.a.set(sectionMask, bucketIndex, lockIndex, keyLocationBlockID, offset, keyHashA, keyHashB, seq)
		} else {
			klms.b.set(sectionMask, bucketIndex, lockIndex, keyLocationBlockID, offset, keyHashA, keyHashB, seq)
		}
	} else if storageB == nil {
		count := klms.setSingle(storageA, bucketIndex, lockIndex, keyLocationBlockID, offset, keyHashA, keyHashB, seq)
		if count > int32(len(storageA.buckets)) {
			klms.split(sectionMask)
		}
	} else {
		sectionMask >>= 1
		if keyHashA&sectionMask == 0 {
			klms.setSingle(storageA, bucketIndex, lockIndex, keyLocationBlockID, offset, keyHashA, keyHashB, seq)
		} else {
			klms.setWithFallback(storageB, storageA, bucketIndex, lockIndex, keyLocationBlockID, offset, keyHashA, keyHashB, seq)
		}
	}
}

func (klms *keyLocationMapSection) getSingle(storage *keyLocationMapSectionStorage, bucketIndex int, lockIndex int, keyHashA uint64, keyHashB uint64) (uint16, uint32, uint64) {
	storage.locks[lockIndex].RLock()
	for item := &storage.buckets[bucketIndex]; item != nil; item = item.next {
		if item.keyLocationBlockID != KEY_LOCATION_BLOCK_UNUSED && item.keyHashA == keyHashA && item.keyHashB == keyHashB {
			storage.locks[lockIndex].RUnlock()
			return item.keyLocationBlockID, item.offset, item.seq
		}
	}
	storage.locks[lockIndex].RUnlock()
	return KEY_LOCATION_BLOCK_UNUSED, 0, 0
}

func (klms *keyLocationMapSection) setSingle(storage *keyLocationMapSectionStorage, bucketIndex int, lockIndex int, keyLocationBlockID uint16, offset uint32, keyHashA uint64, keyHashB uint64, seq uint64) int32 {
	var count int32
	var done bool
	var unusedItem *keyLocation
	storage.locks[lockIndex].Lock()
	for item := &storage.buckets[bucketIndex]; item != nil; item = item.next {
		if item.keyLocationBlockID == KEY_LOCATION_BLOCK_UNUSED {
			if unusedItem == nil {
				unusedItem = item
			}
			continue
		}
		if item.keyHashA == keyHashA && item.keyHashB == keyHashB {
			if keyLocationBlockID == KEY_LOCATION_BLOCK_UNUSED {
				if item.seq == seq {
					count = atomic.AddInt32(&storage.count, -1)
					item.keyLocationBlockID = KEY_LOCATION_BLOCK_UNUSED
				}
			} else if item.seq <= seq {
				item.keyLocationBlockID = keyLocationBlockID
				item.offset = offset
				item.seq = seq
			}
			done = true
			break
		}
	}
	if !done && keyLocationBlockID != KEY_LOCATION_BLOCK_UNUSED {
		count = atomic.AddInt32(&storage.count, 1)
		if unusedItem != nil {
			unusedItem.keyLocationBlockID = keyLocationBlockID
			unusedItem.offset = offset
			unusedItem.keyHashA = keyHashA
			unusedItem.keyHashB = keyHashB
			unusedItem.seq = seq
		} else {
			storage.buckets[bucketIndex].next = &keyLocation{
				keyLocationBlockID: keyLocationBlockID,
				offset:             offset,
				keyHashA:           keyHashA,
				keyHashB:           keyHashB,
				seq:                seq,
				next:               storage.buckets[bucketIndex].next,
			}
		}
	}
	storage.locks[lockIndex].Unlock()
	return count
}

func (klms *keyLocationMapSection) getWithFallback(storage *keyLocationMapSectionStorage, fallback *keyLocationMapSectionStorage, bucketIndex int, lockIndex int, keyHashA uint64, keyHashB uint64) (uint16, uint32, uint64) {
	storage.locks[lockIndex].RLock()
	fallback.locks[lockIndex].RLock()
	for item := &storage.buckets[bucketIndex]; item != nil; item = item.next {
		if item.keyLocationBlockID != KEY_LOCATION_BLOCK_UNUSED && item.keyHashA == keyHashA && item.keyHashB == keyHashB {
			fallback.locks[lockIndex].RUnlock()
			storage.locks[lockIndex].RUnlock()
			return item.keyLocationBlockID, item.offset, item.seq
		}
	}
	for item := &fallback.buckets[bucketIndex]; item != nil; item = item.next {
		if item.keyLocationBlockID != KEY_LOCATION_BLOCK_UNUSED && item.keyHashA == keyHashA && item.keyHashB == keyHashB {
			fallback.locks[lockIndex].RUnlock()
			storage.locks[lockIndex].RUnlock()
			return item.keyLocationBlockID, item.offset, item.seq
		}
	}
	fallback.locks[lockIndex].RUnlock()
	storage.locks[lockIndex].RUnlock()
	return KEY_LOCATION_BLOCK_UNUSED, 0, 0
}

func (klms *keyLocationMapSection) setWithFallback(storage *keyLocationMapSectionStorage, fallback *keyLocationMapSectionStorage, bucketIndex int, lockIndex int, keyLocationBlockID uint16, offset uint32, keyHashA uint64, keyHashB uint64, seq uint64) {
	var done bool
	var unusedItem *keyLocation
	storage.locks[lockIndex].Lock()
	fallback.locks[lockIndex].Lock()
	for item := &storage.buckets[bucketIndex]; item != nil; item = item.next {
		if item.keyLocationBlockID == KEY_LOCATION_BLOCK_UNUSED {
			if unusedItem == nil {
				unusedItem = item
			}
			continue
		}
		if item.keyHashA == keyHashA && item.keyHashB == keyHashB {
			if keyLocationBlockID == KEY_LOCATION_BLOCK_UNUSED {
				if item.seq == seq {
					atomic.AddInt32(&storage.count, -1)
					item.keyLocationBlockID = KEY_LOCATION_BLOCK_UNUSED
				}
			} else if item.seq <= seq {
				item.keyLocationBlockID = keyLocationBlockID
				item.offset = offset
				item.seq = seq
			}
			done = true
			break
		}
	}
	if !done {
		for fallbackItem := &fallback.buckets[bucketIndex]; fallbackItem != nil; fallbackItem = fallbackItem.next {
			if fallbackItem.keyLocationBlockID == KEY_LOCATION_BLOCK_UNUSED {
				continue
			}
			if fallbackItem.keyHashA == keyHashA && fallbackItem.keyHashB == keyHashB {
				if keyLocationBlockID == KEY_LOCATION_BLOCK_UNUSED {
					if fallbackItem.seq == seq {
						atomic.AddInt32(&fallback.count, -1)
						fallbackItem.keyLocationBlockID = KEY_LOCATION_BLOCK_UNUSED
					}
					done = true
				} else if fallbackItem.seq <= seq {
					atomic.AddInt32(&fallback.count, -1)
					fallbackItem.keyLocationBlockID = KEY_LOCATION_BLOCK_UNUSED
				} else {
					done = true
				}
				break
			}
		}
	}
	if !done && keyLocationBlockID != KEY_LOCATION_BLOCK_UNUSED {
		atomic.AddInt32(&storage.count, 1)
		if unusedItem != nil {
			unusedItem.keyLocationBlockID = keyLocationBlockID
			unusedItem.offset = offset
			unusedItem.keyHashA = keyHashA
			unusedItem.keyHashB = keyHashB
			unusedItem.seq = seq
		} else {
			storage.buckets[bucketIndex].next = &keyLocation{
				keyLocationBlockID: keyLocationBlockID,
				offset:             offset,
				keyHashA:           keyHashA,
				keyHashB:           keyHashB,
				seq:                seq,
				next:               storage.buckets[bucketIndex].next,
			}
		}
	}
	fallback.locks[lockIndex].Unlock()
	storage.locks[lockIndex].Unlock()
}

func (klms *keyLocationMapSection) isResizing() bool {
	return klms.resizing || (klms.a != nil && klms.a.isResizing()) || (klms.b != nil && klms.b.isResizing())
}

func (klms *keyLocationMapSection) split(sectionMask uint64) {
	if klms.resizing {
		return
	}
	klms.resizeLock.Lock()
	storageA := klms.storageA
	storageB := klms.storageB
	if klms.resizing || storageA == nil || storageB != nil || atomic.LoadInt32(&storageA.count) < int32(len(storageA.buckets)) {
		klms.resizeLock.Unlock()
		return
	}
	klms.resizing = true
	klms.resizeLock.Unlock()
	storageB = newKeyLocationMapSectionStorage(len(storageA.buckets), len(storageA.locks))
	klms.storageB = storageB
	sectionMask >>= 1
	lockMask := len(storageA.locks) - 1
	cores := runtime.GOMAXPROCS(0)
	wg := &sync.WaitGroup{}
	wg.Add(cores)
	for core := 0; core < cores; core++ {
		go func(coreOffset int) {
			clean := false
			for !clean {
				clean = true
				for bucketIndex := len(storageA.buckets) - 1 - coreOffset; bucketIndex >= 0; bucketIndex -= cores {
					lockIndex := bucketIndex & lockMask
					storageB.locks[lockIndex].Lock()
					storageA.locks[lockIndex].Lock()
				NEXT_ITEM_A:
					for itemA := &storageA.buckets[bucketIndex]; itemA != nil; itemA = itemA.next {
						if itemA.keyLocationBlockID == KEY_LOCATION_BLOCK_UNUSED || itemA.keyHashA&sectionMask == 0 {
							continue
						}
						clean = false
						var unusedItemB *keyLocation
						for itemB := &storageB.buckets[bucketIndex]; itemB != nil; itemB = itemB.next {
							if itemB.keyLocationBlockID == KEY_LOCATION_BLOCK_UNUSED {
								if unusedItemB == nil {
									unusedItemB = itemB
								}
								continue
							}
							if itemA.keyHashA == itemB.keyHashA && itemA.keyHashB == itemB.keyHashB {
								if itemA.seq > itemB.seq {
									itemB.keyLocationBlockID = itemA.keyLocationBlockID
									itemB.offset = itemA.offset
									itemB.keyHashA = itemA.keyHashA
									itemB.keyHashB = itemA.keyHashB
									itemB.seq = itemA.seq
								}
								atomic.AddInt32(&storageA.count, -1)
								itemA.keyLocationBlockID = KEY_LOCATION_BLOCK_UNUSED
								continue NEXT_ITEM_A
							}
						}
						atomic.AddInt32(&storageB.count, 1)
						if unusedItemB != nil {
							unusedItemB.keyLocationBlockID = itemA.keyLocationBlockID
							unusedItemB.offset = itemA.offset
							unusedItemB.keyHashA = itemA.keyHashA
							unusedItemB.keyHashB = itemA.keyHashB
							unusedItemB.seq = itemA.seq
						} else {
							storageB.buckets[bucketIndex].next = &keyLocation{
								keyLocationBlockID: itemA.keyLocationBlockID,
								offset:             itemA.offset,
								keyHashA:           itemA.keyHashA,
								keyHashB:           itemA.keyHashB,
								seq:                itemA.seq,
								next:               storageB.buckets[bucketIndex].next,
							}
						}
						atomic.AddInt32(&storageA.count, -1)
						itemA.keyLocationBlockID = 0
					}
					storageA.locks[lockIndex].Unlock()
					storageB.locks[lockIndex].Unlock()
				}
			}
			wg.Done()
		}(core)
	}
	wg.Wait()
	klms.a = &keyLocationMapSection{storageA: storageA}
	klms.b = &keyLocationMapSection{storageA: storageB}
	klms.storageA = nil
	klms.storageB = nil
	klms.resizing = false
}

type keyLocationMapSectionStorage struct {
	// The first level of each bucket is preallocated with a keyLocation rather
	// than a *keyLocation. This trades memory usage for keeping the Go garbage
	// collector sane.
	buckets []keyLocation
	count   int32
	locks   []sync.RWMutex
}

func newKeyLocationMapSectionStorage(bucketCount int, lockCount int) *keyLocationMapSectionStorage {
	return &keyLocationMapSectionStorage{
		buckets: make([]keyLocation, bucketCount),
		locks:   make([]sync.RWMutex, lockCount),
	}
}

// Each keyLocation uses 2+4+8+8+8+8 = 38 bytes (assuming 64 bit pointer) which
// means we can store ~28,256,364 key locations in 1G of memory or
// 1,808,407,296 key locations in 64G of memory (half the memory of our test
// 128G machine).
type keyLocation struct {
	// 0 is reserved for "not set", so a max of 65,355 IDs can be used. Each
	// block can be up to 4G in size based on the offset as uint32, so a total
	// addressable space of almost 256T.
	keyLocationBlockID uint16
	offset             uint32
	keyHashA           uint64
	keyHashB           uint64
	seq                uint64
	next               *keyLocation
}
