package brimstore

import (
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/gholt/brimutil"
)

type valuesLocMap struct {
	a          *valuesLocMapSection
	b          *valuesLocMapSection
	bucketMask uint64
	lockMask   uint64
}

const (
	_VALUESBLOCK_UNUSED   = iota
	_VALUESBLOCK_TOMB     = iota
	_VALUESBLOCK_IDOFFSET = iota
)

func newValuesLocMap(opts *ValuesStoreOpts) *valuesLocMap {
	if opts == nil {
		opts = NewValuesStoreOpts()
	}
	cores := opts.Cores
	if cores < 1 {
		cores = 1
	}
	valuesLocMapPageSize := opts.ValuesLocMapPageSize
	if env := os.Getenv("BRIMSTORE_VALUESSTORE_VALUESLOCMAP_PAGESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			valuesLocMapPageSize = val
		}
	}
	if valuesLocMapPageSize < 4096 {
		valuesLocMapPageSize = 4096
	}
	bucketCount := 1 << brimutil.PowerOfTwoNeeded(uint64(valuesLocMapPageSize)/uint64(unsafe.Sizeof(valueLoc{})))
	lockCount := 1 << brimutil.PowerOfTwoNeeded(uint64(cores*cores))
	if lockCount > bucketCount {
		lockCount = bucketCount
	}
	return &valuesLocMap{
		a:          newValuesLocMapSection(bucketCount, lockCount),
		b:          newValuesLocMapSection(bucketCount, lockCount),
		bucketMask: uint64(bucketCount) - 1,
		lockMask:   uint64(lockCount) - 1,
	}
}

func (vlm *valuesLocMap) get(keyA uint64, keyB uint64) (uint64, uint16, uint32, uint32) {
	sectionMask := uint64(1) << 63
	bucketIndex := int(keyB & vlm.bucketMask)
	lockIndex := int(keyB & vlm.lockMask)
	if keyA&sectionMask == 0 {
		return vlm.a.get(sectionMask, bucketIndex, lockIndex, keyA, keyB)
	} else {
		return vlm.b.get(sectionMask, bucketIndex, lockIndex, keyA, keyB)
	}
}

func (vlm *valuesLocMap) set(keyA uint64, keyB uint64, seq uint64, valuesLocBlockID uint16, offset uint32, length uint32, evenIfSameSeq bool) uint64 {
	sectionMask := uint64(1) << 63
	bucketIndex := int(keyB & vlm.bucketMask)
	lockIndex := int(keyB & vlm.lockMask)
	if keyA&sectionMask == 0 {
		return vlm.a.set(sectionMask, bucketIndex, lockIndex, keyA, keyB, seq, valuesLocBlockID, offset, length, evenIfSameSeq)
	} else {
		return vlm.b.set(sectionMask, bucketIndex, lockIndex, keyA, keyB, seq, valuesLocBlockID, offset, length, evenIfSameSeq)
	}
}

func (vlm *valuesLocMap) isResizing() bool {
	return vlm.a.isResizing() || vlm.b.isResizing()
}

type valuesLocMapStats struct {
	depth    uint64
	sections uint64
	storages uint64
	locs     uint64
	unused   uint64
	tombs    uint64
	used     uint64
	length   uint64
}

func (vlm *valuesLocMap) gatherStats() *valuesLocMapStats {
	stats := &valuesLocMapStats{}
	stats.depth++
	vlm.a.gatherStats(stats)
	vlm.b.gatherStats(stats)
	return stats
}

type valuesLocMapSection struct {
	storageA   *valuesLocMapSectionStorage
	storageB   *valuesLocMapSectionStorage
	resizing   bool
	resizeLock sync.Mutex
	a          *valuesLocMapSection
	b          *valuesLocMapSection
}

func newValuesLocMapSection(bucketCount int, lockCount int) *valuesLocMapSection {
	return &valuesLocMapSection{storageA: newValuesLocMapSectionStorage(bucketCount, lockCount)}
}

func (vlms *valuesLocMapSection) get(sectionMask uint64, bucketIndex int, lockIndex int, keyA uint64, keyB uint64) (uint64, uint16, uint32, uint32) {
	storageA := vlms.storageA
	storageB := vlms.storageB
	if storageA == nil {
		sectionMask >>= 1
		if keyA&sectionMask == 0 {
			return vlms.a.get(sectionMask, bucketIndex, lockIndex, keyA, keyB)
		} else {
			return vlms.b.get(sectionMask, bucketIndex, lockIndex, keyA, keyB)
		}
	} else if storageB == nil {
		return vlms.getSingle(storageA, bucketIndex, lockIndex, keyA, keyB)
	} else {
		sectionMask >>= 1
		if keyA&sectionMask == 0 {
			return vlms.getSingle(storageA, bucketIndex, lockIndex, keyA, keyB)
		} else {
			return vlms.getWithFallback(storageB, storageA, bucketIndex, lockIndex, keyA, keyB)
		}
	}
}

func (vlms *valuesLocMapSection) set(sectionMask uint64, bucketIndex int, lockIndex int, keyA uint64, keyB uint64, seq uint64, valuesLocBlockID uint16, offset uint32, length uint32, evenIfSameSeq bool) uint64 {
	storageA := vlms.storageA
	storageB := vlms.storageB
	if storageA == nil {
		sectionMask >>= 1
		if keyA&sectionMask == 0 {
			return vlms.a.set(sectionMask, bucketIndex, lockIndex, keyA, keyB, seq, valuesLocBlockID, offset, length, evenIfSameSeq)
		} else {
			return vlms.b.set(sectionMask, bucketIndex, lockIndex, keyA, keyB, seq, valuesLocBlockID, offset, length, evenIfSameSeq)
		}
	} else if storageB == nil {
		seq, bucketsInUse := vlms.setSingle(storageA, bucketIndex, lockIndex, keyA, keyB, seq, valuesLocBlockID, offset, length, evenIfSameSeq)
		if bucketsInUse > int32(len(storageA.buckets)) {
			go vlms.split(sectionMask)
		}
		return seq
	} else {
		sectionMask >>= 1
		if keyA&sectionMask == 0 {
			seq, _ := vlms.setSingle(storageA, bucketIndex, lockIndex, keyA, keyB, seq, valuesLocBlockID, offset, length, evenIfSameSeq)
			return seq
		} else {
			return vlms.setWithFallback(storageB, storageA, bucketIndex, lockIndex, keyA, keyB, seq, valuesLocBlockID, offset, length, evenIfSameSeq)
		}
	}
}

func (vlms *valuesLocMapSection) getSingle(storage *valuesLocMapSectionStorage, bucketIndex int, lockIndex int, keyA uint64, keyB uint64) (uint64, uint16, uint32, uint32) {
	storage.locks[lockIndex].RLock()
	for item := &storage.buckets[bucketIndex]; item != nil; item = item.next {
		if item.valuesLocBlockID != _VALUESBLOCK_UNUSED && item.keyA == keyA && item.keyB == keyB {
			q, i, o, z := item.seq, item.valuesLocBlockID, item.offset, item.length
			storage.locks[lockIndex].RUnlock()
			return q, i, o, z
		}
	}
	storage.locks[lockIndex].RUnlock()
	return 0, _VALUESBLOCK_UNUSED, 0, 0
}

func (vlms *valuesLocMapSection) setSingle(storage *valuesLocMapSectionStorage, bucketIndex int, lockIndex int, keyA uint64, keyB uint64, seq uint64, valuesLocBlockID uint16, offset uint32, length uint32, evenIfSameSeq bool) (uint64, int32) {
	var oldSeq uint64
	var bucketsInUse int32
	var done bool
	var unusedItem *valueLoc
	storage.locks[lockIndex].Lock()
	for item := &storage.buckets[bucketIndex]; item != nil; item = item.next {
		if item.valuesLocBlockID == _VALUESBLOCK_UNUSED {
			if unusedItem == nil {
				unusedItem = item
			}
			continue
		}
		if item.keyA == keyA && item.keyB == keyB {
			oldSeq = item.seq
			if (evenIfSameSeq && item.seq == seq) || item.seq < seq {
				if valuesLocBlockID == _VALUESBLOCK_UNUSED {
					bucketsInUse = atomic.AddInt32(&storage.atBucketsInUse, -1)
				} else {
					bucketsInUse = atomic.LoadInt32(&storage.atBucketsInUse)
				}
				item.seq = seq
				item.valuesLocBlockID = valuesLocBlockID
				item.offset = offset
				item.length = length
			} else {
				bucketsInUse = atomic.LoadInt32(&storage.atBucketsInUse)
			}
			done = true
			break
		}
	}
	if !done && valuesLocBlockID != _VALUESBLOCK_UNUSED {
		bucketsInUse = atomic.AddInt32(&storage.atBucketsInUse, 1)
		if unusedItem != nil {
			unusedItem.keyA = keyA
			unusedItem.keyB = keyB
			unusedItem.seq = seq
			unusedItem.valuesLocBlockID = valuesLocBlockID
			unusedItem.offset = offset
			unusedItem.length = length
		} else {
			storage.buckets[bucketIndex].next = &valueLoc{
				next:             storage.buckets[bucketIndex].next,
				keyA:             keyA,
				keyB:             keyB,
				seq:              seq,
				valuesLocBlockID: valuesLocBlockID,
				offset:           offset,
				length:           length,
			}
		}
	}
	storage.locks[lockIndex].Unlock()
	return oldSeq, bucketsInUse
}

func (vlms *valuesLocMapSection) getWithFallback(storage *valuesLocMapSectionStorage, fallback *valuesLocMapSectionStorage, bucketIndex int, lockIndex int, keyA uint64, keyB uint64) (uint64, uint16, uint32, uint32) {
	storage.locks[lockIndex].RLock()
	fallback.locks[lockIndex].RLock()
	for item := &storage.buckets[bucketIndex]; item != nil; item = item.next {
		if item.valuesLocBlockID != _VALUESBLOCK_UNUSED && item.keyA == keyA && item.keyB == keyB {
			q, i, o, z := item.seq, item.valuesLocBlockID, item.offset, item.length
			fallback.locks[lockIndex].RUnlock()
			storage.locks[lockIndex].RUnlock()
			return q, i, o, z
		}
	}
	for item := &fallback.buckets[bucketIndex]; item != nil; item = item.next {
		if item.valuesLocBlockID != _VALUESBLOCK_UNUSED && item.keyA == keyA && item.keyB == keyB {
			q, i, o, z := item.seq, item.valuesLocBlockID, item.offset, item.length
			fallback.locks[lockIndex].RUnlock()
			storage.locks[lockIndex].RUnlock()
			return q, i, o, z
		}
	}
	fallback.locks[lockIndex].RUnlock()
	storage.locks[lockIndex].RUnlock()
	return 0, _VALUESBLOCK_UNUSED, 0, 0
}

func (vlms *valuesLocMapSection) setWithFallback(storage *valuesLocMapSectionStorage, fallback *valuesLocMapSectionStorage, bucketIndex int, lockIndex int, keyA uint64, keyB uint64, seq uint64, valuesLocBlockID uint16, offset uint32, length uint32, evenIfSameSeq bool) uint64 {
	var oldSeq uint64
	var done bool
	var unusedItem *valueLoc
	storage.locks[lockIndex].Lock()
	fallback.locks[lockIndex].Lock()
	for item := &storage.buckets[bucketIndex]; item != nil; item = item.next {
		if item.valuesLocBlockID == _VALUESBLOCK_UNUSED {
			if unusedItem == nil {
				unusedItem = item
			}
			continue
		}
		if item.keyA == keyA && item.keyB == keyB {
			oldSeq = item.seq
			if (evenIfSameSeq && item.seq == seq) || item.seq < seq {
				if valuesLocBlockID == _VALUESBLOCK_UNUSED {
					atomic.AddInt32(&storage.atBucketsInUse, -1)
				}
				item.seq = seq
				item.valuesLocBlockID = valuesLocBlockID
				item.offset = offset
				item.length = length
			}
			done = true
			break
		}
	}
	if !done {
		for fallbackItem := &fallback.buckets[bucketIndex]; fallbackItem != nil; fallbackItem = fallbackItem.next {
			if fallbackItem.valuesLocBlockID == _VALUESBLOCK_UNUSED {
				continue
			}
			if fallbackItem.keyA == keyA && fallbackItem.keyB == keyB {
				oldSeq = fallbackItem.seq
				if (evenIfSameSeq && fallbackItem.seq == seq) || fallbackItem.seq < seq {
					atomic.AddInt32(&fallback.atBucketsInUse, -1)
					fallbackItem.valuesLocBlockID = _VALUESBLOCK_UNUSED
				} else {
					done = true
				}
				break
			}
		}
	}
	if !done && valuesLocBlockID != _VALUESBLOCK_UNUSED {
		atomic.AddInt32(&storage.atBucketsInUse, 1)
		if unusedItem != nil {
			unusedItem.keyA = keyA
			unusedItem.keyB = keyB
			unusedItem.seq = seq
			unusedItem.valuesLocBlockID = valuesLocBlockID
			unusedItem.offset = offset
			unusedItem.length = length
		} else {
			storage.buckets[bucketIndex].next = &valueLoc{
				next:             storage.buckets[bucketIndex].next,
				keyA:             keyA,
				keyB:             keyB,
				seq:              seq,
				valuesLocBlockID: valuesLocBlockID,
				offset:           offset,
				length:           length,
			}
		}
	}
	fallback.locks[lockIndex].Unlock()
	storage.locks[lockIndex].Unlock()
	return oldSeq
}

func (vlms *valuesLocMapSection) isResizing() bool {
	a := vlms.a
	b := vlms.b
	return vlms.resizing || (a != nil && a.isResizing()) || (b != nil && b.isResizing())
}

func (vlms *valuesLocMapSection) gatherStats(stats *valuesLocMapStats) {
	stats.sections++
	storageA := vlms.storageA
	storageB := vlms.storageB
	a := vlms.a
	b := vlms.b
	if storageA != nil {
		vlms.gatherStatsFromStorage(storageA, stats)
	}
	if storageB != nil {
		vlms.gatherStatsFromStorage(storageB, stats)
	}
	if a != nil || b != nil {
		stats.depth++
		if a != nil {
			a.gatherStats(stats)
		}
		if b != nil {
			b.gatherStats(stats)
		}
	}
}

func (vlms *valuesLocMapSection) gatherStatsFromStorage(storage *valuesLocMapSectionStorage, stats *valuesLocMapStats) {
	stats.storages++
	lockMask := len(storage.locks) - 1
	for bucketIndex := len(storage.buckets) - 1; bucketIndex >= 0; bucketIndex-- {
		lockIndex := bucketIndex & lockMask
		storage.locks[lockIndex].RLock()
		for item := &storage.buckets[bucketIndex]; item != nil; item = item.next {
			stats.locs++
			switch item.valuesLocBlockID {
			case _VALUESBLOCK_UNUSED:
				stats.unused++
			case _VALUESBLOCK_TOMB:
				stats.tombs++
			default:
				stats.used++
				stats.length += uint64(item.length)
			}
		}
		storage.locks[lockIndex].RUnlock()
	}
}

func (vlms *valuesLocMapSection) split(sectionMask uint64) {
	if vlms.resizing {
		return
	}
	vlms.resizeLock.Lock()
	storageA := vlms.storageA
	storageB := vlms.storageB
	if vlms.resizing || storageA == nil || storageB != nil || atomic.LoadInt32(&storageA.atBucketsInUse) < int32(len(storageA.buckets)) {
		vlms.resizeLock.Unlock()
		return
	}
	vlms.resizing = true
	vlms.resizeLock.Unlock()
	storageB = newValuesLocMapSectionStorage(len(storageA.buckets), len(storageA.locks))
	vlms.storageB = storageB
	sectionMask >>= 1
	lockMask := len(storageA.locks) - 1
	cores := int(math.Sqrt(float64(len(storageA.locks))))
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
						if itemA.valuesLocBlockID == _VALUESBLOCK_UNUSED || itemA.keyA&sectionMask == 0 {
							continue
						}
						clean = false
						var unusedItemB *valueLoc
						for itemB := &storageB.buckets[bucketIndex]; itemB != nil; itemB = itemB.next {
							if itemB.valuesLocBlockID == _VALUESBLOCK_UNUSED {
								if unusedItemB == nil {
									unusedItemB = itemB
								}
								continue
							}
							if itemA.keyA == itemB.keyA && itemA.keyB == itemB.keyB {
								if itemA.seq > itemB.seq {
									itemB.keyA = itemA.keyA
									itemB.keyB = itemA.keyB
									itemB.seq = itemA.seq
									itemB.valuesLocBlockID = itemA.valuesLocBlockID
									itemB.offset = itemA.offset
									itemB.length = itemA.length
								}
								atomic.AddInt32(&storageA.atBucketsInUse, -1)
								itemA.valuesLocBlockID = _VALUESBLOCK_UNUSED
								continue NEXT_ITEM_A
							}
						}
						atomic.AddInt32(&storageB.atBucketsInUse, 1)
						if unusedItemB != nil {
							unusedItemB.keyA = itemA.keyA
							unusedItemB.keyB = itemA.keyB
							unusedItemB.seq = itemA.seq
							unusedItemB.valuesLocBlockID = itemA.valuesLocBlockID
							unusedItemB.offset = itemA.offset
							unusedItemB.length = itemA.length
						} else {
							storageB.buckets[bucketIndex].next = &valueLoc{
								next:             storageB.buckets[bucketIndex].next,
								keyA:             itemA.keyA,
								keyB:             itemA.keyB,
								seq:              itemA.seq,
								valuesLocBlockID: itemA.valuesLocBlockID,
								offset:           itemA.offset,
								length:           itemA.length,
							}
						}
						atomic.AddInt32(&storageA.atBucketsInUse, -1)
						itemA.valuesLocBlockID = 0
					}
					storageA.locks[lockIndex].Unlock()
					storageB.locks[lockIndex].Unlock()
				}
			}
			wg.Done()
		}(core)
	}
	wg.Wait()
	vlms.a = &valuesLocMapSection{storageA: storageA}
	vlms.b = &valuesLocMapSection{storageA: storageB}
	vlms.storageA = nil
	vlms.storageB = nil
	vlms.resizing = false
}

type valuesLocMapSectionStorage struct {
	// The first level of each bucket is preallocated with a valueLoc rather
	// than a *valueLoc. This trades memory usage for keeping the Go garbage
	// collector sane.
	buckets        []valueLoc
	locks          []sync.RWMutex
	atBucketsInUse int32
}

func newValuesLocMapSectionStorage(bucketCount int, lockCount int) *valuesLocMapSectionStorage {
	return &valuesLocMapSectionStorage{
		buckets: make([]valueLoc, bucketCount),
		locks:   make([]sync.RWMutex, lockCount),
	}
}

// Each valueLoc uses 8+8+8+8+2+4+4 = 42 bytes (assuming 64 bit pointer) which
// means we can store ~25,565,281 key locations in 1G of memory or
// 1,636,178,017 key locations in 64G of memory (half the memory of our test
// 128G machine).
type valueLoc struct {
	next *valueLoc
	keyA uint64
	keyB uint64
	seq  uint64
	// 0 is reserved for "not set" and 1 is for a tombstone, so a max of 65,354
	// IDs can be used. Each block can be up to 4G in size based on the offset
	// as uint32, so a total addressable space of almost 256T.
	valuesLocBlockID uint16
	offset           uint32
	length           uint32
}
