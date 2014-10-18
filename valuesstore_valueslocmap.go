package brimstore

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/gholt/brimtext"
)

// a is used to store at first, growth may then cause a split.
// While splitting, b will be set, c and d will still be nil.
// Once the split is complete, c and d will be set.
// Shrinking may cause an unsplit.
// During unsplit, a and e will be set, c and d will become nil.
// e is considered read-only/fallback during unsplit.
// Once unsplit is done, e will become nil.
//
// FOR SPEED'S SAKE THERE IS A MAJOR ASSUMPTION THAT ALL READS AND WRITES
// ACTIVE AT THE START OF ONE RESIZE WILL BE COMPLETED BEFORE ANOTHER RESIZE OF
// THE SAME KEY SPACE HAPPENS.
//
// As one example, if a write starts, gathers a and b (split in progress), and
// somehow is stalled for an extended period of time and the split completes
// and another subsplit happens, then the write awakens and continues, it will
// have references to quite possibly incorrect memory areas.
//
// This code is not meant to be used with over-subscribed core counts that
// would create artificial goroutine slowdowns / starvations or with extremely
// small memory page sizes. In the rare case a single core on a CPU is going
// bad and is running slow, this code should still be safe as it is meant to be
// run with the data stored on multiple server replicas where if one server is
// behaving badly the other servers will supersede it.
//
// If you would rather have perfect correctness at the cost of speed, you will
// have to use an additional lock around all uses of a-e.
type valuesLocMap struct {
	leftMask     uint64
	rangeStart   uint64
	rangeStop    uint64
	a            *valuesLocStore
	b            *valuesLocStore
	c            *valuesLocMap
	d            *valuesLocMap
	e            *valuesLocStore
	resizing     bool
	resizingLock sync.RWMutex
	cores        int
	splitCount   int
}

type valuesLocStore struct {
	buckets []valueLoc
	locks   []sync.RWMutex
	used    int32
}

type valueLoc struct {
	next      *valueLoc
	keyA      uint64
	keyB      uint64
	timestamp uint64
	blockID   uint16
	offset    uint32
	length    uint32
}

type valuesLocMapStats struct {
	extended     bool
	wg           sync.WaitGroup
	depth        uint64
	depthCounts  []uint64
	sections     uint64
	storages     uint64
	buckets      uint64
	bucketCounts []uint64
	splitCount   uint64
	locs         uint64
	pointerLocs  uint64
	unused       uint64
	used         uint64
	active       uint64
	length       uint64
	tombstones   uint64
}

type valuesLocMapBackground struct {
	wg                  sync.WaitGroup
	tombstoneCutoff     uint64
	tombstonesDiscarded uint64
	tombstonesRetained  uint64
}

func newValuesLocMap(opts *ValuesStoreOpts) *valuesLocMap {
	if opts == nil {
		opts = NewValuesStoreOpts("")
	}
	cores := opts.Cores
	if cores < 1 {
		cores = 1
	}
	valuesLocMapPageSize := opts.ValuesLocMapPageSize
	if valuesLocMapPageSize < 4096 {
		valuesLocMapPageSize = 4096
	}
	bucketCount := valuesLocMapPageSize / int(unsafe.Sizeof(valueLoc{}))
	lockCount := cores
	if lockCount > bucketCount {
		lockCount = bucketCount
	}
	splitMultiplier := opts.ValuesLocMapSplitMultiplier
	if splitMultiplier <= 0 {
		splitMultiplier = 3.0
	}
	vlm := &valuesLocMap{
		leftMask:   uint64(1) << 63,
		rangeStart: 0,
		rangeStop:  math.MaxUint64,
		cores:      cores,
		splitCount: int(float64(bucketCount) * splitMultiplier),
	}
	a := &valuesLocStore{
		buckets: make([]valueLoc, bucketCount),
		locks:   make([]sync.RWMutex, lockCount),
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.a)), unsafe.Pointer(a))
	return vlm
}

func (vlm *valuesLocMap) get(keyA uint64, keyB uint64) (uint64, uint16, uint32, uint32) {
	var timestamp uint64
	var blockID uint16
	var offset uint32
	var length uint32
	for {
		c := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.c))))
		if c == nil {
			break
		}
		if keyA&vlm.leftMask == 0 {
			vlm = c
		} else {
			vlm = (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.d))))
		}
	}
	a := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.a))))
	bix := keyB % uint64(len(a.buckets))
	lix := bix % uint64(len(a.locks))
	f := func(s *valuesLocStore, fb *valuesLocStore) {
		blockID = 0
		s.locks[lix].RLock()
		if fb != nil {
			fb.locks[lix].RLock()
		}
		for item := &s.buckets[bix]; item != nil; item = item.next {
			if item.blockID != 0 && item.keyA == keyA && item.keyB == keyB {
				timestamp, blockID, offset, length = item.timestamp, item.blockID, item.offset, item.length
				break
			}
		}
		if fb != nil && blockID == 0 {
			for item := &fb.buckets[bix]; item != nil; item = item.next {
				if item.blockID != 0 && item.keyA == keyA && item.keyB == keyB {
					timestamp, blockID, offset, length = item.timestamp, item.blockID, item.offset, item.length
					break
				}
			}
		}
		if fb != nil {
			fb.locks[lix].RUnlock()
		}
		s.locks[lix].RUnlock()
	}
	if keyA&vlm.leftMask == 0 {
		f(a, (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.e)))))
	} else {
		b := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.b))))
		if b != nil {
			f(b, a)
		} else {
			f(a, (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.e)))))
			b := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.b))))
			if b != nil {
				f(b, a)
			}
		}
	}
	return timestamp, blockID, offset, length
}

func (vlm *valuesLocMap) set(keyA uint64, keyB uint64, timestamp uint64, blockID uint16, offset uint32, length uint32, evenIfSameTimestamp bool) uint64 {
	var oldTimestamp uint64
	var vlmPrev *valuesLocMap
	for {
		c := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.c))))
		if c == nil {
			break
		}
		vlmPrev = vlm
		if keyA&vlm.leftMask == 0 {
			vlm = c
		} else {
			vlm = (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.d))))
		}
	}
	a := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.a))))
	bix := keyB % uint64(len(a.buckets))
	lix := bix % uint64(len(a.locks))
	f := func(s *valuesLocStore, fb *valuesLocStore) {
		oldTimestamp = 0
		var sMatch *valueLoc
		var fbMatch *valueLoc
		var unusedItem *valueLoc
		s.locks[lix].Lock()
		if fb != nil {
			fb.locks[lix].Lock()
		}
		for item := &s.buckets[bix]; item != nil; item = item.next {
			if item.blockID == 0 {
				if unusedItem == nil {
					unusedItem = item
				}
				continue
			}
			if item.keyA == keyA && item.keyB == keyB {
				sMatch = item
				break
			}
		}
		if fb != nil {
			for item := &fb.buckets[bix]; item != nil; item = item.next {
				if item.blockID == 0 {
					continue
				}
				if item.keyA == keyA && item.keyB == keyB {
					fbMatch = item
					break
				}
			}
		}
		if sMatch != nil {
			if fbMatch != nil {
				if sMatch.timestamp >= fbMatch.timestamp {
					oldTimestamp = sMatch.timestamp
					if timestamp > sMatch.timestamp || (evenIfSameTimestamp && timestamp == sMatch.timestamp) {
						sMatch.timestamp = timestamp
						sMatch.blockID = blockID
						sMatch.offset = offset
						sMatch.length = length
					}
				} else {
					oldTimestamp = fbMatch.timestamp
					if timestamp > fbMatch.timestamp || (evenIfSameTimestamp && timestamp == fbMatch.timestamp) {
						sMatch.timestamp = timestamp
						sMatch.blockID = blockID
						sMatch.offset = offset
						sMatch.length = length
					} else {
						sMatch.timestamp = fbMatch.timestamp
						sMatch.blockID = fbMatch.blockID
						sMatch.offset = fbMatch.offset
						sMatch.length = fbMatch.length
					}
				}
				atomic.AddInt32(&fb.used, -1)
				fbMatch.blockID = 0
			} else {
				oldTimestamp = sMatch.timestamp
				if timestamp > sMatch.timestamp || (evenIfSameTimestamp && timestamp == sMatch.timestamp) {
					sMatch.timestamp = timestamp
					sMatch.blockID = blockID
					sMatch.offset = offset
					sMatch.length = length
				}
			}
		} else {
			atomic.AddInt32(&s.used, 1)
			if unusedItem == nil {
				unusedItem = &valueLoc{next: s.buckets[bix].next}
				s.buckets[bix].next = unusedItem
			}
			unusedItem.keyA = keyA
			unusedItem.keyB = keyB
			if fbMatch != nil {
				oldTimestamp = fbMatch.timestamp
				if timestamp > fbMatch.timestamp || (evenIfSameTimestamp && timestamp == fbMatch.timestamp) {
					unusedItem.timestamp = timestamp
					unusedItem.blockID = blockID
					unusedItem.offset = offset
					unusedItem.length = length
				} else {
					unusedItem.timestamp = fbMatch.timestamp
					unusedItem.blockID = fbMatch.blockID
					unusedItem.offset = fbMatch.offset
					unusedItem.length = fbMatch.length
				}
				atomic.AddInt32(&fb.used, -1)
				fbMatch.blockID = 0
			} else {
				unusedItem.timestamp = timestamp
				unusedItem.blockID = blockID
				unusedItem.offset = offset
				unusedItem.length = length
			}
		}
		if fb != nil {
			fb.locks[lix].Unlock()
		}
		s.locks[lix].Unlock()
	}
	if keyA&vlm.leftMask == 0 {
		e := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.e))))
		f(a, e)
		if e == nil {
			b := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.b))))
			if b == nil {
				used := atomic.LoadInt32(&a.used)
				if int(used) > vlm.splitCount {
					go vlm.split()
				} else if used == 0 && vlmPrev != nil {
					go vlmPrev.unsplit()
				}
			}
		}
	} else {
		b := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.b))))
		if b != nil {
			f(b, a)
		} else {
			e := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.e))))
			f(a, e)
			if e == nil {
				b := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.b))))
				if b == nil {
					used := atomic.LoadInt32(&a.used)
					if int(used) > vlm.splitCount {
						go vlm.split()
					} else if used == 0 && vlmPrev != nil {
						go vlmPrev.unsplit()
					}
				}
			}
		}
	}
	return oldTimestamp
}

func (vlm *valuesLocMap) isResizing() bool {
	vlm.resizingLock.RLock()
	resizing := vlm.resizing
	c := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.c))))
	d := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.d))))
	vlm.resizingLock.RUnlock()
	return resizing || (c != nil && c.isResizing()) || (d != nil && d.isResizing())
}

func (vlm *valuesLocMap) gatherStats(extended bool) *valuesLocMapStats {
	stats := &valuesLocMapStats{}
	if extended {
		stats.extended = true
		stats.depthCounts = []uint64{0}
		stats.splitCount = uint64(vlm.splitCount)
	}
	vlm.gatherStatsHelper(stats)
	stats.wg.Wait()
	if extended {
		stats.depthCounts = stats.depthCounts[1:]
	}
	return stats
}

func (vlm *valuesLocMap) gatherStatsHelper(stats *valuesLocMapStats) {
	if stats.extended {
		stats.sections++
		stats.depth++
		if stats.depth < uint64(len(stats.depthCounts)) {
			stats.depthCounts[stats.depth]++
		} else {
			stats.depthCounts = append(stats.depthCounts, 1)
		}
	}
	c := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.c))))
	if c != nil {
		d := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.d))))
		if stats.extended {
			depthOrig := stats.depth
			c.gatherStatsHelper(stats)
			depthC := stats.depth
			stats.depth = depthOrig
			d.gatherStatsHelper(stats)
			if depthC > stats.depth {
				stats.depth = depthC
			}
		} else {
			c.gatherStatsHelper(stats)
			d.gatherStatsHelper(stats)
		}
		return
	}
	f := func(s *valuesLocStore) {
		if stats.buckets == 0 {
			stats.buckets = uint64(len(s.buckets))
			stats.bucketCounts = make([]uint64, len(s.buckets))
		}
		stats.wg.Add(1)
		go func() {
			var bucketCounts []uint64
			var pointerLocs uint64
			var locs uint64
			var unused uint64
			var used uint64
			var active uint64
			var length uint64
			var tombstones uint64
			if stats.extended {
				bucketCounts = make([]uint64, len(s.buckets))
			}
			for bix := len(s.buckets) - 1; bix >= 0; bix-- {
				lix := bix % len(s.locks)
				s.locks[lix].RLock()
				if stats.extended {
					for item := &s.buckets[bix]; item != nil; item = item.next {
						bucketCounts[bix]++
						if item.next != nil {
							pointerLocs++
						}
						locs++
						if item.blockID == 0 {
							unused++
						} else {
							used++
							if item.timestamp&1 == 0 {
								active++
								length += uint64(item.length)
							} else {
								tombstones++
							}
						}
					}
				} else {
					for item := &s.buckets[bix]; item != nil; item = item.next {
						if item.blockID > 0 {
							if item.timestamp&1 == 0 {
								active++
								length += uint64(item.length)
							}
						}
					}
				}
				s.locks[lix].RUnlock()
			}
			if stats.extended {
				atomic.AddUint64(&stats.storages, 1)
				for bix := 0; bix < len(bucketCounts); bix++ {
					atomic.AddUint64(&stats.bucketCounts[bix], bucketCounts[bix])
				}
				atomic.AddUint64(&stats.pointerLocs, pointerLocs)
				atomic.AddUint64(&stats.locs, locs)
				atomic.AddUint64(&stats.used, used)
				atomic.AddUint64(&stats.unused, unused)
				atomic.AddUint64(&stats.tombstones, tombstones)
			}
			atomic.AddUint64(&stats.active, active)
			atomic.AddUint64(&stats.length, length)
			stats.wg.Done()
		}()
	}
	a := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.a))))
	if a != nil {
		f(a)
	}
	b := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.b))))
	if b != nil {
		f(b)
	}
	e := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.e))))
	if e != nil {
		f(e)
	}
}

func (stats *valuesLocMapStats) String() string {
	if stats.extended {
		averageBucketCount := uint64(0)
		minBucketCount := uint64(math.MaxUint64)
		maxBucketCount := uint64(0)
		for i := 0; i < len(stats.bucketCounts); i++ {
			averageBucketCount += stats.bucketCounts[i]
			if stats.bucketCounts[i] < minBucketCount {
				minBucketCount = stats.bucketCounts[i]
			}
			if stats.bucketCounts[i] > maxBucketCount {
				maxBucketCount = stats.bucketCounts[i]
			}
		}
		averageBucketCount /= stats.buckets
		depthCounts := fmt.Sprintf("%d", stats.depthCounts[0])
		for i := 1; i < len(stats.depthCounts); i++ {
			depthCounts += fmt.Sprintf(" %d", stats.depthCounts[i])
		}
		return brimtext.Align([][]string{
			[]string{"depth", fmt.Sprintf("%d", stats.depth)},
			[]string{"depthCounts", depthCounts},
			[]string{"sections", fmt.Sprintf("%d", stats.sections)},
			[]string{"storages", fmt.Sprintf("%d", stats.storages)},
			[]string{"buckets", fmt.Sprintf("%d", stats.buckets)},
			[]string{"averageBucketCount", fmt.Sprintf("%d", averageBucketCount)},
			[]string{"minBucketCount", fmt.Sprintf("%d %.1f%%", minBucketCount, float64(averageBucketCount-minBucketCount)/float64(averageBucketCount)*100)},
			[]string{"maxBucketCount", fmt.Sprintf("%d %.1f%%", maxBucketCount, float64(maxBucketCount-averageBucketCount)/float64(averageBucketCount)*100)},
			[]string{"splitCount", fmt.Sprintf("%d", stats.splitCount)},
			[]string{"locs", fmt.Sprintf("%d", stats.locs)},
			[]string{"pointerLocs", fmt.Sprintf("%d %.1f%%", stats.pointerLocs, float64(stats.pointerLocs)/float64(stats.locs)*100)},
			[]string{"unused", fmt.Sprintf("%d %.1f%%", stats.unused, float64(stats.unused)/float64(stats.locs)*100)},
			[]string{"used", fmt.Sprintf("%d", stats.used)},
			[]string{"active", fmt.Sprintf("%d", stats.active)},
			[]string{"length", fmt.Sprintf("%d", stats.length)},
			[]string{"tombstones", fmt.Sprintf("%d", stats.tombstones)},
		}, nil)
	} else {
		return brimtext.Align([][]string{
			[]string{"active", fmt.Sprintf("%d", stats.active)},
			[]string{"length", fmt.Sprintf("%d", stats.length)},
		}, nil)
	}
}

func (vlm *valuesLocMap) split() {
	vlm.resizingLock.Lock()
	a := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.a))))
	c := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.c))))
	if vlm.resizing || c != nil || int(atomic.LoadInt32(&a.used)) < vlm.splitCount {
		vlm.resizingLock.Unlock()
		return
	}
	vlm.resizing = true
	vlm.resizingLock.Unlock()
	b := &valuesLocStore{
		buckets: make([]valueLoc, len(a.buckets)),
		locks:   make([]sync.RWMutex, len(a.locks)),
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.b)), unsafe.Pointer(b))
	wg := &sync.WaitGroup{}
	var copies uint32
	var clears uint32
	f := func(coreOffset int, clear bool) {
		for bix := len(a.buckets) - 1 - coreOffset; bix >= 0; bix -= vlm.cores {
			lix := bix % len(a.locks)
			b.locks[lix].Lock()
			a.locks[lix].Lock()
		NEXT_ITEM_A:
			for itemA := &a.buckets[bix]; itemA != nil; itemA = itemA.next {
				if itemA.blockID == 0 || itemA.keyA&vlm.leftMask == 0 {
					continue
				}
				var unusedItemB *valueLoc
				for itemB := &b.buckets[bix]; itemB != nil; itemB = itemB.next {
					if itemB.blockID == 0 {
						if unusedItemB == nil {
							unusedItemB = itemB
						}
						continue
					}
					if itemA.keyA == itemB.keyA && itemA.keyB == itemB.keyB {
						if itemA.timestamp > itemB.timestamp {
							itemB.keyA = itemA.keyA
							itemB.keyB = itemA.keyB
							itemB.timestamp = itemA.timestamp
							itemB.blockID = itemA.blockID
							itemB.offset = itemA.offset
							itemB.length = itemA.length
							atomic.AddUint32(&copies, 1)
						}
						if clear {
							atomic.AddInt32(&a.used, -1)
							itemA.blockID = 0
							atomic.AddUint32(&clears, 1)
						}
						continue NEXT_ITEM_A
					}
				}
				atomic.AddInt32(&b.used, 1)
				if unusedItemB != nil {
					unusedItemB.keyA = itemA.keyA
					unusedItemB.keyB = itemA.keyB
					unusedItemB.timestamp = itemA.timestamp
					unusedItemB.blockID = itemA.blockID
					unusedItemB.offset = itemA.offset
					unusedItemB.length = itemA.length
				} else {
					b.buckets[bix].next = &valueLoc{
						next:      b.buckets[bix].next,
						keyA:      itemA.keyA,
						keyB:      itemA.keyB,
						timestamp: itemA.timestamp,
						blockID:   itemA.blockID,
						offset:    itemA.offset,
						length:    itemA.length,
					}
				}
				atomic.AddUint32(&copies, 1)
				if clear {
					atomic.AddInt32(&a.used, -1)
					itemA.blockID = 0
					atomic.AddUint32(&clears, 1)
				}
			}
			a.locks[lix].Unlock()
			b.locks[lix].Unlock()
		}
		wg.Done()
	}
	for passes := 0; passes < 2 || copies > 0; passes++ {
		copies = 0
		wg.Add(vlm.cores)
		for core := 0; core < vlm.cores; core++ {
			go f(core, false)
		}
		wg.Wait()
	}
	for passes := 0; passes < 2 || copies > 0 || clears > 0; passes++ {
		copies = 0
		clears = 0
		wg.Add(vlm.cores)
		for core := 0; core < vlm.cores; core++ {
			go f(core, true)
		}
		wg.Wait()
	}
	newVLM := &valuesLocMap{
		leftMask:   vlm.leftMask >> 1,
		rangeStart: vlm.rangeStart + vlm.leftMask,
		rangeStop:  vlm.rangeStop,
		cores:      vlm.cores,
		splitCount: vlm.splitCount,
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&newVLM.a)), unsafe.Pointer(b))
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.d)), unsafe.Pointer(newVLM))
	newVLM = &valuesLocMap{
		leftMask:   vlm.leftMask >> 1,
		rangeStart: vlm.rangeStart,
		rangeStop:  vlm.rangeStop - vlm.leftMask,
		cores:      vlm.cores,
		splitCount: vlm.splitCount,
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&newVLM.a)), unsafe.Pointer(a))
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.c)), unsafe.Pointer(newVLM))
	vlm.resizingLock.Lock()
	vlm.resizing = false
	vlm.resizingLock.Unlock()
}

func (vlm *valuesLocMap) unsplit() {
	vlm.resizingLock.Lock()
	c := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.c))))
	if vlm.resizing || c == nil {
		vlm.resizingLock.Unlock()
		return
	}
	c.resizingLock.Lock()
	cc := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&c.c))))
	if c.resizing || cc != nil {
		c.resizingLock.Unlock()
		vlm.resizingLock.Unlock()
		return
	}
	d := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.d))))
	d.resizingLock.Lock()
	dc := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&d.c))))
	if d.resizing || dc != nil {
		d.resizingLock.Unlock()
		c.resizingLock.Unlock()
		vlm.resizingLock.Unlock()
		return
	}
	d.resizing = true
	c.resizing = true
	vlm.resizing = true
	d.resizingLock.Unlock()
	c.resizingLock.Unlock()
	vlm.resizingLock.Unlock()
	a := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&c.a))))
	e := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&d.a))))
	if atomic.LoadInt32(&a.used) < atomic.LoadInt32(&e.used) {
		a, e = e, a
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.a)), nil)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.b)), nil)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.e)), unsafe.Pointer(e))
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.a)), unsafe.Pointer(a))
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.c)), nil)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.d)), nil)
	wg := &sync.WaitGroup{}
	var copies uint32
	var clears uint32
	f := func(coreOffset int, clear bool) {
		for bix := len(e.buckets) - 1 - coreOffset; bix >= 0; bix -= vlm.cores {
			lix := bix % len(e.locks)
			a.locks[lix].Lock()
			e.locks[lix].Lock()
		NEXT_ITEM_E:
			for itemE := &e.buckets[bix]; itemE != nil; itemE = itemE.next {
				if itemE.blockID == 0 {
					continue
				}
				var unusedItemA *valueLoc
				for itemA := &a.buckets[bix]; itemA != nil; itemA = itemA.next {
					if itemA.blockID == 0 {
						if unusedItemA == nil {
							unusedItemA = itemA
						}
						continue
					}
					if itemE.keyA == itemA.keyA && itemE.keyB == itemA.keyB {
						if itemE.timestamp > itemA.timestamp {
							itemA.keyA = itemE.keyA
							itemA.keyB = itemE.keyB
							itemA.timestamp = itemE.timestamp
							itemA.blockID = itemE.blockID
							itemA.offset = itemE.offset
							itemA.length = itemE.length
							atomic.AddUint32(&copies, 1)
						}
						if clear {
							atomic.AddInt32(&e.used, -1)
							itemE.blockID = 0
							atomic.AddUint32(&clears, 1)
						}
						continue NEXT_ITEM_E
					}
				}
				atomic.AddInt32(&a.used, 1)
				if unusedItemA != nil {
					unusedItemA.keyA = itemE.keyA
					unusedItemA.keyB = itemE.keyB
					unusedItemA.timestamp = itemE.timestamp
					unusedItemA.blockID = itemE.blockID
					unusedItemA.offset = itemE.offset
					unusedItemA.length = itemE.length
				} else {
					a.buckets[bix].next = &valueLoc{
						next:      a.buckets[bix].next,
						keyA:      itemE.keyA,
						keyB:      itemE.keyB,
						timestamp: itemE.timestamp,
						blockID:   itemE.blockID,
						offset:    itemE.offset,
						length:    itemE.length,
					}
				}
				atomic.AddUint32(&copies, 1)
				if clear {
					atomic.AddInt32(&e.used, -1)
					itemE.blockID = 0
					atomic.AddUint32(&clears, 1)
				}
			}
			e.locks[lix].Unlock()
			a.locks[lix].Unlock()
		}
		wg.Done()
	}
	for passes := 0; passes < 2 || copies > 0; passes++ {
		copies = 0
		wg.Add(vlm.cores)
		for core := 0; core < vlm.cores; core++ {
			go f(core, false)
		}
		wg.Wait()
	}
	for passes := 0; passes < 2 || copies > 0 || clears > 0; passes++ {
		copies = 0
		clears = 0
		wg.Add(vlm.cores)
		for core := 0; core < vlm.cores; core++ {
			go f(core, true)
		}
		wg.Wait()
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.e)), nil)
	vlm.resizingLock.Lock()
	vlm.resizing = false
	vlm.resizingLock.Unlock()
}

func (vlm *valuesLocMap) background(vs *ValuesStore, iteration uint16) {
	p := 0
	ppower := 1
	pincrement := uint64(1) << uint64(64-ppower)
	pstart := uint64(0)
	pstop := pstart + (pincrement - 1)
	wg := &sync.WaitGroup{}
	// Here I'm doing bloom filter scans for every partition when eventually it
	// should just do filters for partitions we're in the ring for. Partitions
	// we're not in the ring for (handoffs, old data from ring changes, etc.)
	// we should just send out what data we have and the remove it locally.
	for {
		for i := 0; i < vlm.cores; i++ {
			wg.Add(1)
			go vlm.scan(iteration, p, pstart, pstop, wg)
			if pstop == math.MaxUint64 {
				break
			}
			p++
			pstart += pincrement
			pstop += pincrement
		}
		wg.Wait()
		if pstop == math.MaxUint64 {
			break
		}
	}
	wg.Wait()
	// This is what I had as the background job before. Need to reimplement
	// this tombstone expiration as part of scanCount most likely.
	// bg := &valuesLocMapBackground{tombstoneCutoff: uint64(time.Now().UnixNano()) - vs.tombstoneAge}
	// vlm.backgroundHelper(bg, nil)
	// bg.wg.Wait()
}

const GLH_BLOOM_FILTER_N = 1000000
const GLH_BLOOM_FILTER_P = 0.001

func (vlm *valuesLocMap) scan(iteration uint16, p int, pstart uint64, pstop uint64, wg *sync.WaitGroup) {
	count := vlm.scanCount(pstart, pstop, 0)
	for count > GLH_BLOOM_FILTER_N {
		pstartNew := pstart + (pstop-pstart+1)/2
		wg.Add(1)
		go vlm.scan(iteration, p, pstart, pstartNew-1, wg)
		pstart = pstartNew
		count = vlm.scanCount(pstart, pstop, 0)
	}
	if count > 0 {
		ktbf := newKTBloomFilter(GLH_BLOOM_FILTER_N, GLH_BLOOM_FILTER_P, iteration)
		vlm.scanIntoBloomFilter(pstart, pstop, ktbf)
		if ktbf.hasData {
			// Here we'll send the bloom filter to the other replicas and ask
			// them to send us back all their data that isn't in the filter.
			// fmt.Printf("%016x %016x-%016x %s\n", p, pstart, pstop, ktbf)
		}
	}
	wg.Done()
}

func (vlm *valuesLocMap) scanCount(pstart uint64, pstop uint64, count uint64) uint64 {
	if vlm.rangeStart > pstop {
		return count
	}
	if vlm.rangeStop < pstart {
		return count
	}
	c := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.c))))
	if c != nil {
		d := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.d))))
		count = c.scanCount(pstart, pstop, count)
		if count > GLH_BLOOM_FILTER_N {
			return count
		}
		return d.scanCount(pstart, pstop, count)
	}
	a := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.a))))
	b := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.b))))
	if b == nil {
		if atomic.LoadInt32(&a.used) <= 0 {
			return count
		}
		for bix := len(a.buckets) - 1; bix >= 0; bix-- {
			lix := bix % len(a.locks)
			a.locks[lix].RLock()
			for itemA := &a.buckets[bix]; itemA != nil; itemA = itemA.next {
				if itemA.blockID == 0 || itemA.keyA < pstart || itemA.keyA > pstop {
					continue
				}
				count++
				if count > GLH_BLOOM_FILTER_N {
					a.locks[lix].RUnlock()
					return count
				}
			}
			a.locks[lix].RUnlock()
		}
	} else {
		if atomic.LoadInt32(&a.used) <= 0 && atomic.LoadInt32(&b.used) <= 0 {
			return count
		}
		for bix := len(b.buckets) - 1; bix >= 0; bix-- {
			lix := bix % len(b.locks)
			b.locks[lix].RLock()
			for itemB := &b.buckets[bix]; itemB != nil; itemB = itemB.next {
				if itemB.blockID == 0 || itemB.keyA < pstart || itemB.keyA > pstop {
					continue
				}
				count++
				if count > GLH_BLOOM_FILTER_N {
					b.locks[lix].RUnlock()
					return count
				}
			}
			b.locks[lix].RUnlock()
		}
		for bix := len(a.buckets) - 1; bix >= 0; bix-- {
			lix := bix % len(a.locks)
			b.locks[lix].RLock()
			a.locks[lix].RLock()
		NEXT_ITEM_A:
			for itemA := &a.buckets[bix]; itemA != nil; itemA = itemA.next {
				if itemA.blockID == 0 || itemA.keyA < pstart || itemA.keyA > pstop {
					continue
				}
				for itemB := &a.buckets[bix]; itemB != nil; itemB = itemB.next {
					if itemB.blockID == 0 {
						continue
					}
					if itemB.keyA == itemA.keyA && itemB.keyB == itemA.keyB {
						if itemB.timestamp >= itemA.timestamp {
							count++
							if count > GLH_BLOOM_FILTER_N {
								a.locks[lix].RUnlock()
								b.locks[lix].RUnlock()
								return count
							}
							continue NEXT_ITEM_A
						}
						break
					}
				}
				count++
				if count > GLH_BLOOM_FILTER_N {
					a.locks[lix].RUnlock()
					b.locks[lix].RUnlock()
					return count
				}
			}
			a.locks[lix].RUnlock()
			b.locks[lix].RUnlock()
		}
	}
	return count
}

func (vlm *valuesLocMap) scanIntoBloomFilter(pstart uint64, pstop uint64, ktbf *ktBloomFilter) {
	if vlm.rangeStart > pstop {
		return
	}
	if vlm.rangeStop < pstart {
		return
	}
	c := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.c))))
	if c != nil {
		d := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.d))))
		c.scanIntoBloomFilter(pstart, pstop, ktbf)
		d.scanIntoBloomFilter(pstart, pstop, ktbf)
		return
	}
	a := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.a))))
	b := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.b))))
	if b == nil {
		if atomic.LoadInt32(&a.used) <= 0 {
			return
		}
		for bix := len(a.buckets) - 1; bix >= 0; bix-- {
			lix := bix % len(a.locks)
			a.locks[lix].RLock()
			for itemA := &a.buckets[bix]; itemA != nil; itemA = itemA.next {
				if itemA.blockID == 0 || itemA.keyA < pstart || itemA.keyA > pstop {
					continue
				}
				ktbf.add(itemA.keyA, itemA.keyB, itemA.timestamp)
			}
		}
	} else {
		if atomic.LoadInt32(&a.used) <= 0 && atomic.LoadInt32(&b.used) <= 0 {
			return
		}
		for bix := len(b.buckets) - 1; bix >= 0; bix-- {
			lix := bix % len(b.locks)
			b.locks[lix].RLock()
			for itemB := &b.buckets[bix]; itemB != nil; itemB = itemB.next {
				if itemB.blockID == 0 || itemB.keyA < pstart || itemB.keyA > pstop {
					continue
				}
				ktbf.add(itemB.keyA, itemB.keyB, itemB.timestamp)
			}
			b.locks[lix].RUnlock()
		}
		for bix := len(a.buckets) - 1; bix >= 0; bix-- {
			lix := bix % len(a.locks)
			b.locks[lix].RLock()
			a.locks[lix].RLock()
		NEXT_ITEM_A:
			for itemA := &a.buckets[bix]; itemA != nil; itemA = itemA.next {
				if itemA.blockID == 0 || itemA.keyA < pstart || itemA.keyA > pstop {
					continue
				}
				for itemB := &a.buckets[bix]; itemB != nil; itemB = itemB.next {
					if itemB.blockID == 0 {
						continue
					}
					if itemB.keyA == itemA.keyA && itemB.keyB == itemA.keyB {
						if itemB.timestamp >= itemA.timestamp {
							ktbf.add(itemB.keyA, itemB.keyB, itemB.timestamp)
							continue NEXT_ITEM_A
						}
						break
					}
				}
				ktbf.add(itemA.keyA, itemA.keyB, itemA.timestamp)
			}
			a.locks[lix].RUnlock()
			b.locks[lix].RUnlock()
		}
	}
}

func (vlm *valuesLocMap) backgroundHelper(bg *valuesLocMapBackground, vlmPrev *valuesLocMap) {
	c := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.c))))
	if c != nil {
		d := (*valuesLocMap)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.d))))
		c.backgroundHelper(bg, vlm)
		d.backgroundHelper(bg, vlm)
		return
	}
	f := func(s *valuesLocStore) {
		var tombstonesDiscarded uint64
		var tombstonesRetained uint64
		for bix := len(s.buckets) - 1; bix >= 0; bix-- {
			lix := bix % len(s.locks)
			s.locks[lix].RLock()
			for item := &s.buckets[bix]; item != nil; item = item.next {
				if item.blockID > 0 && item.timestamp&1 == 1 {
					if item.timestamp < bg.tombstoneCutoff {
						atomic.AddInt32(&s.used, -1)
						item.blockID = 0
						tombstonesDiscarded++
					} else {
						tombstonesRetained++
					}
				}
			}
			s.locks[lix].RUnlock()
		}
		if tombstonesDiscarded > 0 {
			atomic.AddUint64(&bg.tombstonesDiscarded, tombstonesDiscarded)
		}
		if tombstonesRetained > 0 {
			atomic.AddUint64(&bg.tombstonesRetained, tombstonesRetained)
		}
		if atomic.LoadInt32(&s.used) == 0 && vlmPrev != nil {
			vlmPrev.unsplit()
		}
		bg.wg.Done()
	}
	a := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.a))))
	if a != nil {
		bg.wg.Add(1)
		go f(a)
	}
	b := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.b))))
	if b != nil {
		bg.wg.Add(1)
		go f(b)
	}
	e := (*valuesLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vlm.e))))
	if e != nil {
		bg.wg.Add(1)
		go f(e)
	}
}
