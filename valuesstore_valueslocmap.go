package brimstore

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/gholt/brimtext"
)

const (
	_VALUESBLOCK_UNUSED   = iota
	_VALUESBLOCK_TOMB     = iota
	_VALUESBLOCK_IDOFFSET = iota
)

type valuesLocMap struct {
	leftMask   uint64
	a          *valuesLocStore
	b          *valuesLocStore
	c          *valuesLocMap
	d          *valuesLocMap
	resizing   bool
	resizeLock sync.Mutex
	cores      int
	splitCount int
}

type valuesLocStore struct {
	buckets []valueLoc
	locks   []sync.RWMutex
	used    int32
}

type valueLoc struct {
	next    *valueLoc
	keyA    uint64
	keyB    uint64
	seq     uint64
	blockID uint16
	offset  uint32
	length  uint32
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
	tombs        uint64
	used         uint64
	length       uint64
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
	return &valuesLocMap{
		leftMask: uint64(1) << 63,
		a: &valuesLocStore{
			buckets: make([]valueLoc, bucketCount),
			locks:   make([]sync.RWMutex, lockCount),
		},
		cores:      cores,
		splitCount: int(float64(bucketCount) * splitMultiplier),
	}
}

func (vlm *valuesLocMap) get(keyA uint64, keyB uint64) (uint64, uint16, uint32, uint32) {
	var seq uint64
	var blockID uint16 = _VALUESBLOCK_UNUSED
	var offset uint32
	var length uint32
	var a *valuesLocStore
	var b *valuesLocStore
	for {
		a = vlm.a
		b = vlm.b
		c := vlm.c
		d := vlm.d
		if c == nil {
			break
		}
		if keyA&vlm.leftMask == 0 {
			vlm = c
		} else {
			vlm = d
		}
	}
	if b != nil {
		if keyA&vlm.leftMask == 0 {
			b = nil
		} else {
			a, b = b, a
		}
	}
	bix := keyB % uint64(len(a.buckets))
	lix := bix % uint64(len(a.locks))
	a.locks[lix].RLock()
	if b != nil {
		b.locks[lix].RLock()
	}
	for itemA := &a.buckets[bix]; itemA != nil; itemA = itemA.next {
		if itemA.blockID != _VALUESBLOCK_UNUSED && itemA.keyA == keyA && itemA.keyB == keyB {
			seq, blockID, offset, length = itemA.seq, itemA.blockID, itemA.offset, itemA.length
			break
		}
	}
	if blockID == _VALUESBLOCK_UNUSED && b != nil {
		for itemB := &b.buckets[bix]; itemB != nil; itemB = itemB.next {
			if itemB.blockID != _VALUESBLOCK_UNUSED && itemB.keyA == keyA && itemB.keyB == keyB {
				seq, blockID, offset, length = itemB.seq, itemB.blockID, itemB.offset, itemB.length
				break
			}
		}
	}
	if b != nil {
		b.locks[lix].RUnlock()
	}
	a.locks[lix].RUnlock()
	return seq, blockID, offset, length
}

func (vlm *valuesLocMap) set(keyA uint64, keyB uint64, seq uint64, blockID uint16, offset uint32, length uint32, evenIfSameSeq bool) uint64 {
	var oldSeq uint64
	var a *valuesLocStore
	var b *valuesLocStore
	for {
		a = vlm.a
		b = vlm.b
		c := vlm.c
		d := vlm.d
		if c == nil {
			break
		}
		if keyA&vlm.leftMask == 0 {
			vlm = c
		} else {
			vlm = d
		}
	}
	if b != nil {
		if keyA&vlm.leftMask == 0 {
			b = nil
		} else {
			a, b = b, a
		}
	}
	bix := keyB % uint64(len(a.buckets))
	lix := bix % uint64(len(a.locks))
	done := false
	var unusedItemA *valueLoc
	a.locks[lix].Lock()
	if b != nil {
		b.locks[lix].Lock()
	}
	for itemA := &a.buckets[bix]; itemA != nil; itemA = itemA.next {
		if itemA.blockID == _VALUESBLOCK_UNUSED {
			if unusedItemA == nil {
				unusedItemA = itemA
			}
			continue
		}
		if itemA.keyA == keyA && itemA.keyB == keyB {
			oldSeq = itemA.seq
			if (evenIfSameSeq && itemA.seq == seq) || itemA.seq < seq {
				if blockID == _VALUESBLOCK_UNUSED {
					atomic.AddInt32(&a.used, -1)
				}
				itemA.seq = seq
				itemA.blockID = blockID
				itemA.offset = offset
				itemA.length = length
			}
			done = true
			break
		}
	}
	if !done && b != nil {
		for itemB := &b.buckets[bix]; itemB != nil; itemB = itemB.next {
			if itemB.blockID == _VALUESBLOCK_UNUSED {
				continue
			}
			if itemB.keyA == keyA && itemB.keyB == keyB {
				oldSeq = itemB.seq
				if (evenIfSameSeq && itemB.seq == seq) || itemB.seq < seq {
					atomic.AddInt32(&b.used, -1)
					itemB.blockID = _VALUESBLOCK_UNUSED
				} else {
					done = true
				}
				break
			}
		}
	}
	if !done && blockID != _VALUESBLOCK_UNUSED {
		atomic.AddInt32(&a.used, 1)
		if unusedItemA != nil {
			unusedItemA.keyA = keyA
			unusedItemA.keyB = keyB
			unusedItemA.seq = seq
			unusedItemA.blockID = blockID
			unusedItemA.offset = offset
			unusedItemA.length = length
		} else {
			a.buckets[bix].next = &valueLoc{
				next:    a.buckets[bix].next,
				keyA:    keyA,
				keyB:    keyB,
				seq:     seq,
				blockID: blockID,
				offset:  offset,
				length:  length,
			}
		}
	}
	if b != nil {
		b.locks[lix].Unlock()
	}
	a.locks[lix].Unlock()
	if b == nil {
		if int(atomic.LoadInt32(&a.used)) > vlm.splitCount {
			go vlm.split()
		}
	}
	return oldSeq
}

func (vlm *valuesLocMap) isResizing() bool {
	c, d := vlm.c, vlm.d
	return vlm.resizing || (c != nil && c.isResizing()) || (d != nil && d.isResizing())
}

func (vlm *valuesLocMap) gatherStats(extended bool) *valuesLocMapStats {
	buckets := 0
	if extended {
		for llm := vlm; llm != nil; llm = llm.c {
			a := llm.a
			if a != nil {
				buckets = len(a.buckets)
				break
			}
		}
	}
	stats := &valuesLocMapStats{}
	if extended {
		stats.extended = true
		stats.depthCounts = []uint64{0}
		stats.buckets = uint64(buckets)
		stats.bucketCounts = make([]uint64, buckets)
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
	a := vlm.a
	b := vlm.b
	c := vlm.c
	d := vlm.d
	for _, s := range []*valuesLocStore{a, b} {
		if s != nil {
			stats.wg.Add(1)
			go func(s *valuesLocStore) {
				var bucketCounts []uint64
				var pointerLocs uint64
				var locs uint64
				var unused uint64
				var tombs uint64
				var used uint64
				var length uint64
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
							switch item.blockID {
							case _VALUESBLOCK_UNUSED:
								unused++
							case _VALUESBLOCK_TOMB:
								tombs++
							default:
								used++
								length += uint64(item.length)
							}
						}
					} else {
						for item := &s.buckets[bix]; item != nil; item = item.next {
							if item.blockID >= _VALUESBLOCK_IDOFFSET {
								used++
								length += uint64(item.length)
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
					atomic.AddUint64(&stats.unused, unused)
					atomic.AddUint64(&stats.tombs, tombs)
				}
				atomic.AddUint64(&stats.used, used)
				atomic.AddUint64(&stats.length, length)
				stats.wg.Done()
			}(s)
		}
	}
	if stats.extended {
		depthOrig := stats.depth
		if c != nil {
			c.gatherStatsHelper(stats)
			depthC := stats.depth
			stats.depth = depthOrig
			d.gatherStatsHelper(stats)
			if depthC > stats.depth {
				stats.depth = depthC
			}
		}
	} else {
		if c != nil {
			c.gatherStatsHelper(stats)
			d.gatherStatsHelper(stats)
		}
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
			[]string{"tombs", fmt.Sprintf("%d", stats.tombs)},
			[]string{"used", fmt.Sprintf("%d", stats.used)},
			[]string{"length", fmt.Sprintf("%d", stats.length)},
		}, nil)
	} else {
		return brimtext.Align([][]string{
			[]string{"used", fmt.Sprintf("%d", stats.used)},
			[]string{"length", fmt.Sprintf("%d", stats.length)},
		}, nil)
	}
}

func (vlm *valuesLocMap) split() {
	if vlm.resizing {
		return
	}
	vlm.resizeLock.Lock()
	a := vlm.a
	b := vlm.b
	if vlm.resizing || a == nil || b != nil || int(atomic.LoadInt32(&a.used)) < vlm.splitCount {
		vlm.resizeLock.Unlock()
		return
	}
	vlm.resizing = true
	vlm.resizeLock.Unlock()
	b = &valuesLocStore{
		buckets: make([]valueLoc, len(a.buckets)),
		locks:   make([]sync.RWMutex, len(a.locks)),
	}
	vlm.b = b
	wg := &sync.WaitGroup{}
	wg.Add(vlm.cores)
	for core := 0; core < vlm.cores; core++ {
		go func(coreOffset int) {
			clean := false
			for !clean {
				clean = true
				for bix := len(a.buckets) - 1 - coreOffset; bix >= 0; bix -= vlm.cores {
					lix := bix % len(a.locks)
					b.locks[lix].Lock()
					a.locks[lix].Lock()
				NEXT_ITEM_A:
					for itemA := &a.buckets[bix]; itemA != nil; itemA = itemA.next {
						if itemA.blockID == _VALUESBLOCK_UNUSED || itemA.keyA&vlm.leftMask == 0 {
							continue
						}
						clean = false
						var unusedItemB *valueLoc
						for itemB := &b.buckets[bix]; itemB != nil; itemB = itemB.next {
							if itemB.blockID == _VALUESBLOCK_UNUSED {
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
									itemB.blockID = itemA.blockID
									itemB.offset = itemA.offset
									itemB.length = itemA.length
								}
								atomic.AddInt32(&a.used, -1)
								itemA.blockID = _VALUESBLOCK_UNUSED
								continue NEXT_ITEM_A
							}
						}
						atomic.AddInt32(&b.used, 1)
						if unusedItemB != nil {
							unusedItemB.keyA = itemA.keyA
							unusedItemB.keyB = itemA.keyB
							unusedItemB.seq = itemA.seq
							unusedItemB.blockID = itemA.blockID
							unusedItemB.offset = itemA.offset
							unusedItemB.length = itemA.length
						} else {
							b.buckets[bix].next = &valueLoc{
								next:    b.buckets[bix].next,
								keyA:    itemA.keyA,
								keyB:    itemA.keyB,
								seq:     itemA.seq,
								blockID: itemA.blockID,
								offset:  itemA.offset,
								length:  itemA.length,
							}
						}
						atomic.AddInt32(&a.used, -1)
						itemA.blockID = _VALUESBLOCK_UNUSED
					}
					a.locks[lix].Unlock()
					b.locks[lix].Unlock()
				}
			}
			wg.Done()
		}(core)
	}
	wg.Wait()
	vlm.d = &valuesLocMap{
		leftMask:   vlm.leftMask >> 1,
		a:          b,
		cores:      vlm.cores,
		splitCount: vlm.splitCount,
	}
	vlm.c = &valuesLocMap{
		leftMask:   vlm.leftMask >> 1,
		a:          a,
		cores:      vlm.cores,
		splitCount: vlm.splitCount,
	}
	vlm.a = nil
	vlm.b = nil
	vlm.resizing = false
}
