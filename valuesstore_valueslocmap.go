package brimstore

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/gholt/brimutil"
)

const (
	_VALUESBLOCK_UNUSED   = iota
	_VALUESBLOCK_TOMB     = iota
	_VALUESBLOCK_IDOFFSET = iota
)

type locMap struct {
	leftMask   uint64
	a          *locStore
	b          *locStore
	c          *locMap
	d          *locMap
	resizing   bool
	resizeLock sync.Mutex
	cores      int
}

type locStore struct {
	buckets []loc
	locks   []sync.RWMutex
	used    int32
}

type loc struct {
	next    *loc
	keyA    uint64
	keyB    uint64
	seq     uint64
	blockID uint16
	offset  uint32
	length  uint32
}

type valuesLocMapStats struct {
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

func newLocMap(opts *ValuesStoreOpts) *locMap {
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
	bucketCount := 1 << brimutil.PowerOfTwoNeeded(uint64(valuesLocMapPageSize)/uint64(unsafe.Sizeof(loc{})))
	lockCount := 1 << brimutil.PowerOfTwoNeeded(uint64(cores*cores))
	if lockCount > bucketCount {
		lockCount = bucketCount
	}
	return &locMap{
		leftMask: uint64(1) << 63,
		a: &locStore{
			buckets: make([]loc, bucketCount),
			locks:   make([]sync.RWMutex, lockCount),
		},
		cores: cores,
	}
}

func (lm *locMap) get(keyA uint64, keyB uint64) (uint64, uint16, uint32, uint32) {
	var seq uint64
	var blockID uint16 = _VALUESBLOCK_UNUSED
	var offset uint32
	var length uint32
	var a *locStore
	var b *locStore
	for {
		a = lm.a
		b = lm.b
		c := lm.c
		d := lm.d
		if c == nil {
			break
		}
		if keyA&lm.leftMask == 0 {
			lm = c
		} else {
			lm = d
		}
	}
	if b != nil {
		if keyA&lm.leftMask == 0 {
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

func (lm *locMap) set(keyA uint64, keyB uint64, seq uint64, blockID uint16, offset uint32, length uint32, evenIfSameSeq bool) uint64 {
	var oldSeq uint64
	var a *locStore
	var b *locStore
	for {
		a = lm.a
		b = lm.b
		c := lm.c
		d := lm.d
		if c == nil {
			break
		}
		if keyA&lm.leftMask == 0 {
			lm = c
		} else {
			lm = d
		}
	}
	if b != nil {
		if keyA&lm.leftMask == 0 {
			b = nil
		} else {
			a, b = b, a
		}
	}
	bix := keyB % uint64(len(a.buckets))
	lix := bix % uint64(len(a.locks))
	done := false
	var unusedItemA *loc
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
			a.buckets[bix].next = &loc{
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
		if int(atomic.LoadInt32(&a.used)) > len(a.buckets) {
			go lm.split()
		}
	}
	return oldSeq
}

func (lm *locMap) isResizing() bool {
	c, d := lm.c, lm.d
	return lm.resizing || (c != nil && c.isResizing()) || (d != nil && d.isResizing())
}

func (lm *locMap) gatherStats() *valuesLocMapStats {
	buckets := 0
	for llm := lm; llm != nil; llm = llm.c {
		a := llm.a
		if a != nil {
			buckets = len(a.buckets)
			break
		}
	}
	stats := &valuesLocMapStats{
		depthCounts:  []uint64{0},
		buckets:      uint64(buckets),
		bucketCounts: make([]uint64, buckets),
	}
	lm.gatherStats2(stats)
	stats.depthCounts = stats.depthCounts[1:]
	return stats
}

func (lm *locMap) gatherStats2(stats *valuesLocMapStats) {
	stats.sections++
	stats.depth++
	if stats.depth < uint64(len(stats.depthCounts)) {
		stats.depthCounts[stats.depth]++
	} else {
		stats.depthCounts = append(stats.depthCounts, 1)
	}
	a := lm.a
	b := lm.b
	c := lm.c
	d := lm.d
	for _, s := range []*locStore{a, b} {
		if s != nil {
			stats.storages++
			for bix := len(s.buckets) - 1; bix >= 0; bix-- {
				lix := bix % len(s.locks)
				s.locks[lix].RLock()
				for item := &s.buckets[bix]; item != nil; item = item.next {
					stats.bucketCounts[bix]++
					if item.next != nil {
						stats.pointerLocs++
					}
					stats.locs++
					switch item.blockID {
					case _VALUESBLOCK_UNUSED:
						stats.unused++
					case _VALUESBLOCK_TOMB:
						stats.tombs++
					default:
						stats.used++
						stats.length += uint64(item.length)
					}
				}
				s.locks[lix].RUnlock()
			}
		}
	}
	depthOrig := stats.depth
	if c != nil {
		c.gatherStats2(stats)
		depthC := stats.depth
		stats.depth = depthOrig
		d.gatherStats2(stats)
		if depthC > stats.depth {
			stats.depth = depthC
		}
	}
}

func (lm *locMap) split() {
	if lm.resizing {
		return
	}
	lm.resizeLock.Lock()
	a := lm.a
	b := lm.b
	if lm.resizing || a == nil || b != nil || int(atomic.LoadInt32(&a.used)) < len(a.buckets) {
		lm.resizeLock.Unlock()
		return
	}
	lm.resizing = true
	lm.resizeLock.Unlock()
	b = &locStore{
		buckets: make([]loc, len(a.buckets)),
		locks:   make([]sync.RWMutex, len(a.locks)),
	}
	lm.b = b
	wg := &sync.WaitGroup{}
	wg.Add(lm.cores)
	for core := 0; core < lm.cores; core++ {
		go func(coreOffset int) {
			clean := false
			for !clean {
				clean = true
				for bix := len(a.buckets) - 1 - coreOffset; bix >= 0; bix -= lm.cores {
					lix := bix % len(a.locks)
					b.locks[lix].Lock()
					a.locks[lix].Lock()
				NEXT_ITEM_A:
					for itemA := &a.buckets[bix]; itemA != nil; itemA = itemA.next {
						if itemA.blockID == _VALUESBLOCK_UNUSED || itemA.keyA&lm.leftMask == 0 {
							continue
						}
						clean = false
						var unusedItemB *loc
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
							b.buckets[bix].next = &loc{
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
	lm.d = &locMap{leftMask: lm.leftMask >> 1, a: b, cores: lm.cores}
	lm.c = &locMap{leftMask: lm.leftMask >> 1, a: a, cores: lm.cores}
	lm.a = nil
	lm.b = nil
	lm.resizing = false
}
