// Package valuelocmap provides a concurrency-safe data structure that maps
// keys to value locations. A key is 128 bits and is specified using two
// uint64s (keyA, keyB). A value location is specified using a blockID, offset,
// and length triplet. Each mapping is assigned a timestamp and the greatest
// timestamp wins. The timestamp is also used to indicate a deletion marker; if
// timestamp & 1 == 1 then the mapping is considered a mark for deletion at
// that time. Deletion markers are used in case mappings come in out of order
// and for replication to others that may have missed the deletion.
//
// This implementation uses a tree structure of slices of key to location
// assignments. As the slices fill up, they are split into two and the tree
// structure grows. If a slice empties, it is merged with its pair in the tree
// structure and the tree shrinks. The tree is balanced by high bits of the
// key, and locations are distributed in the slices by the low bits.
//
// There are also functions for scanning key ranges, both to clean out old
// tombstones and to provide callbacks for replication or other tasks.
package valuelocmap

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/gholt/brimtext"
)

type config struct {
	cores           int
	pageSize        int
	splitMultiplier float64
}

func resolveConfig(opts ...func(*config)) *config {
	cfg := &config{}
	if env := os.Getenv("BRIMSTORE_VALUELOCMAP_CORES"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.cores = val
		}
	} else if env = os.Getenv("BRIMSTORE_CORES"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.cores = val
		}
	}
	if cfg.cores <= 0 {
		cfg.cores = runtime.GOMAXPROCS(0)
	}
	if env := os.Getenv("BRIMSTORE_VALUELOCMAP_PAGESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.pageSize = val
		}
	}
	if cfg.pageSize <= 0 {
		cfg.pageSize = 524288
	}
	if env := os.Getenv("BRIMSTORE_VALUELOCMAP_SPLITMULTIPLIER"); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil {
			cfg.splitMultiplier = val
		}
	}
	if cfg.splitMultiplier <= 0 {
		cfg.splitMultiplier = 3.0
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.cores < 1 {
		cfg.cores = 1
	}
	if cfg.pageSize < 1 {
		cfg.pageSize = 1
	}
	if cfg.splitMultiplier <= 0 {
		cfg.splitMultiplier = 0.01
	}
	return cfg
}

// OptList returns a slice with the opts given; useful if you want to possibly
// append more options to the list before using it with
// NewValueLocMap(list...).
func OptList(opts ...func(*config)) []func(*config) {
	return opts
}

// OptCores indicates how many cores may be in use (for calculating the number
// of locks to create, for example) and how many cores may be used for resizes.
// Defaults to env BRIMSTORE_VALUELOCMAP_CORES, BRIMSTORE_CORES, or GOMAXPROCS.
func OptCores(cores int) func(*config) {
	return func(cfg *config) {
		cfg.cores = cores
	}
}

// OptPageSize controls the size of each chunk of memory allocated. Defaults to
// env BRIMSTORE_VALUELOCMAP_PAGESIZE or 524,288.
func OptPageSize(bytes int) func(*config) {
	return func(cfg *config) {
		cfg.pageSize = bytes
	}
}

// OptSplitMultiplier indicates how full a memory page can get before being
// split into two pages. Defaults to env BRIMSTORE_VALUELOCMAP_SPLITMULTIPLIER
// or 3.0.
func OptSplitMultiplier(multiplier float64) func(*config) {
	return func(cfg *config) {
		cfg.splitMultiplier = multiplier
	}
}

// ValueLocMap instances are created with NewValueLocMap.
type ValueLocMap struct {
	root                    *valueLocNode
	cores                   int
	splitCount              int
	outOfPlaceKeyDetections int32
	replicationChan         chan interface{}
}

// OVERALL NOTES:
//
//  a is used to store at first, growth may then cause a split.
//  While splitting, b will be set, c and d will still be nil.
//  Once the split is complete, c and d will be set.
//  Shrinking may cause an unsplit.
//  During unsplit, a and e will be set, c and d will become nil.
//  e is considered read-only/fallback during unsplit.
//  Once unsplit is done, e will become nil.
//
// FOR SPEED'S SAKE THERE IS AN ASSUMPTION THAT ALL READS AND WRITES ACTIVE AT
// THE START OR DURING ONE RESIZE WILL BE COMPLETED BEFORE ANOTHER RESIZE OF
// THE SAME KEY SPACE STARTS.
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
// behaving badly the other servers will supersede it. In addition, background
// routines are in place to detect and repair any out of place data and
// so these rare anomalies shoud be resolved fairly quickly. Any repairs done
// will be reported via the gatherStats' outOfPlaceKeyDetections value in case
// their rarity isn't as uncommon as they should be.
//
// If you would rather have perfect correctness at the cost of speed, you will
// have to use an additional lock around all uses of a-e.
type valueLocNode struct {
	leftMask     uint64
	rangeStart   uint64
	rangeStop    uint64
	a            *valueLocStore
	b            *valueLocStore
	c            *valueLocNode
	d            *valueLocNode
	e            *valueLocStore
	resizing     bool
	resizingLock sync.RWMutex
}

type valueLocStore struct {
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

type valueLocMapStats struct {
	goroutines              int
	debug                   bool
	funcChan                chan func()
	cores                   int
	depth                   uint64
	depthCounts             []uint64
	sections                uint64
	storages                uint64
	buckets                 uint64
	bucketCounts            []uint64
	splitCount              int
	outOfPlaceKeyDetections int32
	locs                    uint64
	pointerLocs             uint64
	unused                  uint64
	used                    uint64
	active                  uint64
	length                  uint64
	tombstones              uint64
}

// NewValueLocMap creates a new ValueLocMap instance. You can provide Opt*
// functions for optional configuration items, such as OptCores:
//
//  vlmWithDefaults := valuelocmap.NewValueLocMap()
//  vlmWithOptions := valuelocmap.NewValueLocMap(
//      valuelocmap.OptCores(10),
//      valuelocmap.OptPageSize(4194304),
//  )
//  opts := valuelocmap.OptList()
//  if commandLineOptionForCores {
//      opts = append(opts, valuelocmap.OptCores(commandLineOptionValue)
//  }
//  vlmWithOptionsBuiltUp := valuelocmap.NewValueLocMap(opts...)
func NewValueLocMap(opts ...func(*config)) *ValueLocMap {
	cfg := resolveConfig(opts...)
	bucketCount := cfg.pageSize / int(unsafe.Sizeof(valueLoc{}))
	if bucketCount < 1 {
		bucketCount = 1
	}
	lockCount := cfg.cores
	if lockCount > bucketCount {
		lockCount = bucketCount
	}
	vlm := &ValueLocMap{
		root: &valueLocNode{
			leftMask:   uint64(1) << 63,
			rangeStart: 0,
			rangeStop:  math.MaxUint64,
			a: &valueLocStore{
				buckets: make([]valueLoc, bucketCount),
				locks:   make([]sync.RWMutex, lockCount),
			},
		},
		cores:      cfg.cores,
		splitCount: int(float64(bucketCount) * cfg.splitMultiplier),
	}
	return vlm
}

// Get returns timestamp, blockID, offset, and length for keyA, keyB. The
// blockID will be 0 if keyA, keyB was not found. The timestamp & 1 == 1 if
// keyA, keyB is marked for deletion.
func (vlm *ValueLocMap) Get(keyA uint64, keyB uint64) (uint64, uint16, uint32, uint32) {
	var timestamp uint64
	var blockID uint16
	var offset uint32
	var length uint32
	vln := vlm.root
VLN_SELECTION:
	// Traverse the tree until we hit a leaf node (no c [and therefore no d]).
	for {
		c := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.c))))
		if c == nil {
			break
		}
		if keyA&vln.leftMask == 0 {
			vln = c
		} else {
			vln = (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.d))))
		}
	}
	a := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.a))))
	bix := keyB % uint64(len(a.buckets))
	lix := bix % uint64(len(a.locks))
	f := func(s *valueLocStore, fb *valueLocStore) {
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
	if keyA&vln.leftMask == 0 {
		// If we're on the left side, even if a split is in progress or happens
		// while we're reading it won't matter because we'd still read from the
		// same memory block, assuming more than one split doesn't occur while
		// we're reading.
		f(a, nil)
		// If an unsplit happened while we were reading, store a will end up
		// nil and we need to retry the read.
		a = (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.a))))
		if a == nil {
			vln = vlm.root
			goto VLN_SELECTION
		}
	} else {
		// If we're on the right side, then things might be a bit trickier...
		b := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.b))))
		if b != nil {
			// If a split is in progress, then we can read from b and fallback
			// to a and we're safe, assuming another split doesn't occur during
			// our read.
			f(b, a)
		} else {
			// If no split is in progress, we'll read from a and fallback to e
			// if it exists...
			f(a, (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.e)))))
			// If an unsplit happened while we were reading, store a will end
			// up nil and we need to retry the read.
			a = (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.a))))
			if a == nil {
				vln = vlm.root
				goto VLN_SELECTION
			}
			// If we pass that test, we'll double check b...
			b := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.b))))
			if b != nil {
				// If b is set, a split started while we were reading, so we'll
				// re-read from b and fallback to a and we're safe, assuming
				// another split doesn't occur during this re-read.
				f(b, a)
			} else {
				// If b isn't set, either no split happened while we were
				// reading, or the split happened and finished while we were
				// reading, so we'll double check d to find out...
				d := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.d))))
				if d != nil {
					// If a complete split occurred while we were reading,
					// we'll traverse the tree node and jump back to any
					// further tree node traversal and retry the read.
					vln = d
					goto VLN_SELECTION
				}
			}
		}
	}
	return timestamp, blockID, offset, length
}

// Set returns the previous timestamp after updating keyA, keyB to have the
// timestamp, blockID, offset, and length.
//
// If blockID is 0 then keyA, keyB will be removed from the map, though this
// isn't usually done. Instead, setting the timestamp to a value where
// timestamp & 1 == 1 will mark keyA, keyB for deletion and the deletion marker
// will be automatically removed after some time (assuming it isn't overridden
// with another set).
//
// Normally a set will only take affect if the given timestamp is greater than
// any existing timestamp for keyA, keyB. You can set evenIfSameTimestamp to
// true to update the location even if the existing timestamp is the same as
// the timestamp passed in, which is useful to update where the value for keyA,
// keyB is now located (moving from memory to a disk file, or from one disk
// file to another, as examples).
//
// The previous timestamp returned can be used to determine if a set had any
// effect. If the previous timestamp is greater than (or equal to, if
// evenIfSameTimestamp is false) the timestamp passed in, the set had no
// effect. This information can be used to decide whether to persist the
// pointed to value, for example.
func (vlm *ValueLocMap) Set(keyA uint64, keyB uint64, timestamp uint64, blockID uint16, offset uint32, length uint32, evenIfSameTimestamp bool) uint64 {
	var oldTimestamp uint64
	var originalOldTimestampCheck bool
	var originalOldTimestamp uint64
	var vlmPrev *valueLocNode
	vln := vlm.root
VLN_SELECTION:
	// Traverse the tree until we hit a leaf node (no c [and therefore no d]).
	for {
		c := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.c))))
		if c == nil {
			break
		}
		vlmPrev = vln
		if keyA&vln.leftMask == 0 {
			vln = c
		} else {
			vln = (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.d))))
		}
	}
	a := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.a))))
	bix := keyB % uint64(len(a.buckets))
	lix := bix % uint64(len(a.locks))
	f := func(s *valueLocStore, fb *valueLocStore) {
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
	if keyA&vln.leftMask == 0 {
		// If we're on the left side, even if a split is in progress or happens
		// while we're writing it won't matter because we'd still write to the
		// same memory block, assuming more than one split doesn't occur while
		// we're writing.
		f(a, nil)
		// If our write was not superseded...
		if oldTimestamp < timestamp || (evenIfSameTimestamp && oldTimestamp == timestamp) {
			// If an unsplit happened while we were writing, store a will end
			// up nil and we need to clear what we wrote and retry the write.
			aAgain := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.a))))
			if aAgain == nil {
				a.locks[lix].Lock()
				for item := &a.buckets[bix]; item != nil; item = item.next {
					if item.blockID == 0 {
						continue
					}
					if item.keyA == keyA && item.keyB == keyB {
						if item.timestamp == timestamp && item.blockID == blockID && item.offset == offset && item.length == length {
							item.blockID = 0
						}
						break
					}
				}
				a.locks[lix].Unlock()
				if !originalOldTimestampCheck {
					originalOldTimestampCheck = true
					originalOldTimestamp = oldTimestamp
				}
				vln = vlm.root
				goto VLN_SELECTION
			}
			// Otherwise, we read b and e and if both are nil (no split/unsplit
			// in progress) we check a's used counter to see if we should
			// request a split/unsplit.
			b := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.b))))
			if b == nil {
				e := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.e))))
				if e == nil {
					used := atomic.LoadInt32(&a.used)
					if int(used) > vlm.splitCount {
						go vln.split(vlm.splitCount, vlm.cores)
					} else if used == 0 && vlmPrev != nil {
						go vlmPrev.unsplit(vlm.cores)
					}
				}
			}
		}
	} else {
		// If we're on the right side, then things might be a bit trickier...
		b := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.b))))
		if b != nil {
			// If a split is in progress, then we can write to b checking a for
			// any competing value and we're safe, assuming another split
			// doesn't occur during our write.
			f(b, a)
		} else {
			// If no split is in progress, we'll write to a checking e (if it
			// exists) for any competing value...
			f(a, (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.e)))))
			// If our write was not superseded...
			if oldTimestamp < timestamp || (evenIfSameTimestamp && oldTimestamp == timestamp) {
				// If an unsplit happened while we were writing, store a will
				// end up nil and we need to clear what we wrote and retry the
				// write.
				aAgain := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.a))))
				if aAgain == nil {
					a.locks[lix].Lock()
					for item := &a.buckets[bix]; item != nil; item = item.next {
						if item.blockID == 0 {
							continue
						}
						if item.keyA == keyA && item.keyB == keyB {
							if item.timestamp == timestamp && item.blockID == blockID && item.offset == offset && item.length == length {
								item.blockID = 0
							}
							break
						}
					}
					a.locks[lix].Unlock()
					if !originalOldTimestampCheck {
						originalOldTimestampCheck = true
						originalOldTimestamp = oldTimestamp
					}
					vln = vlm.root
					goto VLN_SELECTION
				}
				// If we pass that test, we'll double check b...
				b := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.b))))
				if b != nil {
					// If b is set, a split started while we were writing, so
					// we'll re-write to b checking a for a competing value
					// (should at least be the one we just wrote) and we're
					// safe, assuming another split doesn't occur during our
					// write.
					if !originalOldTimestampCheck {
						originalOldTimestampCheck = true
						originalOldTimestamp = oldTimestamp
					}
					f(b, a)
				} else {
					// If b isn't set, either no split happened while we were
					// writing, or the split happened and finished while we
					// were writing, so we'll double check d to find out...
					d := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.d))))
					if d != nil {
						// If a complete split occurred while we were writing,
						// we'll clear our write and then we'll traverse the
						// tree node and jump back to any further tree node
						// traversal and retry the write.
						a.locks[lix].Lock()
						for item := &a.buckets[bix]; item != nil; item = item.next {
							if item.blockID == 0 {
								continue
							}
							if item.keyA == keyA && item.keyB == keyB {
								if item.timestamp == timestamp && item.blockID == blockID && item.offset == offset && item.length == length {
									item.blockID = 0
								}
								break
							}
						}
						a.locks[lix].Unlock()
						if !originalOldTimestampCheck {
							originalOldTimestampCheck = true
							originalOldTimestamp = oldTimestamp
						}
						vln = d
						goto VLN_SELECTION
					} else {
						// If no split is progress or had ocurred while we were
						// writing, we check e to see if an unsplit is in
						// progress and, if not, we check a's used counter to
						// see if we should request a split/unsplit.
						e := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.e))))
						if e == nil {
							used := atomic.LoadInt32(&a.used)
							if int(used) > vlm.splitCount {
								go vln.split(vlm.splitCount, vlm.cores)
							} else if used == 0 && vlmPrev != nil {
								go vlmPrev.unsplit(vlm.cores)
							}
						}
					}
				}
			}
		}
	}
	if originalOldTimestampCheck && originalOldTimestamp < oldTimestamp {
		oldTimestamp = originalOldTimestamp
	}
	return oldTimestamp
}

func (vlm *ValueLocMap) isResizing() bool {
	return vlm.root.isResizing()
}

func (vln *valueLocNode) isResizing() bool {
	vln.resizingLock.RLock()
	if vln.resizing {
		vln.resizingLock.RUnlock()
		return true
	}
	c := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.c))))
	if c != nil && c.isResizing() {
		vln.resizingLock.RUnlock()
		return true
	}
	d := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.d))))
	if d != nil && d.isResizing() {
		vln.resizingLock.RUnlock()
		return true
	}
	vln.resizingLock.RUnlock()
	return false
}

// GatherStats returns the active (non deletion markers)  mapping count and
// total length referenced as well as a fmt.Stringer that contains debug
// information if debug is true; note that when debug is true additional
// resources are consumed to collect the additional information. Also note that
// while data collection is ongoing, other operations with the location map
// will be slower, especially if debug is true. You can use the goroutines
// setting to limit the impact of data collection; 0 will use the number of
// cores the ValueLocMap is configured for.
func (vlm *ValueLocMap) GatherStats(goroutines int, debug bool) (uint64, uint64, fmt.Stringer) {
	if goroutines < 1 {
		goroutines = vlm.cores
	}
	stats := &valueLocMapStats{
		goroutines: goroutines,
		debug:      debug,
		funcChan:   make(chan func(), goroutines),
		cores:      vlm.cores,
	}
	funcsDone := make(chan struct{}, 1)
	go func() {
		wg := &sync.WaitGroup{}
		for {
			f := <-stats.funcChan
			if f == nil {
				break
			}
			wg.Add(1)
			go func() {
				f()
				wg.Done()
			}()
		}
		wg.Wait()
		funcsDone <- struct{}{}
	}()
	if stats.debug {
		stats.depthCounts = []uint64{0}
		stats.splitCount = vlm.splitCount
		stats.outOfPlaceKeyDetections = vlm.outOfPlaceKeyDetections
	}
	vlm.root.gatherStatsHelper(stats)
	stats.funcChan <- nil
	<-funcsDone
	if debug {
		stats.depthCounts = stats.depthCounts[1:]
	}
	return stats.active, stats.length, stats
}

func (vln *valueLocNode) gatherStatsHelper(stats *valueLocMapStats) {
	if stats.debug {
		stats.sections++
		stats.depth++
		if stats.depth < uint64(len(stats.depthCounts)) {
			stats.depthCounts[stats.depth]++
		} else {
			stats.depthCounts = append(stats.depthCounts, 1)
		}
	}
	c := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.c))))
	if c != nil {
		d := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.d))))
		if stats.debug {
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
	f := func(s *valueLocStore) {
		if stats.buckets == 0 {
			stats.buckets = uint64(len(s.buckets))
		}
		stats.funcChan <- func() {
			var bucketCounts []uint64
			var pointerLocs uint64
			var locs uint64
			var unused uint64
			var used uint64
			var active uint64
			var length uint64
			var tombstones uint64
			if stats.debug {
				bucketCounts = make([]uint64, len(s.buckets))
			}
			for bix := len(s.buckets) - 1; bix >= 0; bix-- {
				lix := bix % len(s.locks)
				s.locks[lix].RLock()
				if stats.debug {
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
			if stats.debug {
				atomic.AddUint64(&stats.storages, 1)
				atomic.AddUint64(&stats.pointerLocs, pointerLocs)
				atomic.AddUint64(&stats.locs, locs)
				atomic.AddUint64(&stats.used, used)
				atomic.AddUint64(&stats.unused, unused)
				atomic.AddUint64(&stats.tombstones, tombstones)
			}
			atomic.AddUint64(&stats.active, active)
			atomic.AddUint64(&stats.length, length)
		}
	}
	a := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.a))))
	if a != nil {
		f(a)
	}
	b := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.b))))
	if b != nil {
		f(b)
	}
	e := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.e))))
	if e != nil {
		f(e)
	}
}

func (stats *valueLocMapStats) String() string {
	if stats.debug {
		depthCounts := fmt.Sprintf("%d", stats.depthCounts[0])
		for i := 1; i < len(stats.depthCounts); i++ {
			depthCounts += fmt.Sprintf(" %d", stats.depthCounts[i])
		}
		return brimtext.Align([][]string{
			[]string{"statsGoroutines", fmt.Sprintf("%d", stats.goroutines)},
			[]string{"cores", fmt.Sprintf("%d", stats.cores)},
			[]string{"pageSize", fmt.Sprintf("%d", stats.buckets*uint64(unsafe.Sizeof(valueLoc{})))},
			[]string{"splitMultiplier", fmt.Sprintf("%f", float64(stats.splitCount)/float64(stats.buckets))},
			[]string{"depth", fmt.Sprintf("%d", stats.depth)},
			[]string{"depthCounts", depthCounts},
			[]string{"sections", fmt.Sprintf("%d", stats.sections)},
			[]string{"storages", fmt.Sprintf("%d", stats.storages)},
			[]string{"bucketsPerPage", fmt.Sprintf("%d", stats.buckets)},
			[]string{"splitCount", fmt.Sprintf("%d", stats.splitCount)},
			[]string{"outOfPlaceKeyDetections", fmt.Sprintf("%d", stats.outOfPlaceKeyDetections)},
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

func (vln *valueLocNode) split(splitCount int, cores int) {
	vln.resizingLock.Lock()
	a := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.a))))
	c := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.c))))
	if vln.resizing || c != nil || int(atomic.LoadInt32(&a.used)) < splitCount {
		vln.resizingLock.Unlock()
		return
	}
	vln.resizing = true
	vln.resizingLock.Unlock()
	b := &valueLocStore{
		buckets: make([]valueLoc, len(a.buckets)),
		locks:   make([]sync.RWMutex, len(a.locks)),
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vln.b)), unsafe.Pointer(b))
	wg := &sync.WaitGroup{}
	var copies uint32
	var clears uint32
	f := func(coreOffset int, clear bool) {
		for bix := len(a.buckets) - 1 - coreOffset; bix >= 0; bix -= cores {
			lix := bix % len(a.locks)
			b.locks[lix].Lock()
			a.locks[lix].Lock()
		NEXT_ITEM_A:
			for itemA := &a.buckets[bix]; itemA != nil; itemA = itemA.next {
				if itemA.blockID == 0 || itemA.keyA&vln.leftMask == 0 {
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
		wg.Add(cores)
		for core := 0; core < cores; core++ {
			go f(core, false)
		}
		wg.Wait()
	}
	for passes := 0; passes < 2 || copies > 0 || clears > 0; passes++ {
		copies = 0
		clears = 0
		wg.Add(cores)
		for core := 0; core < cores; core++ {
			go f(core, true)
		}
		wg.Wait()
	}
	newVLN := &valueLocNode{
		leftMask:   vln.leftMask >> 1,
		rangeStart: vln.rangeStart + vln.leftMask,
		rangeStop:  vln.rangeStop,
		a:          b,
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vln.d)), unsafe.Pointer(newVLN))
	newVLN = &valueLocNode{
		leftMask:   vln.leftMask >> 1,
		rangeStart: vln.rangeStart,
		rangeStop:  vln.rangeStop - vln.leftMask,
		a:          a,
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vln.c)), unsafe.Pointer(newVLN))
	vln.resizingLock.Lock()
	vln.resizing = false
	vln.resizingLock.Unlock()
}

func (vln *valueLocNode) unsplit(cores int) {
	vln.resizingLock.Lock()
	c := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.c))))
	if vln.resizing || c == nil {
		vln.resizingLock.Unlock()
		return
	}
	c.resizingLock.Lock()
	cc := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&c.c))))
	if c.resizing || cc != nil {
		c.resizingLock.Unlock()
		vln.resizingLock.Unlock()
		return
	}
	d := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.d))))
	d.resizingLock.Lock()
	dc := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&d.c))))
	if d.resizing || dc != nil {
		d.resizingLock.Unlock()
		c.resizingLock.Unlock()
		vln.resizingLock.Unlock()
		return
	}
	d.resizing = true
	c.resizing = true
	vln.resizing = true
	d.resizingLock.Unlock()
	c.resizingLock.Unlock()
	vln.resizingLock.Unlock()
	a := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&c.a))))
	e := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&d.a))))
	// Even if a has less items than e, we copy items from e to a because
	// get/set and other routines assume a is left and e is right.
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vln.a)), nil)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vln.b)), nil)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vln.e)), unsafe.Pointer(e))
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vln.a)), unsafe.Pointer(a))
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&c.a)), nil)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&d.a)), nil)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vln.c)), nil)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vln.d)), nil)
	wg := &sync.WaitGroup{}
	var copies uint32
	var clears uint32
	f := func(coreOffset int, clear bool) {
		for bix := len(e.buckets) - 1 - coreOffset; bix >= 0; bix -= cores {
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
		wg.Add(cores)
		for core := 0; core < cores; core++ {
			go f(core, false)
		}
		wg.Wait()
	}
	for passes := 0; passes < 2 || copies > 0 || clears > 0; passes++ {
		copies = 0
		clears = 0
		wg.Add(cores)
		for core := 0; core < cores; core++ {
			go f(core, true)
		}
		wg.Wait()
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&vln.e)), nil)
	vln.resizingLock.Lock()
	vln.resizing = false
	vln.resizingLock.Unlock()
}

// Scan will scan the key range (of keyA) from start to stop (inclusive) and
// discard any tombstones older than the tombstoneCutoff.
func (vlm *ValueLocMap) Scan(tombstoneCutoff uint64, start uint64, stop uint64) {
	vlm.root.scan(vlm, tombstoneCutoff, start, stop)
}

// ScanCount will scan the key range (of keyA) from start to stop (inclusive),
// discard any tombstones older than the tombstoneCutoff, and return the count
// of mappings found (including deletion markers). If the count at any point
// exceeds the max given, the scan will stop and the count thus far will be
// returned.
func (vlm *ValueLocMap) ScanCount(tombstoneCutoff uint64, start uint64, stop uint64, max uint64) uint64 {
	return vlm.root.scanCount(vlm, tombstoneCutoff, start, stop, max, 0)
}

// ScanCallback will scan the key range (of keyA) from start to stop
// (inclusive) and call the callback with any mappings found (including
// deletion markers). If the callback returns false, scanning will stop.
func (vlm *ValueLocMap) ScanCallback(start uint64, stop uint64, callback func(keyA uint64, keyB uint64, timestamp uint64, blockID uint16, offset uint32, length uint32) bool) {
	vlm.root.scanCallback(vlm, start, stop, callback)
}

func (vln *valueLocNode) scan(vlm *ValueLocMap, tombstoneCutoff uint64, pstart uint64, pstop uint64) {
	if vln.rangeStart > pstop {
		return
	}
	if vln.rangeStop < pstart {
		return
	}
	c := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.c))))
	if c != nil {
		d := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.d))))
		c.scan(vlm, tombstoneCutoff, pstart, pstop)
		d.scan(vlm, tombstoneCutoff, pstart, pstop)
		return
	}
	a := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.a))))
	b := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.b))))
	if b != nil {
		// Just skip splits in progress and assume future scans wil eventually
		// hit these areas.
		return
	}
	if atomic.LoadInt32(&a.used) <= 0 {
		return
	}
	for bix := len(a.buckets) - 1; bix >= 0; bix-- {
		lix := bix % len(a.locks)
		a.locks[lix].RLock()
		for itemA := &a.buckets[bix]; itemA != nil; itemA = itemA.next {
			if itemA.blockID == 0 {
				continue
			}
			if itemA.keyA < vln.rangeStart || itemA.keyA > vln.rangeStop {
				// Out of place key, extract and reinsert.
				atomic.AddInt32(&vlm.outOfPlaceKeyDetections, 1)
				go vlm.Set(itemA.keyA, itemA.keyB, itemA.timestamp, itemA.blockID, itemA.offset, itemA.length, false)
				itemA.blockID = 0
				continue
			}
			if itemA.keyA < pstart || itemA.keyA > pstop {
				continue
			}
			if itemA.timestamp&1 == 1 && itemA.timestamp < tombstoneCutoff {
				atomic.AddInt32(&a.used, -1)
				itemA.blockID = 0
			}
		}
		a.locks[lix].RUnlock()
	}
}

func (vln *valueLocNode) scanCount(vlm *ValueLocMap, tombstoneCutoff uint64, pstart uint64, pstop uint64, max uint64, count uint64) uint64 {
	if vln.rangeStart > pstop {
		return count
	}
	if vln.rangeStop < pstart {
		return count
	}
	c := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.c))))
	if c != nil {
		d := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.d))))
		count = c.scanCount(vlm, tombstoneCutoff, pstart, pstop, max, count)
		if count > max {
			return count
		}
		return d.scanCount(vlm, tombstoneCutoff, pstart, pstop, max, count)
	}
	a := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.a))))
	b := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.b))))
	if b == nil {
		if atomic.LoadInt32(&a.used) <= 0 {
			return count
		}
		for bix := len(a.buckets) - 1; bix >= 0; bix-- {
			lix := bix % len(a.locks)
			a.locks[lix].RLock()
			for itemA := &a.buckets[bix]; itemA != nil; itemA = itemA.next {
				if itemA.blockID == 0 {
					continue
				}
				if itemA.keyA < vln.rangeStart || itemA.keyA > vln.rangeStop {
					// Out of place key, extract and reinsert.
					atomic.AddInt32(&vlm.outOfPlaceKeyDetections, 1)
					go vlm.Set(itemA.keyA, itemA.keyB, itemA.timestamp, itemA.blockID, itemA.offset, itemA.length, false)
					itemA.blockID = 0
					continue
				}
				if itemA.keyA < pstart || itemA.keyA > pstop {
					continue
				}
				if itemA.timestamp&1 == 1 && itemA.timestamp < tombstoneCutoff {
					atomic.AddInt32(&a.used, -1)
					itemA.blockID = 0
				}
				count++
				if count > max {
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
		for bix := len(a.buckets) - 1; bix >= 0; bix-- {
			lix := bix % len(a.locks)
			b.locks[lix].RLock()
			a.locks[lix].RLock()
		NEXT_ITEM_A:
			for itemA := &a.buckets[bix]; itemA != nil; itemA = itemA.next {
				if itemA.blockID == 0 || itemA.keyA < pstart || itemA.keyA > pstop {
					continue
				}
				for itemB := &b.buckets[bix]; itemB != nil; itemB = itemB.next {
					if itemB.blockID == 0 {
						continue
					}
					if itemB.keyA == itemA.keyA && itemB.keyB == itemA.keyB {
						if itemB.timestamp >= itemA.timestamp {
							continue NEXT_ITEM_A
						}
						break
					}
				}
				count++
				if count > max {
					a.locks[lix].RUnlock()
					b.locks[lix].RUnlock()
					return count
				}
			}
		NEXT_ITEM_B:
			for itemB := &b.buckets[bix]; itemB != nil; itemB = itemB.next {
				if itemB.blockID == 0 || itemB.keyA < pstart || itemB.keyA > pstop {
					continue
				}
				for itemA := &a.buckets[bix]; itemA != nil; itemA = itemA.next {
					if itemA.blockID == 0 {
						continue
					}
					if itemA.keyA == itemB.keyA && itemA.keyB == itemB.keyB {
						if itemA.timestamp >= itemB.timestamp {
							continue NEXT_ITEM_B
						}
						break
					}
				}
				count++
				if count > max {
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

func (vln *valueLocNode) scanCallback(vlm *ValueLocMap, pstart uint64, pstop uint64, callback func(keyA uint64, keyB uint64, timestamp uint64, blockID uint16, offset uint32, length uint32) bool) {
	if vln.rangeStart > pstop {
		return
	}
	if vln.rangeStop < pstart {
		return
	}
	c := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.c))))
	if c != nil {
		d := (*valueLocNode)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.d))))
		c.scanCallback(vlm, pstart, pstop, callback)
		d.scanCallback(vlm, pstart, pstop, callback)
		return
	}
	a := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.a))))
	b := (*valueLocStore)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&vln.b))))
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
				if !callback(itemA.keyA, itemA.keyB, itemA.timestamp, itemA.blockID, itemA.offset, itemA.length) {
					a.locks[lix].RUnlock()
					return
				}
			}
			a.locks[lix].RUnlock()
		}
	} else {
		if atomic.LoadInt32(&a.used) <= 0 && atomic.LoadInt32(&b.used) <= 0 {
			return
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
				for itemB := &b.buckets[bix]; itemB != nil; itemB = itemB.next {
					if itemB.blockID == 0 {
						continue
					}
					if itemB.keyA == itemA.keyA && itemB.keyB == itemA.keyB {
						if itemB.timestamp >= itemA.timestamp {
							continue NEXT_ITEM_A
						}
						break
					}
				}
				if !callback(itemA.keyA, itemA.keyB, itemA.timestamp, itemA.blockID, itemA.offset, itemA.length) {
					a.locks[lix].RUnlock()
					b.locks[lix].RUnlock()
					return
				}
			}
		NEXT_ITEM_B:
			for itemB := &b.buckets[bix]; itemB != nil; itemB = itemB.next {
				if itemB.blockID == 0 || itemB.keyA < pstart || itemB.keyA > pstop {
					continue
				}
				for itemA := &a.buckets[bix]; itemA != nil; itemA = itemA.next {
					if itemA.blockID == 0 {
						continue
					}
					if itemA.keyA == itemB.keyA && itemA.keyB == itemB.keyB {
						if itemA.timestamp >= itemB.timestamp {
							continue NEXT_ITEM_B
						}
						break
					}
				}
				if !callback(itemB.keyA, itemB.keyB, itemB.timestamp, itemB.blockID, itemB.offset, itemB.length) {
					a.locks[lix].RUnlock()
					b.locks[lix].RUnlock()
					return
				}
			}
			a.locks[lix].RUnlock()
			b.locks[lix].RUnlock()
		}
	}
}
