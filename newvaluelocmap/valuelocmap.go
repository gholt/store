// Package newvaluelocmap provides a concurrency-safe data structure that maps
// keys to value locations. A key is 128 bits and is specified using two
// uint64s (keyA, keyB). A value location is specified using a blockID, offset,
// and length triplet. Each mapping is assigned a timestamp and the greatest
// timestamp wins. The timestamp is also used to indicate a deletion marker; if
// timestamp & 1 == 1 then the mapping is considered a mark for deletion at
// that time. Deletion markers are used in case mappings come in out of order
// and for replication to others that may have missed the deletion.
//
// This implementation essentially uses a tree structure of slices of key to
// location assignments. When a slice fills up, an additional slice is created
// and half the data is moved to the new slice and the tree structure grows. If
// a slice empties, it is merged with its pair in the tree structure and the
// tree shrinks. The tree is balanced by high bits of the key, and locations
// are distributed in the slices by the low bits.
//
// There are also functions for scanning key ranges, both to clean out old
// tombstones and to provide callbacks for replication or other tasks.
package newvaluelocmap

import (
	"fmt"
	"math/rand"
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
	roots           int
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
	if env := os.Getenv("BRIMSTORE_VALUELOCMAP_ROOTS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.roots = val
		}
	}
	if cfg.roots <= 0 {
		cfg.roots = cfg.cores * cfg.cores
	}
	if env := os.Getenv("BRIMSTORE_VALUELOCMAP_PAGESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.pageSize = val
		}
	}
	if cfg.pageSize <= 0 {
		cfg.pageSize = 1048576
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
	if cfg.roots < 2 {
		cfg.roots = 2
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
// of locks to create, for example). Defaults to env
// BRIMSTORE_VALUELOCMAP_CORES, BRIMSTORE_CORES, or GOMAXPROCS.
func OptCores(cores int) func(*config) {
	return func(cfg *config) {
		cfg.cores = cores
	}
}

// OptRoots indicates how many top level nodes the map should have. More top
// level nodes means less contention but a bit more memory. Defaults to
// BRIMSTORE_VALUELOCMAP_ROOTS or the OptCores value squared. This will be
// rounded up to the next power of two.
func OptRoots(roots int) func(*config) {
	return func(cfg *config) {
		cfg.roots = roots
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
	bits            uint32
	lowMask         uint32
	entriesLockMask uint32
	cores           uint32
	splitCount      uint32
	rootsShift      uint64
	roots           []node
}

type entry struct {
	keyA      uint64
	keyB      uint64
	timestamp uint64
	blockID   uint32
	offset    uint32
	length    uint32
	next      uint32
}

type node struct {
	resizingLock       sync.Mutex
	resizing           bool
	lock               sync.RWMutex
	highMask           uint64
	rangeStart         uint64
	rangeStop          uint64
	splitCount         uint32
	mergeCount         uint32
	a                  *node
	b                  *node
	entries            []entry
	entriesLocks       []sync.RWMutex
	overflow           [][]entry
	overflowLowestFree uint32
	overflowLock       sync.RWMutex
	used               uint32
}

type stats struct {
	statsDebug        bool
	cores             uint32
	roots             uint32
	usedRoots         uint32
	entryPageSize     uint64
	entryLockPageSize uint64
	splitCount        uint32
	nodes             uint64
	depthCounts       []uint64
	allocedEntries    uint64
	allocedInOverflow uint64
	usedEntries       uint64
	usedInOverflow    uint64
	tombstones        uint64
	active            uint64
	length            uint64
}

func NewValueLocMap(opts ...func(*config)) *ValueLocMap {
	cfg := resolveConfig(opts...)
	vlm := &ValueLocMap{cores: uint32(cfg.cores)}
	est := uint32(cfg.pageSize / int(unsafe.Sizeof(entry{})))
	if est < 1 {
		est = 1
	}
	vlm.bits = 1
	c := uint32(2)
	for c < est {
		vlm.bits++
		c <<= 1
	}
	vlm.lowMask = c - 1
	vlm.rootsShift = 63
	c = 2
	for c < uint32(cfg.roots) {
		vlm.rootsShift--
		c <<= 1
	}
	vlm.entriesLockMask = c - 1
	vlm.splitCount = uint32(float64(uint32(1<<vlm.bits)) * cfg.splitMultiplier)
	vlm.roots = make([]node, c)
	j := uint64(1<<(64-vlm.rootsShift)) - 1
	for i := 0; i < len(vlm.roots); i++ {
		vlm.roots[i].highMask = 1 << (vlm.rootsShift - 1)
		vlm.roots[i].rangeStart = uint64(i) << vlm.rootsShift
		vlm.roots[i].rangeStop = vlm.roots[i].rangeStart + j
		vlm.roots[i].splitCount = uint32(float64(vlm.splitCount) + (rand.Float64()-.5)/5*float64(vlm.splitCount))
		vlm.roots[i].mergeCount = uint32(rand.Float64() / 10 * float64(vlm.splitCount))
	}
	return vlm
}

func (vlm *ValueLocMap) split(n *node) {
	n.resizingLock.Lock()
	if n.resizing {
		n.resizingLock.Unlock()
		return
	}
	n.resizing = true
	n.resizingLock.Unlock()
	go vlm.split2(n)
}

func (vlm *ValueLocMap) split2(n *node) {
	n.lock.Lock()
	hm := n.highMask
	an := &node{
		highMask:           hm >> 1,
		rangeStart:         n.rangeStart,
		rangeStop:          n.rangeStop - hm,
		splitCount:         uint32(float64(vlm.splitCount) + (rand.Float64()-.5)/5*float64(vlm.splitCount)),
		mergeCount:         uint32(rand.Float64() / 10 * float64(vlm.splitCount)),
		entries:            n.entries,
		entriesLocks:       n.entriesLocks,
		overflow:           n.overflow,
		overflowLowestFree: n.overflowLowestFree,
		used:               n.used,
	}
	bn := &node{
		highMask:     hm >> 1,
		rangeStart:   n.rangeStart + hm,
		rangeStop:    n.rangeStop,
		splitCount:   uint32(float64(vlm.splitCount) + (rand.Float64()-.5)/5*float64(vlm.splitCount)),
		mergeCount:   uint32(rand.Float64() / 10 * float64(vlm.splitCount)),
		entries:      make([]entry, len(n.entries)),
		entriesLocks: make([]sync.RWMutex, len(n.entriesLocks)),
	}
	n.a = an
	n.b = bn
	n.entries = nil
	n.entriesLocks = nil
	n.overflow = nil
	b := vlm.bits
	lm := vlm.lowMask
	ao := an.overflow
	bes := bn.entries
	bo := bn.overflow
	boc := uint32(0)
	// Move over all matching overflow entries that have an overflow entry
	// pointing to them.
	c := true
	for c {
		c = false
		for _, aes := range ao {
			for j := uint32(0); j <= lm; j++ {
				ae := &aes[j]
				if ae.blockID != 0 && ae.next != 0 {
					aen := &ao[ae.next>>b][ae.next&lm]
					if aen.keyA&hm != 0 {
						be := &bes[uint32(aen.keyB)&lm]
						if be.blockID == 0 {
							*be = *aen
							be.next = 0
						} else {
							if bn.overflowLowestFree != 0 {
								be2 := &bo[bn.overflowLowestFree>>b][bn.overflowLowestFree&lm]
								*be2 = *aen
								be2.next = be.next
								be.next = bn.overflowLowestFree
								bn.overflowLowestFree++
								if bn.overflowLowestFree&lm == 0 {
									bn.overflowLowestFree = 0
								}
							} else {
								bo = append(bo, make([]entry, 1<<b))
								bn.overflow = bo
								if boc == 0 {
									be2 := &bo[0][1]
									*be2 = *aen
									be2.next = be.next
									be.next = 1
									bn.overflowLowestFree = 2
								} else {
									be2 := &bo[boc][0]
									*be2 = *aen
									be2.next = be.next
									be.next = boc << b
									bn.overflowLowestFree = boc<<b + 1
								}
								boc++
							}
						}
						bn.used++
						if ae.next < an.overflowLowestFree {
							an.overflowLowestFree = ae.next
						}
						ae.next = aen.next
						aen.blockID = 0
						an.used--
						c = true
					}
				}
			}
		}
	}
	// Now any matching overflow entries left are pointed to by their
	// respective non-overflow entry. Move those.
	aes := an.entries
	for i := uint32(0); i <= lm; i++ {
		ae := &aes[i]
		if ae.blockID != 0 && ae.next != 0 {
			aen := &ao[ae.next>>b][ae.next&lm]
			if aen.keyA&hm != 0 {
				be := &bes[i]
				if be.blockID == 0 {
					*be = *aen
					be.next = 0
				} else {
					if bn.overflowLowestFree != 0 {
						be2 := &bo[bn.overflowLowestFree>>b][bn.overflowLowestFree&lm]
						*be2 = *aen
						be2.next = be.next
						be.next = bn.overflowLowestFree
						bn.overflowLowestFree++
						if bn.overflowLowestFree&lm == 0 {
							bn.overflowLowestFree = 0
						}
					} else {
						bo = append(bo, make([]entry, 1<<b))
						bn.overflow = bo
						if boc == 0 {
							be2 := &bo[0][1]
							*be2 = *aen
							be2.next = be.next
							be.next = 1
							bn.overflowLowestFree = 2
						} else {
							be2 := &bo[boc][0]
							*be2 = *aen
							be2.next = be.next
							be.next = boc << b
							bn.overflowLowestFree = boc<<b + 1
						}
						boc++
					}
				}
				bn.used++
				if ae.next < an.overflowLowestFree {
					an.overflowLowestFree = ae.next
				}
				ae.next = aen.next
				aen.blockID = 0
				an.used--
			}
		}
	}
	// Now any matching entries left are non-overflow entries. Move those.
	for i := uint32(0); i <= lm; i++ {
		ae := &aes[i]
		if ae.blockID != 0 && ae.keyA&hm != 0 {
			be := &bes[i]
			if be.blockID == 0 {
				*be = *ae
				be.next = 0
			} else {
				if bn.overflowLowestFree != 0 {
					be2 := &bo[bn.overflowLowestFree>>b][bn.overflowLowestFree&lm]
					*be2 = *ae
					be2.next = be.next
					be.next = bn.overflowLowestFree
					bn.overflowLowestFree++
					if bn.overflowLowestFree&lm == 0 {
						bn.overflowLowestFree = 0
					}
				} else {
					bo = append(bo, make([]entry, 1<<b))
					bn.overflow = bo
					if boc == 0 {
						be2 := &bo[0][1]
						*be2 = *ae
						be2.next = be.next
						be.next = 1
						bn.overflowLowestFree = 2
					} else {
						be2 := &bo[boc][0]
						*be2 = *ae
						be2.next = be.next
						be.next = boc << b
						bn.overflowLowestFree = boc<<b + 1
					}
					boc++
				}
			}
			bn.used++
			if ae.next == 0 {
				ae.blockID = 0
				ae.timestamp = 0
			} else {
				if ae.next < an.overflowLowestFree {
					an.overflowLowestFree = ae.next
				}
				aen := &ao[ae.next>>b][ae.next&lm]
				*ae = *aen
				aen.blockID = 0
				aen.next = 0
			}
			an.used--
		}
	}
	n.lock.Unlock()
	n.resizingLock.Lock()
	n.resizing = false
	n.resizingLock.Unlock()
}

func (vlm *ValueLocMap) merge(n *node) {
	n.resizingLock.Lock()
	if n.resizing {
		n.resizingLock.Unlock()
		return
	}
	n.resizing = true
	n.resizingLock.Unlock()
	go vlm.merge2(n)
}

func (vlm *ValueLocMap) merge2(n *node) {
	n.lock.Lock()
	an := n.a
	bn := n.b
	an.lock.Lock()
	bn.lock.Lock()
	n.a = nil
	n.b = nil
	n.entries = an.entries
	n.entriesLocks = an.entriesLocks
	n.overflow = an.overflow
	n.overflowLowestFree = an.overflowLowestFree
	n.used = an.used
	b := vlm.bits
	lm := vlm.lowMask
	aes := an.entries
	ao := an.overflow
	aoc := uint32(len(ao))
	bo := bn.overflow
	// Move over all overflow entries that have an overflow entry pointing to
	// them.
	c := true
	for c {
		c = false
		for _, bes := range bo {
			for j := uint32(0); j <= lm; j++ {
				be := &bes[j]
				if be.blockID != 0 && be.next != 0 {
					ben := &bo[be.next>>b][be.next&lm]
					ae := &aes[uint32(ben.keyB)&lm]
					if ae.blockID == 0 {
						*ae = *ben
						ae.next = 0
					} else {
						if an.overflowLowestFree != 0 {
							oA := an.overflowLowestFree >> b
							oB := an.overflowLowestFree & lm
							ae2 := &ao[oA][oB]
							*ae2 = *ben
							ae2.next = ae.next
							ae.next = an.overflowLowestFree
							an.overflowLowestFree = 0
							for {
								if oB == lm {
									oA++
									if oA == aoc {
										break
									}
									oB = 0
								} else {
									oB++
								}
								if ao[oA][oB].blockID == 0 {
									an.overflowLowestFree = oA<<b | oB
									break
								}
							}
						} else {
							ao = append(ao, make([]entry, 1<<b))
							an.overflow = ao
							if aoc == 0 {
								ae2 := &ao[0][1]
								*ae2 = *ben
								ae2.next = ae.next
								ae.next = 1
								an.overflowLowestFree = 2
							} else {
								ae2 := &ao[aoc][0]
								*ae2 = *ben
								ae2.next = ae.next
								ae.next = aoc << b
								an.overflowLowestFree = aoc<<b + 1
							}
							aoc++
						}
					}
					an.used++
					be.next = ben.next
					ben.blockID = 0
					c = true
				}
			}
		}
	}
	// Now we just have overflow entries that are pointed to by their
	// respective non-overflow entries. Move those.
	bes := bn.entries
	for i := uint32(0); i <= lm; i++ {
		be := &bes[i]
		if be.blockID != 0 && be.next != 0 {
			ben := &bo[be.next>>b][be.next&lm]
			ae := &aes[i]
			if ae.blockID == 0 {
				*ae = *ben
				ae.next = 0
			} else {
				if an.overflowLowestFree != 0 {
					oA := an.overflowLowestFree >> b
					oB := an.overflowLowestFree & lm
					ae2 := &ao[oA][oB]
					*ae2 = *ben
					ae2.next = ae.next
					ae.next = an.overflowLowestFree
					an.overflowLowestFree = 0
					for {
						if oB == lm {
							oA++
							if oA == aoc {
								break
							}
							oB = 0
						} else {
							oB++
						}
						if ao[oA][oB].blockID == 0 {
							an.overflowLowestFree = oA<<b | oB
							break
						}
					}
				} else {
					ao = append(ao, make([]entry, 1<<b))
					an.overflow = ao
					if aoc == 0 {
						ae2 := &ao[0][1]
						*ae2 = *ben
						ae2.next = ae.next
						ae.next = 1
						an.overflowLowestFree = 2
					} else {
						ae2 := &ao[aoc][0]
						*ae2 = *ben
						ae2.next = ae.next
						ae.next = aoc << b
						an.overflowLowestFree = aoc<<b + 1
					}
					aoc++
				}
			}
			an.used++
			ben.blockID = 0
		}
	}
	// Now we just have the non-overflow entries. Move those.
	for i := uint32(0); i <= lm; i++ {
		be := &bes[i]
		if be.blockID != 0 {
			ae := &aes[i]
			if ae.blockID == 0 {
				*ae = *be
				ae.next = 0
			} else {
				if an.overflowLowestFree != 0 {
					oA := an.overflowLowestFree >> b
					oB := an.overflowLowestFree & lm
					ae2 := &ao[oA][oB]
					*ae2 = *be
					ae2.next = ae.next
					ae.next = an.overflowLowestFree
					an.overflowLowestFree = 0
					for {
						if oB == lm {
							oA++
							if oA == aoc {
								break
							}
							oB = 0
						} else {
							oB++
						}
						if ao[oA][oB].blockID == 0 {
							an.overflowLowestFree = oA<<b | oB
							break
						}
					}
				} else {
					ao = append(ao, make([]entry, 1<<b))
					an.overflow = ao
					if aoc == 0 {
						ae2 := &ao[0][1]
						*ae2 = *be
						ae2.next = ae.next
						ae.next = 1
						an.overflowLowestFree = 2
					} else {
						ae2 := &ao[aoc][0]
						*ae2 = *be
						ae2.next = ae.next
						ae.next = aoc << b
						an.overflowLowestFree = aoc<<b + 1
					}
					aoc++
				}
			}
			an.used++
			be.blockID = 0
		}
	}
	bn.used = 0
	bn.lock.Unlock()
	an.lock.Unlock()
	n.lock.Unlock()
	n.resizingLock.Lock()
	n.resizing = false
	n.resizingLock.Unlock()
}

// TODO: Change blockID returned to uint32
func (vlm *ValueLocMap) Get(keyA uint64, keyB uint64) (uint64, uint16, uint32, uint32) {
	n := &vlm.roots[keyA>>vlm.rootsShift]
	n.lock.RLock()
	for {
		if n.a == nil {
			if n.entries == nil {
				n.lock.RUnlock()
				return 0, 0, 0, 0
			}
			break
		}
		l := &n.lock
		if keyA&n.highMask == 0 {
			n = n.a
		} else {
			n = n.b
		}
		n.lock.RLock()
		l.RUnlock()
	}
	b := vlm.bits
	lm := vlm.lowMask
	i := uint32(keyB) & lm
	l := &n.entriesLocks[i&vlm.entriesLockMask]
	ol := &n.overflowLock
	e := &n.entries[i]
	l.RLock()
	for {
		if e.keyA == keyA && e.keyB == keyB {
			rt := e.timestamp
			rb := e.blockID
			ro := e.offset
			rl := e.length
			l.RUnlock()
			n.lock.RUnlock()
			// TODO: Change rb to return full uint32
			return rt, uint16(rb), ro, rl
		}
		if e.next == 0 {
			break
		}
		ol.RLock()
		e = &n.overflow[e.next>>b][e.next&lm]
		ol.RUnlock()
	}
	l.RUnlock()
	n.lock.RUnlock()
	return 0, 0, 0, 0
}

// TODO: Change blockID to be uint32
func (vlm *ValueLocMap) Set(keyA uint64, keyB uint64, timestamp uint64, blockID uint16, offset uint32, length uint32, evenIfSameTimestamp bool) uint64 {
	bblockID := uint32(blockID)
	n := &vlm.roots[keyA>>vlm.rootsShift]
	var pn *node
	n.lock.RLock()
	for {
		if n.a == nil {
			if n.entries == nil {
				n.lock.RUnlock()
				n.lock.Lock()
				if n.entries == nil {
					n.entries = make([]entry, 1<<vlm.bits)
					n.entriesLocks = make([]sync.RWMutex, vlm.entriesLockMask+1)
				}
				n.lock.Unlock()
				n.lock.RLock()
			}
			break
		}
		pn = n
		if keyA&n.highMask == 0 {
			n = n.a
		} else {
			n = n.b
		}
		n.lock.RLock()
		pn.lock.RUnlock()
	}
	b := vlm.bits
	lm := vlm.lowMask
	i := uint32(keyB) & lm
	l := &n.entriesLocks[i&vlm.entriesLockMask]
	ol := &n.overflowLock
	e := &n.entries[i]
	var ep *entry
	l.Lock()
	for {
		var f uint32
		if e.keyA == keyA && e.keyB == keyB {
			if e.timestamp < timestamp {
				if bblockID != 0 {
					if e.blockID != 0 {
						t := e.timestamp
						e.timestamp = timestamp
						e.blockID = bblockID
						e.offset = offset
						e.length = length
						l.Unlock()
						n.lock.RUnlock()
						return t
					}
					e.timestamp = timestamp
					e.blockID = bblockID
					e.offset = offset
					e.length = length
					u := atomic.AddUint32(&n.used, 1)
					l.Unlock()
					n.lock.RUnlock()
					if u >= n.splitCount {
						vlm.split(n)
					}
					return 0
				}
				if e.blockID != 0 {
					t := e.timestamp
					e.timestamp = 0
					e.blockID = 0
					if ep != nil {
						ep.next = e.next
					}
					u := atomic.AddUint32(&n.used, ^uint32(0))
					if f != 0 {
						ol.Lock()
						if f < n.overflowLowestFree {
							n.overflowLowestFree = f
						}
						ol.Unlock()
					}
					l.Unlock()
					n.lock.RUnlock()
					if u <= n.mergeCount && pn != nil {
						vlm.merge(pn)
					}
					return t
				}
				l.Unlock()
				n.lock.RUnlock()
				return 0
			} else if evenIfSameTimestamp && e.timestamp == timestamp {
				if bblockID != 0 {
					if e.blockID != 0 {
						e.blockID = bblockID
						e.offset = offset
						e.length = length
						l.Unlock()
						n.lock.RUnlock()
						return timestamp
					}
					e.blockID = bblockID
					e.offset = offset
					e.length = length
					u := atomic.AddUint32(&n.used, 1)
					l.Unlock()
					n.lock.RUnlock()
					if u >= n.splitCount {
						vlm.split(n)
					}
					return timestamp
				} else if e.blockID != 0 {
					e.timestamp = 0
					e.blockID = 0
					u := atomic.AddUint32(&n.used, ^uint32(0))
					if f != 0 {
						ol.Lock()
						if f < n.overflowLowestFree {
							n.overflowLowestFree = f
						}
						ol.Unlock()
					}
					l.Unlock()
					n.lock.RUnlock()
					if u <= n.mergeCount && pn != nil {
						vlm.merge(pn)
					}
					return timestamp
				}
				l.Unlock()
				n.lock.RUnlock()
				return timestamp
			}
			l.Unlock()
			n.lock.RUnlock()
			return e.timestamp
		}
		if e.next == 0 {
			break
		}
		ep = e
		f = e.next
		ol.RLock()
		e = &n.overflow[f>>b][f&lm]
		ol.RUnlock()
	}
	var u uint32
	if bblockID != 0 {
		e = &n.entries[i]
		if e.blockID != 0 {
			ol.Lock()
			o := n.overflow
			oc := uint32(len(o))
			if n.overflowLowestFree != 0 {
				oA := n.overflowLowestFree >> b
				oB := n.overflowLowestFree & lm
				e = &o[oA][oB]
				e.next = n.entries[i].next
				n.entries[i].next = n.overflowLowestFree
				n.overflowLowestFree = 0
				for {
					if oB == lm {
						oA++
						if oA == oc {
							break
						}
						oB = 0
					} else {
						oB++
					}
					if o[oA][oB].blockID == 0 {
						n.overflowLowestFree = oA<<b | oB
						break
					}
				}
			} else {
				n.overflow = append(n.overflow, make([]entry, 1<<b))
				if oc == 0 {
					e = &n.overflow[0][1]
					e.next = n.entries[i].next
					n.entries[i].next = 1
					n.overflowLowestFree = 2
				} else {
					e = &n.overflow[oc][0]
					e.next = n.entries[i].next
					n.entries[i].next = oc << b
					n.overflowLowestFree = oc<<b + 1
				}
			}
			ol.Unlock()
		}
		e.keyA = keyA
		e.keyB = keyB
		e.timestamp = timestamp
		e.blockID = bblockID
		e.offset = offset
		e.length = length
		u = atomic.AddUint32(&n.used, 1)
	}
	l.Unlock()
	n.lock.RUnlock()
	if u >= n.splitCount {
		vlm.split(n)
	}
	return 0
}

func (vlm *ValueLocMap) GatherStats(debug bool) (uint64, uint64, fmt.Stringer) {
	s := &stats{
		statsDebug:        debug,
		cores:             vlm.cores,
		roots:             uint32(len(vlm.roots)),
		entryPageSize:     uint64(vlm.lowMask) + 1,
		entryLockPageSize: uint64(vlm.entriesLockMask) + 1,
		splitCount:        uint32(vlm.splitCount),
	}
	for i := uint32(0); i < s.roots; i++ {
		n := &vlm.roots[i]
		n.lock.Lock() // Will be released by gatherStats
		if s.statsDebug && (n.a != nil || n.entries != nil) {
			s.usedRoots++
		}
		vlm.gatherStats(s, n, 0)
	}
	return s.active, s.length, s
}

// Will call n.lock.Unlock()
func (vlm *ValueLocMap) gatherStats(s *stats, n *node, depth int) {
	if s.statsDebug {
		s.nodes++
		for len(s.depthCounts) <= depth {
			s.depthCounts = append(s.depthCounts, 0)
		}
		s.depthCounts[depth]++
	}
	if n.a != nil {
		n.a.lock.Lock() // Will be released by gatherStats
		n.lock.Unlock()
		vlm.gatherStats(s, n.a, depth+1)
		n.lock.Lock()
		if n.b != nil {
			n.b.lock.Lock() // Will be released by gatherStats
			n.lock.Unlock()
			vlm.gatherStats(s, n.b, depth+1)
		}
	} else {
		if s.statsDebug {
			s.allocedEntries += uint64(len(n.entries))
			for _, o := range n.overflow {
				s.allocedEntries += uint64(len(o))
				s.allocedInOverflow += uint64(len(o))
			}
		}
		lm := vlm.lowMask
		es := n.entries
		for i := uint32(0); i <= lm; i++ {
			e := &es[i]
			if e.blockID != 0 {
				if s.statsDebug {
					s.usedEntries++
					if e.timestamp&1 == 0 {
						s.active++
						s.length += uint64(e.length)
					} else {
						s.tombstones++
					}
				} else if e.timestamp&1 == 0 {
					s.active++
					s.length += uint64(e.length)
				}
			}
		}
		for _, es = range n.overflow {
			for i := uint32(0); i <= lm; i++ {
				e := &es[i]
				if e.blockID != 0 {
					if s.statsDebug {
						s.usedEntries++
						s.usedInOverflow++
						if e.timestamp&1 == 0 {
							s.active++
							s.length += uint64(e.length)
						} else {
							s.tombstones++
						}
					} else if e.timestamp&1 == 0 {
						s.active++
						s.length += uint64(e.length)
					}
				}
			}
		}
		n.lock.Unlock()
	}
}

func (vlm *ValueLocMap) ScanCount(start uint64, stop uint64, max uint64) uint64 {
	var c uint64
	for i := 0; i < len(vlm.roots); i++ {
		n := &vlm.roots[i]
		n.lock.RLock() // Will be released by scanCount
		c += vlm.scanCount(start, stop, max, n)
	}
	return c
}

// Will call n.lock.RUnlock()
func (vlm *ValueLocMap) scanCount(start uint64, stop uint64, max uint64, n *node) uint64 {
	if start > n.rangeStop || stop < n.rangeStart {
		n.lock.RUnlock()
		return 0
	}
	if n.a != nil {
		n.a.lock.RLock() // Will be released by scanCount
		n.lock.RUnlock()
		c := vlm.scanCount(start, stop, max, n.a)
		n.lock.RLock()
		if n.b != nil {
			n.b.lock.RLock() // Will be released by scanCount
			n.lock.RUnlock()
			c += vlm.scanCount(start, stop, max, n.b)
		}
		return c
	} else if n.used == 0 {
		n.lock.RUnlock()
		return 0
	} else if start <= n.rangeStart && stop >= n.rangeStop {
		n.lock.RUnlock()
		return uint64(atomic.LoadUint32(&n.used))
	} else {
		var c uint64
		b := vlm.bits
		lm := vlm.lowMask
		es := n.entries
		ol := &n.overflowLock
		for i := uint32(0); i <= lm; i++ {
			e := &es[i]
			l := &n.entriesLocks[i&vlm.entriesLockMask]
			l.RLock()
			if e.blockID != 0 {
				for {
					if e.keyA >= start && e.keyA <= stop {
						c++
					}
					if e.next == 0 {
						break
					}
					ol.RLock()
					e = &n.overflow[e.next>>b][e.next&lm]
					ol.RUnlock()
				}
			}
			l.RUnlock()
		}
		n.lock.RUnlock()
		return c
	}
}

func (vlm *ValueLocMap) ScanCallback(start uint64, stop uint64, callback func(keyA uint64, keyB uint64, timestamp uint64)) {
	for i := 0; i < len(vlm.roots); i++ {
		n := &vlm.roots[i]
		n.lock.RLock() // Will be released by scanCallback
		vlm.scanCallback(start, stop, callback, n)
	}
}

// Will call n.lock.RUnlock()
func (vlm *ValueLocMap) scanCallback(start uint64, stop uint64, callback func(keyA uint64, keyB uint64, timestamp uint64), n *node) {
	if start > n.rangeStop || stop < n.rangeStart {
		n.lock.RUnlock()
		return
	}
	if n.a != nil {
		n.a.lock.RLock() // Will be released by scanCallback
		n.lock.RUnlock()
		vlm.scanCallback(start, stop, callback, n.a)
		n.lock.RLock()
		if n.b != nil {
			n.b.lock.RLock() // Will be released by scanCallback
			n.lock.RUnlock()
			vlm.scanCallback(start, stop, callback, n.b)
		}
	} else if n.used == 0 {
		n.lock.RUnlock()
	} else if start <= n.rangeStart && stop >= n.rangeStop {
		b := vlm.bits
		lm := vlm.lowMask
		es := n.entries
		ol := &n.overflowLock
		for i := uint32(0); i <= lm; i++ {
			e := &es[i]
			l := &n.entriesLocks[i&vlm.entriesLockMask]
			l.RLock()
			if e.blockID != 0 {
				for {
					callback(e.keyA, e.keyB, e.timestamp)
					if e.next == 0 {
						break
					}
					ol.RLock()
					e = &n.overflow[e.next>>b][e.next&lm]
					ol.RUnlock()
				}
			}
			l.RUnlock()
		}
		n.lock.RUnlock()
	} else {
		b := vlm.bits
		lm := vlm.lowMask
		es := n.entries
		ol := &n.overflowLock
		for i := uint32(0); i <= lm; i++ {
			e := &es[i]
			l := &n.entriesLocks[i&vlm.entriesLockMask]
			l.RLock()
			if e.blockID != 0 {
				for {
					if e.keyA >= start && e.keyA <= stop {
						callback(e.keyA, e.keyB, e.timestamp)
					}
					if e.next == 0 {
						break
					}
					ol.RLock()
					e = &n.overflow[e.next>>b][e.next&lm]
					ol.RUnlock()
				}
			}
			l.RUnlock()
		}
		n.lock.RUnlock()
	}
}

func (s *stats) String() string {
	if s.statsDebug {
		depthCounts := fmt.Sprintf("%d", s.depthCounts[0])
		for i := 1; i < len(s.depthCounts); i++ {
			depthCounts += fmt.Sprintf(" %d", s.depthCounts[i])
		}
		return brimtext.Align([][]string{
			[]string{"cores", fmt.Sprintf("%d", s.cores)},
			[]string{"roots", fmt.Sprintf("%d (%d bytes)", s.roots, uint64(s.roots)*uint64(unsafe.Sizeof(node{})))},
			[]string{"usedRoots", fmt.Sprintf("%d", s.usedRoots)},
			[]string{"entryPageSize", fmt.Sprintf("%d (%d bytes)", s.entryPageSize, uint64(s.entryPageSize)*uint64(unsafe.Sizeof(entry{})))},
			[]string{"entryLockPageSize", fmt.Sprintf("%d (%d bytes)", s.entryLockPageSize, uint64(s.entryLockPageSize)*uint64(unsafe.Sizeof(sync.RWMutex{})))},
			[]string{"splitCount", fmt.Sprintf("%d +-10%%", s.splitCount)},
			[]string{"nodes", fmt.Sprintf("%d", s.nodes)},
			[]string{"depth", fmt.Sprintf("%d", len(s.depthCounts))},
			[]string{"depthCounts", depthCounts},
			[]string{"allocedEntries", fmt.Sprintf("%d", s.allocedEntries)},
			[]string{"allocedInOverflow", fmt.Sprintf("%d %.1f%%", s.allocedInOverflow, 100*float64(s.allocedInOverflow)/float64(s.allocedEntries))},
			[]string{"usedEntries", fmt.Sprintf("%d %.1f%%", s.usedEntries, 100*float64(s.usedEntries)/float64(s.allocedEntries))},
			[]string{"usedInOverflow", fmt.Sprintf("%d %.1f%%", s.usedInOverflow, 100*float64(s.usedInOverflow)/float64(s.usedEntries))},
			[]string{"tombstones", fmt.Sprintf("%d %.1f%%", s.tombstones, 100*float64(s.tombstones)/float64(s.usedEntries))},
			[]string{"active", fmt.Sprintf("%d %.1f%%", s.active, 100*float64(s.active)/float64(s.usedEntries))},
			[]string{"length", fmt.Sprintf("%d", s.length)},
		}, nil)
	} else {
		return brimtext.Align([][]string{}, nil)
	}
}
