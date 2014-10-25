package main

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type entry struct {
	keyA      uint64
	keyB      uint64
	timestamp uint64
	blockID   uint32
	offset    uint32
	length    uint32
	next      uint32
}

type OverflowMap struct {
	bits               uint32
	mask               uint32
	buckets            []entry
	bucketLockMask     uint32
	bucketLocks        []sync.RWMutex
	overflow           [][]entry
	overflowLowestFree uint32
	overflowLock       sync.RWMutex
	used               uint32
}

func NewOverflowMap(bits uint32, lockBits uint32) *OverflowMap {
	// Needed so the overflow logic can be simpler.
	if bits < 2 {
		bits = 2
	}
	if lockBits < 1 {
		lockBits = 1
	}
	return &OverflowMap{
		bits:           bits,
		mask:           (1 << bits) - 1,
		buckets:        make([]entry, 1<<bits),
		bucketLockMask: (1 << lockBits) - 1,
		bucketLocks:    make([]sync.RWMutex, 1<<lockBits),
	}
}

// Ensure nothing writes to the OverflowMap when calling Move.
func (om *OverflowMap) Move(leftMask uint64) *OverflowMap {
	b := om.bits
	m := om.mask
	nom := &OverflowMap{
		bits:           b,
		mask:           m,
		buckets:        make([]entry, len(om.buckets)),
		bucketLockMask: om.bucketLockMask,
		bucketLocks:    make([]sync.RWMutex, len(om.bucketLocks)),
	}
	o := om.overflow
	nes := nom.buckets
	noc := uint32(0)
	c := true
	for c {
		c = false
		for _, es := range om.overflow {
			for i := uint32(0); i <= m; i++ {
				e := &es[i]
				if e.blockID != 0 && e.next != 0 {
					en := &om.overflow[e.next>>b][e.next&m]
					if en.keyA&leftMask != 0 {
						ne := &nes[uint32(en.keyB)&m]
						if ne.blockID == 0 {
							*ne = *en
							ne.next = 0
						} else {
							if nom.overflowLowestFree != 0 {
								ne2 := &nom.overflow[nom.overflowLowestFree>>b][nom.overflowLowestFree&m]
								*ne2 = *en
								ne2.next = ne.next
								ne.next = nom.overflowLowestFree
								nom.overflowLowestFree++
								if nom.overflowLowestFree&m == 0 {
									nom.overflowLowestFree = 0
								}
							} else {
								nom.overflow = append(nom.overflow, make([]entry, 1<<b))
								if noc == 0 {
									ne2 := &nom.overflow[0][1]
									*ne2 = *en
									ne2.next = ne.next
									ne.next = 1
									nom.overflowLowestFree = 2
								} else {
									ne2 := &nom.overflow[noc][0]
									*ne2 = *en
									ne2.next = ne.next
									ne.next = noc << b
									nom.overflowLowestFree = noc<<b + 1
								}
								noc++
							}
						}
						nom.used++
						if e.next < om.overflowLowestFree {
							om.overflowLowestFree = e.next
						}
						e.next = en.next
						en.blockID = 0
						om.used--
						c = true
					}
				}
			}
		}
	}
	es := om.buckets
	for i := uint32(0); i <= m; i++ {
		e := &es[i]
		if e.blockID != 0 && e.next != 0 {
			en := &om.overflow[e.next>>b][e.next&m]
			if en.keyA&leftMask != 0 {
				ne := &nes[i]
				if ne.blockID == 0 {
					*ne = *en
					ne.next = 0
				} else {
					if nom.overflowLowestFree != 0 {
						ne2 := &nom.overflow[nom.overflowLowestFree>>b][nom.overflowLowestFree&m]
						*ne2 = *en
						ne2.next = ne.next
						ne.next = nom.overflowLowestFree
						nom.overflowLowestFree++
						if nom.overflowLowestFree&m == 0 {
							nom.overflowLowestFree = 0
						}
					} else {
						nom.overflow = append(nom.overflow, make([]entry, 1<<b))
						if noc == 0 {
							ne2 := &nom.overflow[0][1]
							*ne2 = *en
							ne2.next = ne.next
							ne.next = 1
							nom.overflowLowestFree = 2
						} else {
							ne2 := &nom.overflow[noc][0]
							*ne2 = *en
							ne2.next = ne.next
							ne.next = noc << b
							nom.overflowLowestFree = noc<<b + 1
						}
						noc++
					}
				}
				nom.used++
				if e.next < om.overflowLowestFree {
					om.overflowLowestFree = e.next
				}
				e.next = en.next
				en.blockID = 0
				om.used--
			}
		}
	}
	es = om.buckets
	for i := uint32(0); i <= m; i++ {
		e := &es[i]
		for e.blockID != 0 && e.keyA&leftMask != 0 {
			ne := &nes[i]
			if ne.blockID == 0 {
				*ne = *e
				ne.next = 0
			} else {
				if nom.overflowLowestFree != 0 {
					ne2 := &nom.overflow[nom.overflowLowestFree>>b][nom.overflowLowestFree&m]
					*ne2 = *e
					ne2.next = ne.next
					ne.next = nom.overflowLowestFree
					nom.overflowLowestFree++
					if nom.overflowLowestFree&m == 0 {
						nom.overflowLowestFree = 0
					}
				} else {
					nom.overflow = append(nom.overflow, make([]entry, 1<<b))
					if noc == 0 {
						ne2 := &nom.overflow[0][1]
						*ne2 = *e
						ne2.next = ne.next
						ne.next = 1
						nom.overflowLowestFree = 2
					} else {
						ne2 := &nom.overflow[noc][0]
						*ne2 = *e
						ne2.next = ne.next
						ne.next = noc << b
						nom.overflowLowestFree = noc<<b + 1
					}
					noc++
				}
			}
			nom.used++
			if e.next == 0 {
				e.blockID = 0
				e.timestamp = 0
			} else {
				if e.next < om.overflowLowestFree {
					om.overflowLowestFree = e.next
				}
				n := &o[e.next>>b][e.next&m]
				*e = *n
				n.blockID = 0
				n.next = 0
			}
			om.used--
		}
	}
	return nom
}

func (om *OverflowMap) Get(keyA uint64, keyB uint64) (uint64, uint32, uint32, uint32) {
	b := om.bits
	m := om.mask
	i := uint32(keyB) & m
	l := &om.bucketLocks[i%om.bucketLockMask]
	ol := &om.overflowLock
	e := &om.buckets[i]
	l.RLock()
	for {
		if e.keyA == keyA && e.keyB == keyB {
			rt := e.timestamp
			rb := e.blockID
			ro := e.offset
			rl := e.length
			l.RUnlock()
			return rt, rb, ro, rl
		}
		if e.next == 0 {
			break
		}
		ol.RLock()
		e = &om.overflow[e.next>>b][e.next&m]
		ol.RUnlock()
	}
	l.RUnlock()
	return 0, 0, 0, 0
}

func (om *OverflowMap) Set(keyA uint64, keyB uint64, timestamp uint64, blockID uint32, offset uint32, length uint32, evenIfSameTimestamp bool) uint64 {
	b := om.bits
	m := om.mask
	i := uint32(keyB) & m
	l := &om.bucketLocks[i%om.bucketLockMask]
	ol := &om.overflowLock
	e := &om.buckets[i]
	var p *entry
	l.Lock()
	for {
		var n uint32
		if e.keyA == keyA && e.keyB == keyB {
			if e.timestamp < timestamp {
				if blockID != 0 {
					if e.blockID != 0 {
						t := e.timestamp
						e.timestamp = timestamp
						e.blockID = blockID
						e.offset = offset
						e.length = length
						l.Unlock()
						return t
					}
					e.timestamp = timestamp
					e.blockID = blockID
					e.offset = offset
					e.length = length
					atomic.AddUint32(&om.used, 1)
					l.Unlock()
					return 0
				}
				if e.blockID != 0 {
					t := e.timestamp
					e.timestamp = 0
					e.blockID = 0
					if p != nil {
						p.next = e.next
					}
					atomic.AddUint32(&om.used, ^uint32(0))
					if n != 0 {
						ol.Lock()
						if n < om.overflowLowestFree {
							om.overflowLowestFree = n
						}
						ol.Unlock()
					}
					l.Unlock()
					return t
				}
				l.Unlock()
				return 0
			} else if evenIfSameTimestamp && e.timestamp == timestamp {
				if blockID != 0 {
					if e.blockID != 0 {
						e.blockID = blockID
						e.offset = offset
						e.length = length
						l.Unlock()
						return timestamp
					}
					e.blockID = blockID
					e.offset = offset
					e.length = length
					atomic.AddUint32(&om.used, 1)
					l.Unlock()
					return timestamp
				} else if e.blockID != 0 {
					e.timestamp = 0
					e.blockID = 0
					atomic.AddUint32(&om.used, ^uint32(0))
					if n != 0 {
						ol.Lock()
						if n < om.overflowLowestFree {
							om.overflowLowestFree = n
						}
						ol.Unlock()
					}
					l.Unlock()
					return timestamp
				}
				l.Unlock()
				return timestamp
			}
			l.Unlock()
			return e.timestamp
		}
		if e.next == 0 {
			break
		}
		p = e
		n = e.next
		ol.RLock()
		e = &om.overflow[n>>b][n&m]
		ol.RUnlock()
	}
	if blockID != 0 {
		e = &om.buckets[i]
		if e.blockID != 0 {
			ol.Lock()
			o := om.overflow
			oc := uint32(len(o))
			if om.overflowLowestFree != 0 {
				nA := om.overflowLowestFree >> b
				nB := om.overflowLowestFree & m
				e = &o[nA][nB]
				e.next = om.buckets[i].next
				om.buckets[i].next = om.overflowLowestFree
				om.overflowLowestFree = 0
				for {
					if nB == m {
						nA++
						if nA == oc {
							break
						}
						nB = 0
					} else {
						nB++
					}
					if o[nA][nB].blockID == 0 {
						om.overflowLowestFree = nA<<b | nB
						break
					}
				}
			} else {
				om.overflow = append(om.overflow, make([]entry, 1<<b))
				if oc == 0 {
					e = &om.overflow[0][1]
					e.next = om.buckets[i].next
					om.buckets[i].next = 1
					om.overflowLowestFree = 2
				} else {
					e = &om.overflow[oc][0]
					e.next = om.buckets[i].next
					om.buckets[i].next = oc << b
					om.overflowLowestFree = oc<<b + 1
				}
			}
			ol.Unlock()
		}
		e.keyA = keyA
		e.keyB = keyB
		e.timestamp = timestamp
		e.blockID = blockID
		e.offset = offset
		e.length = length
		atomic.AddUint32(&om.used, 1)
	}
	l.Unlock()
	return 0
}

// Ensure nothing writes to the OverflowMap when calling RangeCount.
func (om *OverflowMap) RangeCount(start uint64, stop uint64) uint64 {
	var c uint64
	m := om.mask
	es := om.buckets
	for i := uint32(0); i <= m; i++ {
		e := &es[i]
		if e.blockID != 0 && e.keyA >= start && e.keyA <= stop {
			c++
		}
	}
	for _, es = range om.overflow {
		for i := uint32(0); i <= m; i++ {
			e := &es[i]
			if e.blockID != 0 && e.keyA >= start && e.keyA <= stop {
				c++
			}
		}
	}
	return c
}

// Ensure nothing writes to the OverflowMap when calling RangeCallback.
func (om *OverflowMap) RangeCallback(start uint64, stop uint64, callback func(keyA uint64, keyB uint64, timestamp uint64)) {
	m := om.mask
	es := om.buckets
	for i := uint32(0); i <= m; i++ {
		e := &es[i]
		if e.blockID != 0 && e.keyA >= start && e.keyA <= stop {
			callback(e.keyA, e.keyB, e.timestamp)
		}
	}
	for _, es = range om.overflow {
		for i := uint32(0); i <= m; i++ {
			e := &es[i]
			if e.blockID != 0 && e.keyA >= start && e.keyA <= stop {
				callback(e.keyA, e.keyB, e.timestamp)
			}
		}
	}
}

// Ensure nothing writes to the OverflowMap when calling Stats.
func (om *OverflowMap) Stats() (uint32, uint32, uint32, uint32, uint32, uint32, uint32) {
	var bu uint32
	var t uint32
	m := om.mask
	es := om.buckets
	for i := uint32(0); i <= m; i++ {
		e := &es[i]
		if e.blockID != 0 {
			bu++
			if e.timestamp&1 != 0 {
				t++
			}
		}
	}
	var ou uint32
	for _, es = range om.overflow {
		for i := uint32(0); i <= m; i++ {
			e := &es[i]
			if e.blockID != 0 {
				ou++
				if e.timestamp&1 != 0 {
					t++
				}
			}
		}
	}
	return 1 << om.bits, uint32(om.bucketLockMask + 1), uint32(len(om.overflow)), atomic.LoadUint32(&om.used), bu, ou, t
}

// Ensure nothing writes to the OverflowMap when calling String.
func (om *OverflowMap) String() string {
	bc, blc, oc, u, bu, ou, t := om.Stats()
	return fmt.Sprintf("OverflowMap %p: %d size, %d locks, %d overflow pages, %d total used, %d %.1f%% buckets used, %d %.1f%% overflow used, %.1f%% total used is overflowed, %d tombstones", om, bc, blc, oc, u, bu, 100*float64(bu)/float64(bc), ou, 100*float64(ou)/float64(oc*bc), 100*float64(ou)/float64(u), t)
}
