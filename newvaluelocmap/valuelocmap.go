package newvaluemap

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

type node struct {
	bits               uint32
	mask               uint32
	entries            []entry
	entriesLockMask    uint32
	entriesLocks       []sync.RWMutex
	overflow           [][]entry
	overflowLowestFree uint32
	overflowLock       sync.RWMutex
	used               uint32
}

func newNode(bits uint32, lockBits uint32) *node {
	// Needed so the overflow logic can be simpler.
	if bits < 2 {
		bits = 2
	}
	if lockBits < 1 {
		lockBits = 1
	}
	return &node{
		bits:            bits,
		mask:            (1 << bits) - 1,
		entries:         make([]entry, 1<<bits),
		entriesLockMask: (1 << lockBits) - 1,
		entriesLocks:    make([]sync.RWMutex, 1<<lockBits),
	}
}

// Ensure nothing writes to the node when calling split.
func (n *node) split(highMask uint64) *node {
	b := n.bits
	m := n.mask
	nn := &node{
		bits:            b,
		mask:            m,
		entries:         make([]entry, len(n.entries)),
		entriesLockMask: n.entriesLockMask,
		entriesLocks:    make([]sync.RWMutex, len(n.entriesLocks)),
	}
	o := n.overflow
	nes := nn.entries
	noc := uint32(0)
	// Move over all matching overflow entries that have an overflow entry
	// pointing to them.
	c := true
	for c {
		c = false
		for _, es := range n.overflow {
			for i := uint32(0); i <= m; i++ {
				e := &es[i]
				if e.blockID != 0 && e.next != 0 {
					en := &n.overflow[e.next>>b][e.next&m]
					if en.keyA&highMask != 0 {
						ne := &nes[uint32(en.keyB)&m]
						if ne.blockID == 0 {
							*ne = *en
							ne.next = 0
						} else {
							if nn.overflowLowestFree != 0 {
								ne2 := &nn.overflow[nn.overflowLowestFree>>b][nn.overflowLowestFree&m]
								*ne2 = *en
								ne2.next = ne.next
								ne.next = nn.overflowLowestFree
								nn.overflowLowestFree++
								if nn.overflowLowestFree&m == 0 {
									nn.overflowLowestFree = 0
								}
							} else {
								nn.overflow = append(nn.overflow, make([]entry, 1<<b))
								if noc == 0 {
									ne2 := &nn.overflow[0][1]
									*ne2 = *en
									ne2.next = ne.next
									ne.next = 1
									nn.overflowLowestFree = 2
								} else {
									ne2 := &nn.overflow[noc][0]
									*ne2 = *en
									ne2.next = ne.next
									ne.next = noc << b
									nn.overflowLowestFree = noc<<b + 1
								}
								noc++
							}
						}
						nn.used++
						if e.next < n.overflowLowestFree {
							n.overflowLowestFree = e.next
						}
						e.next = en.next
						en.blockID = 0
						n.used--
						c = true
					}
				}
			}
		}
	}
	// Now any matching overflow entries left are pointed to by their
	// respective non-overflow entry. Move those.
	es := n.entries
	for i := uint32(0); i <= m; i++ {
		e := &es[i]
		if e.blockID != 0 && e.next != 0 {
			en := &n.overflow[e.next>>b][e.next&m]
			if en.keyA&highMask != 0 {
				ne := &nes[i]
				if ne.blockID == 0 {
					*ne = *en
					ne.next = 0
				} else {
					if nn.overflowLowestFree != 0 {
						ne2 := &nn.overflow[nn.overflowLowestFree>>b][nn.overflowLowestFree&m]
						*ne2 = *en
						ne2.next = ne.next
						ne.next = nn.overflowLowestFree
						nn.overflowLowestFree++
						if nn.overflowLowestFree&m == 0 {
							nn.overflowLowestFree = 0
						}
					} else {
						nn.overflow = append(nn.overflow, make([]entry, 1<<b))
						if noc == 0 {
							ne2 := &nn.overflow[0][1]
							*ne2 = *en
							ne2.next = ne.next
							ne.next = 1
							nn.overflowLowestFree = 2
						} else {
							ne2 := &nn.overflow[noc][0]
							*ne2 = *en
							ne2.next = ne.next
							ne.next = noc << b
							nn.overflowLowestFree = noc<<b + 1
						}
						noc++
					}
				}
				nn.used++
				if e.next < n.overflowLowestFree {
					n.overflowLowestFree = e.next
				}
				e.next = en.next
				en.blockID = 0
				n.used--
			}
		}
	}
	// Now any matching entries left are non-overflow entries. Move those.
	for i := uint32(0); i <= m; i++ {
		e := &es[i]
		if e.blockID != 0 && e.keyA&highMask != 0 {
			ne := &nes[i]
			if ne.blockID == 0 {
				*ne = *e
				ne.next = 0
			} else {
				if nn.overflowLowestFree != 0 {
					ne2 := &nn.overflow[nn.overflowLowestFree>>b][nn.overflowLowestFree&m]
					*ne2 = *e
					ne2.next = ne.next
					ne.next = nn.overflowLowestFree
					nn.overflowLowestFree++
					if nn.overflowLowestFree&m == 0 {
						nn.overflowLowestFree = 0
					}
				} else {
					nn.overflow = append(nn.overflow, make([]entry, 1<<b))
					if noc == 0 {
						ne2 := &nn.overflow[0][1]
						*ne2 = *e
						ne2.next = ne.next
						ne.next = 1
						nn.overflowLowestFree = 2
					} else {
						ne2 := &nn.overflow[noc][0]
						*ne2 = *e
						ne2.next = ne.next
						ne.next = noc << b
						nn.overflowLowestFree = noc<<b + 1
					}
					noc++
				}
			}
			nn.used++
			if e.next == 0 {
				e.blockID = 0
				e.timestamp = 0
			} else {
				if e.next < n.overflowLowestFree {
					n.overflowLowestFree = e.next
				}
				x := &o[e.next>>b][e.next&m]
				*e = *x
				x.blockID = 0
				x.next = 0
			}
			n.used--
		}
	}
	return nn
}

func (n *node) merge(on *node) {
	b := n.bits
	m := n.mask
	es := n.entries
	o := n.overflow
	oc := uint32(len(o))
	// Move over all overflow entries that have an overflow entry pointing to
	// them.
	c := true
	for c {
		c = false
		for _, oes := range on.overflow {
			for i := uint32(0); i <= m; i++ {
				oe := &oes[i]
				if oe.blockID != 0 && oe.next != 0 {
					oen := &on.overflow[oe.next>>b][oe.next&m]
					e := &es[uint32(oen.keyB)&m]
					if e.blockID == 0 {
						*e = *oen
						e.next = 0
					} else {
						if n.overflowLowestFree != 0 {
							nA := n.overflowLowestFree >> b
							nB := n.overflowLowestFree & m
							e2 := &o[nA][nB]
							*e2 = *oen
							e2.next = e.next
							e.next = n.overflowLowestFree
							n.overflowLowestFree = 0
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
									n.overflowLowestFree = nA<<b | nB
									break
								}
							}
						} else {
							o = append(o, make([]entry, 1<<b))
							n.overflow = o
							if oc == 0 {
								e2 := &o[0][1]
								*e2 = *oen
								e2.next = e.next
								e.next = 1
								n.overflowLowestFree = 2
							} else {
								e2 := &o[oc][0]
								*e2 = *oen
								e2.next = e.next
								e.next = oc << b
								n.overflowLowestFree = oc<<b + 1
							}
							oc++
						}
					}
					n.used++
					oe.next = oen.next
					oen.blockID = 0
					c = true
				}
			}
		}
	}
	// Now we just have overflow entries that are pointed to by their
	// respective non-overflow entries. Move those.
	oes := on.entries
	oo := on.overflow
	for i := uint32(0); i <= m; i++ {
		oe := &oes[i]
		if oe.blockID != 0 && oe.next != 0 {
			oen := &oo[oe.next>>b][oe.next&m]
			e := &es[i]
			if e.blockID == 0 {
				*e = *oen
				e.next = 0
			} else {
				if n.overflowLowestFree != 0 {
					nA := n.overflowLowestFree >> b
					nB := n.overflowLowestFree & m
					e2 := &o[nA][nB]
					*e2 = *oen
					e2.next = e.next
					e.next = n.overflowLowestFree
					n.overflowLowestFree = 0
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
							n.overflowLowestFree = nA<<b | nB
							break
						}
					}
				} else {
					o = append(o, make([]entry, 1<<b))
					n.overflow = o
					if oc == 0 {
						e2 := &o[0][1]
						*e2 = *oen
						e2.next = e.next
						e.next = 1
						n.overflowLowestFree = 2
					} else {
						e2 := &o[oc][0]
						*e2 = *oen
						e2.next = e.next
						e.next = oc << b
						n.overflowLowestFree = oc<<b + 1
					}
					oc++
				}
			}
			n.used++
			oen.blockID = 0
		}
	}
	// Now we just have the non-overflow entries. Move those.
	for i := uint32(0); i <= m; i++ {
		oe := &oes[i]
		if oe.blockID != 0 {
			e := &es[i]
			if e.blockID == 0 {
				*e = *oe
				e.next = 0
			} else {
				if n.overflowLowestFree != 0 {
					nA := n.overflowLowestFree >> b
					nB := n.overflowLowestFree & m
					e2 := &o[nA][nB]
					*e2 = *oe
					e2.next = e.next
					e.next = n.overflowLowestFree
					n.overflowLowestFree = 0
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
							n.overflowLowestFree = nA<<b | nB
							break
						}
					}
				} else {
					o = append(o, make([]entry, 1<<b))
					n.overflow = o
					if oc == 0 {
						e2 := &o[0][1]
						*e2 = *oe
						e2.next = e.next
						e.next = 1
						n.overflowLowestFree = 2
					} else {
						e2 := &o[oc][0]
						*e2 = *oe
						e2.next = e.next
						e.next = oc << b
						n.overflowLowestFree = oc<<b + 1
					}
					oc++
				}
			}
			n.used++
			oe.blockID = 0
		}
	}
	on.used = 0
}

func (n *node) get(keyA uint64, keyB uint64) (uint64, uint32, uint32, uint32) {
	b := n.bits
	m := n.mask
	i := uint32(keyB) & m
	l := &n.entriesLocks[i%n.entriesLockMask]
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
			return rt, rb, ro, rl
		}
		if e.next == 0 {
			break
		}
		ol.RLock()
		e = &n.overflow[e.next>>b][e.next&m]
		ol.RUnlock()
	}
	l.RUnlock()
	return 0, 0, 0, 0
}

func (n *node) set(keyA uint64, keyB uint64, timestamp uint64, blockID uint32, offset uint32, length uint32, evenIfSameTimestamp bool) uint64 {
	b := n.bits
	m := n.mask
	i := uint32(keyB) & m
	l := &n.entriesLocks[i%n.entriesLockMask]
	ol := &n.overflowLock
	e := &n.entries[i]
	var p *entry
	l.Lock()
	for {
		var f uint32
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
					atomic.AddUint32(&n.used, 1)
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
					atomic.AddUint32(&n.used, ^uint32(0))
					if f != 0 {
						ol.Lock()
						if f < n.overflowLowestFree {
							n.overflowLowestFree = f
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
					atomic.AddUint32(&n.used, 1)
					l.Unlock()
					return timestamp
				} else if e.blockID != 0 {
					e.timestamp = 0
					e.blockID = 0
					atomic.AddUint32(&n.used, ^uint32(0))
					if f != 0 {
						ol.Lock()
						if f < n.overflowLowestFree {
							n.overflowLowestFree = f
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
		f = e.next
		ol.RLock()
		e = &n.overflow[f>>b][f&m]
		ol.RUnlock()
	}
	if blockID != 0 {
		e = &n.entries[i]
		if e.blockID != 0 {
			ol.Lock()
			o := n.overflow
			oc := uint32(len(o))
			if n.overflowLowestFree != 0 {
				nA := n.overflowLowestFree >> b
				nB := n.overflowLowestFree & m
				e = &o[nA][nB]
				e.next = n.entries[i].next
				n.entries[i].next = n.overflowLowestFree
				n.overflowLowestFree = 0
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
						n.overflowLowestFree = nA<<b | nB
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
		e.blockID = blockID
		e.offset = offset
		e.length = length
		atomic.AddUint32(&n.used, 1)
	}
	l.Unlock()
	return 0
}

// Ensure nothing writes to the node when calling RangeCount.
func (n *node) rangeCount(start uint64, stop uint64) uint64 {
	var c uint64
	m := n.mask
	es := n.entries
	for i := uint32(0); i <= m; i++ {
		e := &es[i]
		if e.blockID != 0 && e.keyA >= start && e.keyA <= stop {
			c++
		}
	}
	for _, es = range n.overflow {
		for i := uint32(0); i <= m; i++ {
			e := &es[i]
			if e.blockID != 0 && e.keyA >= start && e.keyA <= stop {
				c++
			}
		}
	}
	return c
}

// Ensure nothing writes to the node when calling RangeCallback.
func (n *node) rangeCallback(start uint64, stop uint64, callback func(keyA uint64, keyB uint64, timestamp uint64)) {
	m := n.mask
	es := n.entries
	for i := uint32(0); i <= m; i++ {
		e := &es[i]
		if e.blockID != 0 && e.keyA >= start && e.keyA <= stop {
			callback(e.keyA, e.keyB, e.timestamp)
		}
	}
	for _, es = range n.overflow {
		for i := uint32(0); i <= m; i++ {
			e := &es[i]
			if e.blockID != 0 && e.keyA >= start && e.keyA <= stop {
				callback(e.keyA, e.keyB, e.timestamp)
			}
		}
	}
}

// Ensure nothing writes to the node when calling Stats.
func (n *node) stats() (uint32, uint32, uint32, uint32, uint32, uint32, uint32) {
	var bu uint32
	var t uint32
	m := n.mask
	es := n.entries
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
	for _, es = range n.overflow {
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
	return 1 << n.bits, uint32(n.entriesLockMask + 1), uint32(len(n.overflow)), atomic.LoadUint32(&n.used), bu, ou, t
}

// Ensure nothing writes to the node when calling String.
func (n *node) String() string {
	bc, blc, oc, u, bu, ou, t := n.stats()
	return fmt.Sprintf("node %p: %d size, %d locks, %d overflow pages, %d total used, %d %.1f%% entries used, %d %.1f%% overflow used, %.1f%% total used is overflowed, %d tombstones", n, bc, blc, oc, u, bu, 100*float64(bu)/float64(bc), ou, 100*float64(ou)/float64(oc*bc), 100*float64(ou)/float64(u), t)
}
