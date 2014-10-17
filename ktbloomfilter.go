package brimstore

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/spaolacci/murmur3"
)

type KTBloomFilter struct {
	m       uint32
	kDiv4   uint32
	bits    []byte
	salt    uint32
	scratch []byte
}

func NewKTBloomFilter(n uint64, p float64, salt uint16) *KTBloomFilter {
	m := -((float64(n) * math.Log(p)) / math.Pow(math.Log(2), 2))
	return &KTBloomFilter{
		m:       uint32(math.Ceil(m/8)) * 8,
		kDiv4:   uint32(math.Ceil(m / float64(n) * math.Log(2) / 4)),
		bits:    make([]byte, uint32(math.Ceil(m/8))),
		salt:    uint32(salt) << 16,
		scratch: make([]byte, 28),
	}
}

func (ktbf *KTBloomFilter) String() string {
	return fmt.Sprintf("KTBloomFilter %p m=%d k=%d bytes=%d salt:%d", ktbf, ktbf.m, ktbf.kDiv4*4, len(ktbf.bits), ktbf.salt>>16)
}

func (ktbf *KTBloomFilter) Add(keyA uint64, keyB uint64, timestamp uint64) {
	scratch := ktbf.scratch
	binary.BigEndian.PutUint64(scratch[4:], keyA)
	binary.BigEndian.PutUint64(scratch[12:], keyB)
	binary.BigEndian.PutUint64(scratch[20:], timestamp)
	for i := ktbf.kDiv4; i > 0; i-- {
		binary.BigEndian.PutUint32(scratch, ktbf.salt|i)
		h1, h2 := murmur3.Sum128(scratch)
		bit := uint32(h1>>32) % ktbf.m
		ktbf.bits[bit/8] |= 1 << (bit % 8)
		bit = uint32(h1&0xffffffff) % ktbf.m
		ktbf.bits[bit/8] |= 1 << (bit % 8)
		bit = uint32(h2>>32) % ktbf.m
		ktbf.bits[bit/8] |= 1 << (bit % 8)
		bit = uint32(h2&0xffffffff) % ktbf.m
		ktbf.bits[bit/8] |= 1 << (bit % 8)
	}
}

func (ktbf *KTBloomFilter) MayHave(keyA uint64, keyB uint64, timestamp uint64) bool {
	scratch := ktbf.scratch
	binary.BigEndian.PutUint64(scratch[4:], keyA)
	binary.BigEndian.PutUint64(scratch[12:], keyB)
	binary.BigEndian.PutUint64(scratch[20:], timestamp)
	for i := ktbf.kDiv4; i > 0; i-- {
		binary.BigEndian.PutUint32(scratch, ktbf.salt|i)
		h1, h2 := murmur3.Sum128(scratch)
		bit := uint32(h1>>32) % ktbf.m
		if ktbf.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
		bit = uint32(h1&0xffffffff) % ktbf.m
		if ktbf.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
		bit = uint32(h2>>32) % ktbf.m
		if ktbf.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
		bit = uint32(h2&0xffffffff) % ktbf.m
		if ktbf.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}
