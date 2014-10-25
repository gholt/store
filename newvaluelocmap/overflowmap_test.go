package main

import (
	"testing"
)

func TestOverflowMap(t *testing.T) {
	om := NewOverflowMap(1, 1)
	ts := uint64(100)
	b := uint32(200)
	o := uint32(300)
	l := uint32(400)
	for keyA := uint64(0); keyA < 100; keyA++ {
		for keyB := uint64(0); keyB < 100; keyB++ {
			om.Set(keyA, keyB, ts, b, o, l, false)
			ts++
			b++
			o++
			l++
		}
	}
	ts = uint64(100)
	b = uint32(200)
	o = uint32(300)
	l = uint32(400)
	for keyA := uint64(0); keyA < 100; keyA++ {
		for keyB := uint64(0); keyB < 100; keyB++ {
			ts2, b2, o2, l2 := om.Get(keyA, keyB)
			if ts2 != ts || b2 != b || o2 != o || l2 != l {
				t.Fatalf("For keyA == %d, keyB == %d, Get (%d, %d, %d, %d) did not match Set (%d, %d, %d, %d)", keyA, keyB, ts2, b2, o2, l2, ts, b, o, l)
			}
			ts++
			b++
			o++
			l++
		}
	}
}
