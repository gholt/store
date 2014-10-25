package main

import (
	"encoding/binary"
	"fmt"
	"github.com/gholt/brimutil"
	"github.com/spaolacci/murmur3"
	"runtime"
	"time"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	s := brimutil.NewSeededScrambled(1)
	k := make([]byte, 16)
	t := uint64(time.Now().UnixNano())
	p := uint32(21)
	om := NewOverflowMap(p, 6)
	count := uint32(2.3 * float64(uint32(1<<p)))
	begin := time.Now()
	for i := uint32(0); i < count; i++ {
		s.Read(k)
		a, b := murmur3.Sum128(k)
		om.Set(a, b, t+uint64(binary.BigEndian.Uint32(k)), 1, i, i, false)
	}
	dur := time.Now().Sub(begin)
	fmt.Printf("%s %f/s %fns each, setting %d entries\n", dur, float64(count)/(float64(dur)/float64(time.Second)), float64(dur)/float64(count), count)
	fmt.Println(om)
	nom := om.Move(0x8000000000000000)
	fmt.Println(om)
	fmt.Println(nom)
	fmt.Println(om.RangeCount(0, 0x7fffffffffffffff))
	fmt.Println(nom.RangeCount(0, 0x7fffffffffffffff))
	fmt.Println(om.RangeCount(0x8000000000000000, 0xffffffffffffffff))
	fmt.Println(nom.RangeCount(0x8000000000000000, 0xffffffffffffffff))
	dur = time.Duration(0)
	for it := 0; it < 100; it++ {
		s = brimutil.NewSeededScrambled(1)
		om = NewOverflowMap(p, 6)
		for i := uint32(0); i < count; i++ {
			s.Read(k)
			a, b := murmur3.Sum128(k)
			om.Set(a, b, t+uint64(binary.BigEndian.Uint32(k)), 1, i, i, false)
		}
		begin = time.Now()
		nom = om.Move(0x8000000000000000)
		dur += time.Now().Sub(begin)
	}
	count *= 100
	fmt.Printf("%s %f/s %fns each, splitting %d entries\n", dur, float64(count)/(float64(dur)/float64(time.Second)), float64(dur)/float64(count), count)
	fmt.Println(om)
	fmt.Println(nom)
	fmt.Println(om.RangeCount(0, 0x7fffffffffffffff))
	fmt.Println(nom.RangeCount(0, 0x7fffffffffffffff))
	fmt.Println(om.RangeCount(0x8000000000000000, 0xffffffffffffffff))
	fmt.Println(nom.RangeCount(0x8000000000000000, 0xffffffffffffffff))
	//s = brimutil.NewSeededScrambled(1)
	//begin = time.Now()
	//for i := uint32(0); i < count; i++ {
	//	s.Read(k)
	//    a, b := murmur3.Sum128(k)
	//	om.Get(a, b)
	//}
	//dur = time.Now().Sub(begin)
	//fmt.Printf("%s %f/s %fns each, getting %d entries\n", dur, float64(count)/(float64(dur)/float64(time.Second)), float64(dur)/float64(count), count)
	//count = 1000
	//var c uint64
	//begin = time.Now()
	//for i:=uint32(0);i<count;i++ {
	//    c =om.RangeCount(0, 0x7fffffffffffffff)
	//}
	//dur = time.Now().Sub(begin)
	//fmt.Printf("%s %f/s %fns each, %d range counts [%d]\n", dur, float64(count)/(float64(dur)/float64(time.Second)), float64(dur)/float64(count), count, c)
	//f := func(keyA uint64, keyB uint64, timestamp uint64) {
	//	c++
	//}
	//count = 1000
	//begin = time.Now()
	//for i:=uint32(0);i<count;i++ {
	//    c = 0
	//    om.RangeCallback(0, 0x7fffffffffffffff, f)
	//}
	//dur = time.Now().Sub(begin)
	//fmt.Printf("%s %f/s %fns each, %d range callbacks [%d]\n", dur, float64(count)/(float64(dur)/float64(time.Second)), float64(dur)/float64(count), count, c)
	//runtime.GC()
	//time.Sleep(10)
}
