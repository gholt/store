package main

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/gholt/brimstore"
	"github.com/gholt/brimutil"
)

func main() {
	itemCount := uint64(781250)
	scratch := make([]byte, 24)
	scrambled := brimutil.NewScrambled()
	items := make([][]uint64, itemCount)
	for i := uint64(0); i < itemCount; i++ {
		scrambled.Read(scratch)
		items[i] = []uint64{
			binary.BigEndian.Uint64(scratch),
			binary.BigEndian.Uint64(scratch[8:]),
			binary.BigEndian.Uint64(scratch[16:]),
		}
	}

	var falses [][]uint64
	left := func() int {
		count := 0
		for i := 0; i < len(falses); i++ {
			if falses[i] != nil {
				count++
			}
		}
		return count
	}
	iterations := uint16(0)
	for falses == nil || left() > 0 {
		iterations++
		if falses != nil {
			fmt.Println()
			fmt.Println(left(), "false positives to resolve")
		}
		bf := brimstore.NewKTBloomFilter(itemCount, 0.01/(float64(itemCount)/100000), iterations)
		fmt.Println(bf)
		begin := time.Now()
		for i := uint64(0); i < itemCount; i++ {
			item := items[i]
			bf.Add(item[0], item[1], item[2]&0xfffffffffffffffe)
		}
		dur := time.Now().Sub(begin)
		fmt.Printf("%s to add %d items, %.02f/s\n", dur, itemCount, float64(itemCount)/(float64(dur)/float64(time.Second)))

		begin = time.Now()
		falseNegatives := 0
		for i := uint64(0); i < itemCount; i++ {
			item := items[i]
			if !bf.MayHave(item[0], item[1], item[2]&0xfffffffffffffffe) {
				falseNegatives++
			}
		}
		dur = time.Now().Sub(begin)
		fmt.Printf("%s to check for %d items that should exist, %.02f/s\n", dur, itemCount, float64(itemCount)/(float64(dur)/float64(time.Second)))
		fmt.Println(falseNegatives, "false negatives")

		begin = time.Now()
		falsePositives := 0
		if falses == nil {
			falses = make([][]uint64, 0)
			for i := uint64(0); i < itemCount; i++ {
				item := items[i]
				if bf.MayHave(item[0], item[1], item[2]|1) {
					falses = append(falses, item)
					falsePositives++
				}
			}
		} else {
			for i := uint64(0); i < itemCount; i++ {
				item := items[i]
				if bf.MayHave(item[0], item[1], item[2]|1) {
					falsePositives++
				} else {
					for j := 0; j < len(falses); j++ {
						if falses[j] != nil && item[0] == falses[j][0] && item[1] == falses[j][1] && item[2] == falses[j][2] {
							falses[j] = nil
						}
					}
				}
			}
		}
		dur = time.Now().Sub(begin)
		fmt.Printf("%s to check for %d items that should not exist, %.02f/s\n", dur, itemCount, float64(itemCount)/(float64(dur)/float64(time.Second)))
		fmt.Printf("%d false positives, %.02f%%\n", falsePositives, float64(falsePositives)/float64(itemCount)*100)
	}
	fmt.Println()
	fmt.Println(iterations, "iterations to resolve all false positives")
}
