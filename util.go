package brimstore

func PowerOfTwoNeeded(v uint64) uint64 {
	var p uint64 = 1
	for 1<<p < v {
		p++
	}
	return p
}
