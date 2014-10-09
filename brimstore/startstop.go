package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/gholt/brimstore"
)

func startstop() {
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
	var st runtime.MemStats
	runtime.ReadMemStats(&st)
	lastAlloc := st.TotalAlloc
	fmt.Printf("%0.2fG total alloc\n", float64(st.TotalAlloc)/1024/1024/1024)
	start := time.Now()

	fmt.Println()
	vs := brimstore.NewValuesStore(nil)
	dur := time.Now().Sub(start)
	fmt.Println(dur, "to start ValuesStore")
	runtime.ReadMemStats(&st)
	deltaAlloc := st.TotalAlloc - lastAlloc
	lastAlloc = st.TotalAlloc
	fmt.Printf("%0.2fG total alloc, %0.2fG delta\n", float64(st.TotalAlloc)/1024/1024/1024, float64(deltaAlloc)/1024/1024/1024)

	fmt.Println()
	stats := vs.GatherStats()
	dur = time.Now().Sub(start)
	fmt.Println(dur, "to gather stats")
	fmt.Println(stats.ValueCount, "ValueCount")
	fmt.Println(stats.ValuesLength, "ValuesLength")
	runtime.ReadMemStats(&st)
	deltaAlloc = st.TotalAlloc - lastAlloc
	lastAlloc = st.TotalAlloc
	fmt.Printf("%0.2fG total alloc, %0.2fG delta\n", float64(st.TotalAlloc)/1024/1024/1024, float64(deltaAlloc)/1024/1024/1024)

	fmt.Println()
	start = time.Now()
	vs.Close()
	dur = time.Now().Sub(start)
	fmt.Println(dur, "to close ValuesStore")
	runtime.ReadMemStats(&st)
	deltaAlloc = st.TotalAlloc - lastAlloc
	lastAlloc = st.TotalAlloc
	fmt.Printf("%0.2fG total alloc, %0.2fG delta\n", float64(st.TotalAlloc)/1024/1024/1024, float64(deltaAlloc)/1024/1024/1024)
}
