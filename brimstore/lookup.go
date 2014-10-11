package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/gholt/brimstore"
)

// lookup n s	Looks up n random values seeded with s.
func lookup() {
	if len(os.Args) != 4 {
		fmt.Println(`
lookup n s	Looks up n random values seeded with s.`)
		os.Exit(1)
	}
	values, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	seed, err := strconv.Atoi(os.Args[3])
	if err != nil {
		panic(err)
	}
	cores := runtime.GOMAXPROCS(0)
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
		cores = runtime.GOMAXPROCS(0)
	}
	clients := cores * cores
	keys := createKeys(seed, clients, values)
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
	start = time.Now()
	lookupValues(vs, keys, clients)
	dur = time.Now().Sub(start)
	fmt.Printf("%s %.0f/s to lookup %d values\n", dur, float64(values)/(float64(dur)/float64(time.Second)), values)
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

	fmt.Println()
	stats := vs.GatherStats(true)
	dur = time.Now().Sub(start)
	fmt.Println(dur, "to gather stats")
	fmt.Println(stats.ValueCount(), "ValueCount")
	fmt.Println(stats.ValuesLength(), "ValuesLength")
	fmt.Println(stats.String())
	runtime.ReadMemStats(&st)
	deltaAlloc = st.TotalAlloc - lastAlloc
	lastAlloc = st.TotalAlloc
	fmt.Printf("%0.2fG total alloc, %0.2fG delta\n", float64(st.TotalAlloc)/1024/1024/1024, float64(deltaAlloc)/1024/1024/1024)
}
