package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/gholt/brimstore"
	"github.com/gholt/brimutil"
)

// add n s q z	Adds n random values seeded with s using sequence number q and
// 		the values will be z in length.
func add() {
	if len(os.Args) != 6 {
		fmt.Println(`
add n s q z	Adds n random values seeded with s using sequence number q and
		the values will be z in length.`)
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
	seq, err := strconv.Atoi(os.Args[4])
	if err != nil {
		panic(err)
	}
	length, err := strconv.Atoi(os.Args[5])
	if err != nil {
		panic(err)
	}
	cores := runtime.GOMAXPROCS(0)
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
		cores = runtime.GOMAXPROCS(0)
	}
	clients := cores * cores
	if clients < 50 {
		clients = 50
	}
	keys := createKeys(seed, clients, values)
	value := make([]byte, length)
	brimutil.NewSeededScrambled(int64(seed)).Read(value)
	if len(value) > 10 {
		copy(value, []byte("START67890"))
	}
	if len(value) > 20 {
		copy(value[len(value)-10:], []byte("123456STOP"))
	}
	valuesLength := values * length
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
	writeValues(vs, keys, seq, value, clients)
	dur = time.Now().Sub(start)
	fmt.Printf("%s %.0f/s %0.2fG/s to add %d values\n", dur, float64(values)/(float64(dur)/float64(time.Second)), float64(valuesLength)/(float64(dur)/float64(time.Second))/1024/1024/1024, values)
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
