package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/gholt/brimstore"
	"github.com/gholt/brimutil"
)

func startstop() {
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

	seed := int64(1)
	bytesPerValue := 128
	targetBytes := 4 * 1024 * 1024 * 1024
	cores := runtime.GOMAXPROCS(0)
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
		cores = runtime.GOMAXPROCS(0)
	}
	clients := cores * cores
	valuesPerClient := targetBytes / bytesPerValue / clients
	totalValues := clients * valuesPerClient
	value := make([]byte, bytesPerValue)
	brimutil.NewSeededScrambled(seed).Read(value)
	keys := createKeys(seed, clients, valuesPerClient)
	keys2 := createKeys(seed+int64(totalValues), clients, valuesPerClient)

	fmt.Println()
	start = time.Now()
	m := readValues(vs, [][][]byte{keys, keys2}, value, clients, valuesPerClient)
	dur = time.Now().Sub(start)
	fmt.Printf("%s %.0f/s %dns each, to read %d values\n", dur, float64(totalValues*2)/(float64(dur)/float64(time.Second)), int(dur)/(totalValues*2), totalValues*2)
	if m != 0 {
		fmt.Println(m, "MISSING KEYS!")
	}
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
