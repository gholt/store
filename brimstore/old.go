package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/gholt/brimstore"
	"github.com/gholt/brimutil"
)

func old() {
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
	totalValueBytes := totalValues * bytesPerValue
	value := make([]byte, bytesPerValue)
	brimutil.NewSeededScrambled(seed).Read(value)
	keys := createKeys(seed, clients, valuesPerClient)
	keys2 := createKeys(seed+int64(totalValues), clients, valuesPerClient)
	keys3 := createKeys(seed+int64(totalValues*2), clients, valuesPerClient)

	fmt.Println(cores, "cores")
	fmt.Println(bytesPerValue, "bytes per value")
	fmt.Println(clients, "clients")
	fmt.Println(valuesPerClient, "values per client")
	fmt.Printf("%d %0.2fm total values\n", totalValues, float64(totalValues)/1000000)
	fmt.Printf("%d %0.2fG total value bytes\n", totalValueBytes, float64(totalValueBytes)/1024/1024/1024)
	var st runtime.MemStats
	runtime.ReadMemStats(&st)
	lastAlloc := st.TotalAlloc
	fmt.Printf("%0.2fG total alloc\n", float64(st.TotalAlloc)/1024/1024/1024)

	fmt.Println()
	start := time.Now()
	vs := brimstore.NewValuesStore(nil)
	dur := time.Now().Sub(start)
	fmt.Println(dur, "to start ValuesStore")
	runtime.ReadMemStats(&st)
	deltaAlloc := st.TotalAlloc - lastAlloc
	lastAlloc = st.TotalAlloc
	fmt.Printf("%0.2fG total alloc, %0.2fG delta\n", float64(st.TotalAlloc)/1024/1024/1024, float64(deltaAlloc)/1024/1024/1024)

	fmt.Println()
	start = time.Now()
	writeValues(vs, keys, value, clients, valuesPerClient)
	dur = time.Now().Sub(start)
	fmt.Printf("%s %.0f/s %0.2fG/s to add %d values\n", dur, float64(totalValues)/(float64(dur)/float64(time.Second)), float64(totalValueBytes)/(float64(dur)/float64(time.Second))/1024/1024/1024, totalValues)
	runtime.ReadMemStats(&st)
	deltaAlloc = st.TotalAlloc - lastAlloc
	lastAlloc = st.TotalAlloc
	fmt.Printf("%0.2fG total alloc, %0.2fG delta\n", float64(st.TotalAlloc)/1024/1024/1024, float64(deltaAlloc)/1024/1024/1024)

	fmt.Println()
	start = time.Now()
	m := readValues(vs, [][][]byte{keys}, value, clients, valuesPerClient)
	dur = time.Now().Sub(start)
	fmt.Printf("%s %.0f/s %dns each, to read %d values\n", dur, float64(totalValues)/(float64(dur)/float64(time.Second)), int(dur)/totalValues, totalValues)
	if m != 0 {
		fmt.Println(m, "MISSING KEYS!")
	}
	runtime.ReadMemStats(&st)
	deltaAlloc = st.TotalAlloc - lastAlloc
	lastAlloc = st.TotalAlloc
	fmt.Printf("%0.2fG total alloc, %0.2fG delta\n", float64(st.TotalAlloc)/1024/1024/1024, float64(deltaAlloc)/1024/1024/1024)

	fmt.Println()
	start = time.Now()
	readChan, writeChan := readAndWriteValues(vs, keys, keys2, value, clients, valuesPerClient)
	readDone := false
	writeDone := false
	for !readDone || !writeDone {
		if readDone {
			<-writeChan
			dur = time.Now().Sub(start)
			fmt.Printf("%s %.0f/s %0.2fG/s to add %d values\n", dur, float64(totalValues)/(float64(dur)/float64(time.Second)), float64(totalValueBytes)/(float64(dur)/float64(time.Second))/1024/1024/1024, totalValues)
			writeDone = true
		} else if writeDone {
			m = <-readChan
			dur = time.Now().Sub(start)
			fmt.Printf("%s %.0f/s %dns each, to read %d values\n", dur, float64(totalValues)/(float64(dur)/float64(time.Second)), int(dur)/totalValues, totalValues)
			if m != 0 {
				fmt.Println(m, "MISSING KEYS!")
			}
			readDone = true
		} else {
			select {
			case m = <-readChan:
				dur = time.Now().Sub(start)
				fmt.Printf("%s %.0f/s %dns each, to read %d values while writing other values\n", dur, float64(totalValues)/(float64(dur)/float64(time.Second)), int(dur)/totalValues, totalValues)
				if m != 0 {
					fmt.Println(m, "MISSING KEYS!")
				}
				readDone = true
			case <-writeChan:
				dur = time.Now().Sub(start)
				fmt.Printf("%s %.0f/s %0.2fG/s to add %d values while reading other values\n", dur, float64(totalValues)/(float64(dur)/float64(time.Second)), float64(totalValueBytes)/(float64(dur)/float64(time.Second))/1024/1024/1024, totalValues)
				writeDone = true
			}
		}
	}
	runtime.ReadMemStats(&st)
	deltaAlloc = st.TotalAlloc - lastAlloc
	lastAlloc = st.TotalAlloc
	fmt.Printf("%0.2fG total alloc, %0.2fG delta\n", float64(st.TotalAlloc)/1024/1024/1024, float64(deltaAlloc)/1024/1024/1024)

	fmt.Println()
	start = time.Now()
	m = readValues(vs, [][][]byte{keys, keys2}, value, clients, valuesPerClient)
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
	m = readValues(vs, [][][]byte{keys3}, value, clients, valuesPerClient)
	dur = time.Now().Sub(start)
	fmt.Printf("%s %.0f/s %dns each, to read %d non-existent values\n", dur, float64(totalValues)/(float64(dur)/float64(time.Second)), int(dur)/(totalValues), totalValues)
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

func createKeys(seed int64, clients int, valuesPerClient int) [][]byte {
	wg := &sync.WaitGroup{}
	keys := make([][]byte, clients)
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		keys[i] = make([]byte, valuesPerClient*16)
		go func(i int) {
			brimutil.NewSeededScrambled(seed + int64(valuesPerClient*i)).Read(keys[i])
			wg.Done()
		}(i)
	}
	wg.Wait()
	return keys
}

func writeValues(vs *brimstore.ValuesStore, keys [][]byte, value []byte, clients int, valuesPerClient int) {
	wg := &sync.WaitGroup{}
	wg.Add(clients)
	seqstart := uint64(time.Now().UnixNano())
	for i := 0; i < clients; i++ {
		go func(keys []byte, seq uint64) {
			for o := 0; o < len(keys); o += 16 {
				seq++
				if err := vs.WriteValue(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), value, seq); err != nil {
					panic(err)
				}
			}
			wg.Done()
		}(keys[i], seqstart+uint64(i*valuesPerClient))
	}
	wg.Wait()
}

func readValues(vs *brimstore.ValuesStore, keys [][][]byte, value []byte, clients int, valuesPerClient int) int {
	c := make([]chan int, clients)
	for i := 0; i < clients; i++ {
		c[i] = make(chan int)
		go func(i int, c chan int) {
			v := make([]byte, 0, 128)
			m := 0
			for _, keysB := range keys {
				for o := 0; o < len(keysB[i]); o += 16 {
					v, _, err := vs.ReadValue(binary.BigEndian.Uint64(keysB[i][o:]), binary.BigEndian.Uint64(keysB[i][o+8:]), v[:0])
					if err == brimstore.ErrValueNotFound {
						m++
					} else if err != nil {
						panic(err)
					} else if !bytes.Equal(v, value) {
						panic(fmt.Sprintf("%#v != %#v", string(v), string(value)))
					}
				}
			}
			c <- m
		}(i, c[i])
	}
	m := 0
	for i := 0; i < clients; i++ {
		m += <-c[i]
	}
	return m
}

func readAndWriteValues(vs *brimstore.ValuesStore, keys [][]byte, keys2 [][]byte, value []byte, clients int, valuesPerClient int) (chan int, chan struct{}) {
	readChan := make(chan int, 1)
	writeChan := make(chan struct{}, 1)
	go func() {
		readChan <- readValues(vs, [][][]byte{keys}, value, clients, valuesPerClient)
	}()
	go func() {
		writeValues(vs, keys2, value, clients, valuesPerClient)
		writeChan <- struct{}{}
	}()
	return readChan, writeChan
}
