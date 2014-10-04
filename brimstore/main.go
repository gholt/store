package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/gholt/brimstore/valuesstore"
	"github.com/gholt/brimutil"
)

func main() {
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

	fmt.Println(cores, "cores")
	fmt.Println(bytesPerValue, "bytes per value")
	fmt.Println(clients, "clients")
	fmt.Println(valuesPerClient, "values per client")
	fmt.Printf("%d %0.2fm total values\n", totalValues, float64(totalValues)/1000000)
	fmt.Printf("%d %0.2fG total value bytes\n", totalValueBytes, float64(totalValueBytes)/1024/1024/1024)

	value := make([]byte, bytesPerValue)
	brimutil.NewSeededScrambled(seed).Read(value)
	keys := createKeys(seed, clients, valuesPerClient)

	start := time.Now()
	vs := valuesstore.NewValuesStore(nil)
	dur := time.Now().Sub(start)
	fmt.Println(dur, "to start ValuesStore")

	start = time.Now()
	writeValues(vs, keys, value, clients, valuesPerClient)
	dur = time.Now().Sub(start)
	fmt.Printf("%s %.0f/s %0.2fG/s to add %d values\n", dur, float64(totalValues)/float64(dur)/float64(time.Second), float64(totalValueBytes)/float64(dur)/float64(time.Second)/1024/1024/1024, totalValues)
	var st runtime.MemStats
	runtime.ReadMemStats(&st)
	fmt.Printf("%0.2fG total alloc\n", float64(st.TotalAlloc)/1024/1024/1024)

	fmt.Println()
	start = time.Now()
	m := readValues(vs, [][][]byte{keys}, value, clients, valuesPerClient)
	dur = time.Now().Sub(start)
	fmt.Printf("%s %.0f/s %dns each, to read %d values\n", dur, float64(totalValues)/float64(dur)/float64(time.Second), int(dur)/totalValues, totalValues)
	if m != 0 {
		fmt.Println(m, "MISSING KEYS!")
	}

	fmt.Println()
	keys2 := createKeys(seed+int64(totalValues), clients, valuesPerClient)
	start = time.Now()
	readChan, writeChan := readAndWriteValues(vs, keys, keys2, value, clients, valuesPerClient)
	readDone := false
	writeDone := false
	for !readDone || !writeDone {
		if readDone {
			<-writeChan
			dur = time.Now().Sub(start)
			fmt.Printf("%s %.0f/s %0.2fG/s to add %d values\n", dur, float64(totalValues)/float64(dur)/float64(time.Second), float64(totalValueBytes)/float64(dur)/float64(time.Second)/1024/1024/1024, totalValues)
			writeDone = true
		} else if writeDone {
			m = <-readChan
			dur = time.Now().Sub(start)
			fmt.Printf("%s %.0f/s %dns each, to read %d values\n", dur, float64(totalValues)/float64(dur)/float64(time.Second), int(dur)/totalValues, totalValues)
			if m != 0 {
				fmt.Println(m, "MISSING KEYS!")
			}
			readDone = true
		} else {
			select {
			case m = <-readChan:
				dur = time.Now().Sub(start)
				fmt.Printf("%s %.0f/s %dns each, to read %d values while writing other values\n", dur, float64(totalValues)/float64(dur)/float64(time.Second), int(dur)/totalValues, totalValues)
				if m != 0 {
					fmt.Println(m, "MISSING KEYS!")
				}
				readDone = true
			case <-writeChan:
				dur = time.Now().Sub(start)
				fmt.Printf("%s %.0f/s %0.2fG/s to add %d values while reading other values\n", dur, float64(totalValues)/float64(dur)/float64(time.Second), float64(totalValueBytes)/float64(dur)/float64(time.Second)/1024/1024/1024, totalValues)
				writeDone = true
			}
		}
	}
	runtime.ReadMemStats(&st)
	fmt.Printf("%0.2fG total alloc\n", float64(st.TotalAlloc)/1024/1024/1024)

	fmt.Println()
	start = time.Now()
	m = readValues(vs, [][][]byte{keys, keys2}, value, clients, valuesPerClient)
	dur = time.Now().Sub(start)
	fmt.Printf("%s %.0f/s %dns each, to read %d values\n", dur, float64(totalValues*2)/float64(dur)/float64(time.Second), int(dur)/(totalValues*2), totalValues*2)
	if m != 0 {
		fmt.Println(m, "MISSING KEYS!")
	}

	start = time.Now()
	vs.Close()
	dur = time.Now().Sub(start)
	fmt.Println(dur, "to close ValuesStore")
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

func writeValues(vs *valuesstore.ValuesStore, keys [][]byte, value []byte, clients int, valuesPerClient int) {
	wg := &sync.WaitGroup{}
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func(keys []byte, seq uint64) {
			var err error
			w := &valuesstore.WriteValue{
				Value:       value,
				WrittenChan: make(chan error, 1),
				Seq:         seq,
			}
			for o := 0; o < len(keys); o += 16 {
				w.KeyA = binary.BigEndian.Uint64(keys[o:])
				w.KeyB = binary.BigEndian.Uint64(keys[o+8:])
				w.Seq++
				vs.WriteValue(w)
				err = <-w.WrittenChan
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}(keys[i], uint64(i*valuesPerClient))
	}
	wg.Wait()
}

func readValues(vs *valuesstore.ValuesStore, keys [][][]byte, value []byte, clients int, valuesPerClient int) int {
	c := make([]chan int, clients)
	for i := 0; i < clients; i++ {
		c[i] = make(chan int)
		go func(keys [][]byte, c chan int) {
			r := vs.NewReadValue()
			m := 0
			for _, keysB := range keys {
				for o := 0; o < len(keysB); o += 16 {
					r.KeyA = binary.BigEndian.Uint64(keysB[o:])
					r.KeyB = binary.BigEndian.Uint64(keysB[o+8:])
					vs.ReadValue(r)
					err := <-r.ReadChan
					if err == valuesstore.ErrValueNotFound {
						m++
					} else if err != nil {
						panic(err)
					} else if !bytes.Equal(r.Value, value) {
						panic(fmt.Sprintf("%#v != %#v", string(r.Value), string(value)))
					}
				}
			}
			c <- m
		}(keys[i], c[i])
	}
	m := 0
	for i := 0; i < clients; i++ {
		m += <-c[i]
	}
	return m
}

func readAndWriteValues(vs *valuesstore.ValuesStore, keys [][]byte, keys2 [][]byte, value []byte, clients int, valuesPerClient int) (chan int, chan struct{}) {
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
