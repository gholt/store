package main

import (
	"encoding/binary"
	//"runtime/pprof"
	"bytes"
	"fmt"
	"github.com/gholt/brimstore"
	"github.com/gholt/brimutil"
	"os"
	"runtime"
	"sync"
	"time"
)

func main() {
	/*
	   cpuProf, err := os.Create("brimstore.cpu.pprof")
	   if err != nil {
	       panic(err)
	   }
	   pprof.StartCPUProfile(cpuProf)
	*/
	/*
	   blockPprof := pprof.Lookup("block")
	   runtime.SetBlockProfileRate(1)
	*/
	seed := int64(1)
	valueLength := 128
	fmt.Println(valueLength, "value length")
	targetBytes := 1 * 1024 * 1024
	fmt.Println(targetBytes, "target bytes")
	cores := runtime.GOMAXPROCS(0)
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
		cores = runtime.GOMAXPROCS(0)
	}
	fmt.Println(cores, "cores")
	clients := cores * cores
	fmt.Println(clients, "clients")
	keysPerClient := targetBytes / valueLength / clients
	fmt.Println(keysPerClient, "keys per client")
	totalKeys := clients * keysPerClient
	fmt.Println(totalKeys, "total keys")
	totalValueLength := totalKeys * valueLength
	fmt.Println(totalValueLength, "total value length")
	start := time.Now()
	wg := &sync.WaitGroup{}
	keys := make([][]byte, clients)
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		keys[i] = make([]byte, keysPerClient*16)
		go func(i int) {
			brimutil.NewSeededScrambled(seed + int64(keysPerClient*i)).Read(keys[i])
			wg.Done()
		}(i)
	}
	wg.Wait()
	value := make([]byte, valueLength)
	brimutil.NewSeededScrambled(seed).Read(value)
	fmt.Println(time.Now().Sub(start), "to make keys and value")
	start = time.Now()
	speedStart := start
	s := brimstore.NewStore()
	s.Start()
	fmt.Println(time.Now().Sub(start), "to start store")
	start = time.Now()
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func(keys []byte, seq uint64) {
			var err error
			w := &brimstore.WriteValue{
				Value:       value,
				WrittenChan: make(chan error, 1),
				Seq:         seq,
			}
			for o := 0; o < len(keys); o += 16 {
				w.KeyHashA = binary.LittleEndian.Uint64(keys[o:])
				w.KeyHashB = binary.LittleEndian.Uint64(keys[o+8:])
				w.Seq++
				s.Put(w)
				err = <-w.WrittenChan
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}(keys[i], uint64(i*keysPerClient))
	}
	wg.Wait()
	fmt.Println(time.Now().Sub(start), "to add keys")
	start = time.Now()
	bytesWritten := s.Stop()
	speedStop := time.Now()
	fmt.Println(time.Now().Sub(start), "to stop store")
	fmt.Println(bytesWritten, "bytes written")
	seconds := float64(speedStop.UnixNano()-speedStart.UnixNano()) / 1000000000.0
	fmt.Printf("%.2fG/s based on total value length\n", float64(totalValueLength)/seconds/1024.0/1024.0/1024.0)
	fmt.Printf("%.2fG/s based on total bytes to disk\n", float64(bytesWritten)/seconds/1024.0/1024.0/1024.0)
	var st runtime.MemStats
	runtime.ReadMemStats(&st)
	fmt.Printf("%.2fG total alloc\n", float64(st.TotalAlloc)/1024/1024/1024)

	fmt.Println()
	start = time.Now()
	speedStart = start
	c := make([]chan int, clients)
	for i := 0; i < clients; i++ {
		c[i] = make(chan int)
		go func(keys []byte, c chan int) {
			r := &brimstore.ReadValue{
				ReadChan: make(chan error, 1),
			}
			m := 0
			for o := 0; o < len(keys); o += 16 {
				r.KeyHashA = binary.LittleEndian.Uint64(keys[o:])
				r.KeyHashB = binary.LittleEndian.Uint64(keys[o+8:])
				s.Get(r)
				err := <-r.ReadChan
				if err != nil {
					panic(err)
				}
				if r.Value == nil {
					m++
				}
				if !bytes.Equal(r.Value, value) {
					panic(fmt.Sprintf("%#v != %#v", string(r.Value), string(value)))
				}
			}
			c <- m
		}(keys[i], c[i])
	}
	m := 0
	for i := 0; i < clients; i++ {
		m += <-c[i]
	}
	speedStop = time.Now()
	fmt.Println(time.Now().Sub(start), "to lookup keys")
	nanoseconds := speedStop.UnixNano() - speedStart.UnixNano()
	seconds = float64(speedStop.UnixNano()-speedStart.UnixNano()) / 1000000000.0
	fmt.Printf("%.2f key lookups per second\n", float64(totalKeys)/seconds)
	fmt.Printf("%.2fns per key lookup\n", float64(nanoseconds)/float64(totalKeys))
	fmt.Println(m, "keys missing")

	fmt.Println()
	start = time.Now()
	keys2 := make([][]byte, clients)
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		keys2[i] = make([]byte, keysPerClient*16)
		go func(i int) {
			brimutil.NewSeededScrambled(seed + int64(totalKeys+keysPerClient*i)).Read(keys2[i])
			wg.Done()
		}(i)
	}
	wg.Wait()
	fmt.Println(time.Now().Sub(start), "to make second set of keys")
	start = time.Now()
	speedStart = start
	s.Start()
	fmt.Println(time.Now().Sub(start), "to restart store")
	start = time.Now()
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func(keys []byte, seq uint64) {
			var err error
			w := &brimstore.WriteValue{
				Value:       value,
				WrittenChan: make(chan error, 1),
				Seq:         seq,
			}
			for o := 0; o < len(keys); o += 16 {
				w.KeyHashA = binary.LittleEndian.Uint64(keys[o:])
				w.KeyHashB = binary.LittleEndian.Uint64(keys[o+8:])
				w.Seq++
				s.Put(w)
				err = <-w.WrittenChan
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}(keys2[i], uint64(i*keysPerClient))
		go func(keys []byte, c chan int) {
			r := &brimstore.ReadValue{
				ReadChan: make(chan error, 1),
			}
			m := 0
			for o := 0; o < len(keys); o += 16 {
				r.KeyHashA = binary.LittleEndian.Uint64(keys[o:])
				r.KeyHashB = binary.LittleEndian.Uint64(keys[o+8:])
				s.Get(r)
				err := <-r.ReadChan
				if err != nil {
					panic(err)
				}
				if r.Value == nil {
					m++
				}
				if !bytes.Equal(r.Value, value) {
					panic(fmt.Sprintf("%#v != %#v", string(r.Value), string(value)))
				}
			}
			c <- m
		}(keys[i], c[i])
	}
	wg.Wait()
	fmt.Println(time.Now().Sub(start), "to add new keys while looking up old keys")
	start = time.Now()
	bytesWritten2 := s.Stop()
	speedStop = time.Now()
	fmt.Println(time.Now().Sub(start), "to stop store")
	fmt.Println(bytesWritten2-bytesWritten, "bytes written")
	nanoseconds = speedStop.UnixNano() - speedStart.UnixNano()
	seconds = float64(speedStop.UnixNano()-speedStart.UnixNano()) / 1000000000.0
	fmt.Printf("%.2fG/s based on total value length\n", float64(totalValueLength)/seconds/1024.0/1024.0/1024.0)
	fmt.Printf("%.2fG/s based on total bytes to disk\n", float64(bytesWritten2-bytesWritten)/seconds/1024.0/1024.0/1024.0)
	m = 0
	for i := 0; i < clients; i++ {
		m += <-c[i]
	}
	speedStop = time.Now()
	nanoseconds = speedStop.UnixNano() - speedStart.UnixNano()
	seconds = float64(speedStop.UnixNano()-speedStart.UnixNano()) / 1000000000.0
	runtime.ReadMemStats(&st)
	fmt.Printf("%.2fG total alloc\n", float64(st.TotalAlloc)/1024/1024/1024)
	fmt.Printf("%.2f key lookups per second\n", float64(totalKeys)/seconds)
	fmt.Printf("%.2fns per key lookup\n", float64(nanoseconds)/float64(totalKeys))
	fmt.Println(m, "keys missing")

	fmt.Println()
	start = time.Now()
	speedStart = start
	for i := 0; i < clients; i++ {
		go func(keys []byte, c chan int) {
			r := &brimstore.ReadValue{
				ReadChan: make(chan error, 1),
			}
			m := 0
			for o := 0; o < len(keys); o += 16 {
				r.KeyHashA = binary.LittleEndian.Uint64(keys[o:])
				r.KeyHashB = binary.LittleEndian.Uint64(keys[o+8:])
				s.Get(r)
				err := <-r.ReadChan
				if err != nil {
					panic(err)
				}
				if r.Value == nil {
					m++
				}
				if !bytes.Equal(r.Value, value) {
					panic(fmt.Sprintf("%#v != %#v", string(r.Value), string(value)))
				}
			}
			c <- m
		}(keys[i], c[i])
	}
	m = 0
	for i := 0; i < clients; i++ {
		m += <-c[i]
	}
	for i := 0; i < clients; i++ {
		go func(keys []byte, c chan int) {
			r := &brimstore.ReadValue{
				ReadChan: make(chan error, 1),
			}
			m := 0
			for o := 0; o < len(keys); o += 16 {
				r.KeyHashA = binary.LittleEndian.Uint64(keys[o:])
				r.KeyHashB = binary.LittleEndian.Uint64(keys[o+8:])
				s.Get(r)
				err := <-r.ReadChan
				if err != nil {
					panic(err)
				}
				if r.Value == nil {
					m++
				}
				if !bytes.Equal(r.Value, value) {
					panic(fmt.Sprintf("%#v != %#v", string(r.Value), string(value)))
				}
			}
			c <- m
		}(keys2[i], c[i])
	}
	for i := 0; i < clients; i++ {
		m += <-c[i]
	}
	speedStop = time.Now()
	fmt.Println(time.Now().Sub(start), "to lookup both sets of keys")
	nanoseconds = speedStop.UnixNano() - speedStart.UnixNano()
	seconds = float64(speedStop.UnixNano()-speedStart.UnixNano()) / 1000000000.0
	fmt.Printf("%.2f key lookups per second\n", float64(totalKeys*2)/seconds)
	fmt.Printf("%.2fns per key lookup\n", float64(nanoseconds)/float64(totalKeys*2))
	fmt.Println(m, "keys missing")
	/*
	   f, err := os.Create("brimstore.blocking.pprof")
	   if err != nil {
	       panic(err)
	   }
	   blockPprof.WriteTo(f, 0)
	   f.Close()
	*/
	/*
	   pprof.StopCPUProfile()
	   cpuProf.Close()
	*/
}
