package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/gholt/brimstore"
	"github.com/gholt/brimutil"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf(`
start/stop	Simply starts the store and then stops it. Measures recovery.
add n s q z	Adds n random values seeded with s using sequence number q and
		the values will be z in length.
read n s	Reads n random values seeded with s.
lookup n s	Looks up n random values seeded with s.
		`)
		os.Exit(1)
	} else {
		switch os.Args[1] {
		case "old":
			old()
		case "start/stop":
			startstop()
		case "add":
			add()
		case "read":
			read()
		case "lookup":
			lookup()
		default:
			fmt.Printf("unknown command %#v\n", old)
			os.Exit(1)
		}
	}
}

func createKeys(seed int, clients int, keys int) [][]byte {
	wg := &sync.WaitGroup{}
	clientKeys := make([][]byte, clients)
	keysPerClient := keys / clients
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		if i == clients-1 {
			clientKeys[i] = make([]byte, keysPerClient*16+(keys-keysPerClient*clients)*16)
		} else {
			clientKeys[i] = make([]byte, keys/clients*16)
		}
		go func(i int) {
			brimutil.NewSeededScrambled(int64(seed) + int64(keysPerClient*i)).Read(clientKeys[i])
			wg.Done()
		}(i)
	}
	wg.Wait()
	return clientKeys
}

func writeValues(vs *brimstore.ValuesStore, keys [][]byte, seq int, value []byte, clients int) {
	wg := &sync.WaitGroup{}
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func(keys []byte, seq uint64) {
			for o := 0; o < len(keys); o += 16 {
				seq++
				if oldSeq, err := vs.WriteValue(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), seq, value); err != nil {
					panic(err)
				} else if oldSeq > seq {
					panic(fmt.Sprintf("%d > %d\n", oldSeq, seq))
				}
			}
			wg.Done()
		}(keys[i], uint64(seq))
	}
	wg.Wait()
}

func readValues(vs *brimstore.ValuesStore, keys [][]byte, buffers [][]byte, clients int) uint64 {
	var valuesLength uint64
	start := []byte("START67890")
	stop := []byte("123456STOP")
	wg := &sync.WaitGroup{}
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func(keys []byte, buffer []byte) {
			var err error
			var v []byte
			var vl uint64
			for o := 0; o < len(keys); o += 16 {
				_, v, err = vs.ReadValue(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), buffer[:0])
				if err != nil {
					panic(err)
				} else if len(v) > 10 && !bytes.Equal(v[:10], start) {
					panic("bad start to value")
				} else if len(v) > 20 && !bytes.Equal(v[len(v)-10:], stop) {
					panic("bad stop to value")
				} else {
					vl += uint64(len(v))
				}
			}
			if vl > 0 {
				atomic.AddUint64(&valuesLength, vl)
			}
			wg.Done()
		}(keys[i], buffers[i])
	}
	wg.Wait()
	return atomic.LoadUint64(&valuesLength)
}

func lookupValues(vs *brimstore.ValuesStore, keys [][]byte, clients int) {
	wg := &sync.WaitGroup{}
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func(keys []byte) {
			var err error
			for o := 0; o < len(keys); o += 16 {
				_, _, err = vs.LookupValue(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]))
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}(keys[i])
	}
	wg.Wait()
}
