package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/brimstore"
	"github.com/gholt/brimutil"
	"github.com/jessevdk/go-flags"
)

type optsStruct struct {
	Clients       int    `long:"clients" description:"The number of clients. Default: cores*cores"`
	Cores         int    `long:"cores" description:"The number of cores. Default: CPU core count"`
	ExtendedStats bool   `long:"extended-stats" description:"Extended statistics at exit."`
	Length        int    `short:"l" long:"length" description:"Length of values. Default: 0"`
	Number        int    `short:"n" long:"number" description:"Number of keys. Default: 0"`
	Random        int    `long:"random" description:"Random number seed. Default: 0"`
	Replicate     bool   `long:"replicate" description:"Creates a second value store that will test replication."`
	Timestamp     uint64 `long:"timestamp" description:"Timestamp value. Default: current time"`
	TombstoneAge  int    `long:"tombstone-age" description:"Seconds to keep tombstones. Default: 4 hours"`
	Positional    struct {
		Tests []string `name:"tests" description:"background delete lookup read run write"`
	} `positional-args:"yes"`
	keyspace []byte
	buffers  [][]byte
	st       runtime.MemStats
	vs       *brimstore.ValueStore
	vs2      *brimstore.ValueStore
}

var opts optsStruct
var parser = flags.NewParser(&opts, flags.Default)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		args = append(args, "-h")
	}
	if _, err := parser.ParseArgs(args); err != nil {
		os.Exit(1)
	}
	for _, arg := range opts.Positional.Tests {
		switch arg {
		case "background":
		case "delete":
		case "lookup":
		case "read":
		case "run":
		case "write":
		default:
			fmt.Fprintf(os.Stderr, "Unknown test named %#v.\n", arg)
			os.Exit(1)
		}
	}
	if opts.Cores > 0 {
		runtime.GOMAXPROCS(opts.Cores)
	} else if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
	opts.Cores = runtime.GOMAXPROCS(0)
	if opts.Clients == 0 {
		opts.Clients = opts.Cores * opts.Cores
	}
	if opts.Timestamp == 0 {
		opts.Timestamp = uint64(time.Now().UnixNano())
	}
	opts.keyspace = make([]byte, opts.Number*16)
	brimutil.NewSeededScrambled(int64(opts.Random)).Read(opts.keyspace)
	opts.buffers = make([][]byte, opts.Clients)
	for i := 0; i < opts.Clients; i++ {
		opts.buffers[i] = make([]byte, 4*1024*1024)
	}
	fmt.Println(opts.Cores, "cores")
	fmt.Println(opts.Clients, "clients")
	fmt.Println(opts.Number, "values")
	fmt.Println(opts.Length, "value length")
	memstat()
	begin := time.Now()
	vsopts := brimstore.OptList(brimstore.OptCores(opts.Cores))
	if opts.TombstoneAge > 0 {
		vsopts = append(vsopts, brimstore.OptTombstoneAge(opts.TombstoneAge))
	}
	if opts.Replicate {
		prc := make(chan brimstore.PullReplicationMsg, opts.Cores)
		vs2opts := brimstore.OptList(vsopts...)
		vs2opts = append(vs2opts, brimstore.OptPath("replicated"))
		vs2opts = append(vs2opts, brimstore.OptOutPullReplicationChan(prc))
		opts.vs2 = brimstore.NewValueStore(vs2opts...)
		vsopts = append(vsopts, brimstore.OptInPullReplicationChan(prc))
	}
	opts.vs = brimstore.NewValueStore(vsopts...)
	opts.vs.BackgroundStart()
	dur := time.Now().Sub(begin)
	fmt.Println(dur, "to start ValuesStore")
	memstat()
	for _, arg := range opts.Positional.Tests {
		switch arg {
		case "background":
			background()
		case "delete":
			delete()
		case "lookup":
			lookup()
		case "read":
			read()
		case "run":
			run()
		case "write":
			write()
		}
		memstat()
	}
	begin = time.Now()
	opts.vs.Close()
	if opts.vs2 != nil {
		opts.vs2.Close()
	}
	dur = time.Now().Sub(begin)
	fmt.Println(dur, "to close value store(s)")
	memstat()
	begin = time.Now()
	statsCount, statsLength, stats := opts.vs.GatherStats(opts.ExtendedStats)
	dur = time.Now().Sub(begin)
	fmt.Println(dur, "to gather stats")
	if opts.ExtendedStats {
		fmt.Println(stats.String())
	} else {
		fmt.Println(statsCount, "ValueCount")
		fmt.Println(statsLength, "ValuesLength")
	}
	memstat()
	if opts.vs2 != nil {
		begin = time.Now()
		statsCount, statsLength, stats := opts.vs2.GatherStats(opts.ExtendedStats)
		dur = time.Now().Sub(begin)
		fmt.Println(dur, "to gather stats for replicated store")
		if opts.ExtendedStats {
			fmt.Println(stats.String())
		} else {
			fmt.Println(statsCount, "ValueCount")
			fmt.Println(statsLength, "ValuesLength")
		}
		memstat()
	}
}

func memstat() {
	lastAlloc := opts.st.TotalAlloc
	runtime.ReadMemStats(&opts.st)
	deltaAlloc := opts.st.TotalAlloc - lastAlloc
	lastAlloc = opts.st.TotalAlloc
	fmt.Printf("%0.2fG total alloc, %0.2fG delta\n\n", float64(opts.st.TotalAlloc)/1024/1024/1024, float64(deltaAlloc)/1024/1024/1024)
}

func background() {
	begin := time.Now()
	opts.vs.BackgroundNow(opts.Cores)
	if opts.vs2 != nil {
		opts.vs2.BackgroundNow(opts.Cores)
	}
	dur := time.Now().Sub(begin)
	fmt.Printf("%s to run background tasks\n", dur)
}

func delete() {
	var superseded uint64
	timestamp := opts.Timestamp | 1
	begin := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(opts.Clients)
	for i := 0; i < opts.Clients; i++ {
		go func(client int) {
			var s uint64
			number := len(opts.keyspace) / 16
			numberPer := number / opts.Clients
			var keys []byte
			if client == opts.Clients-1 {
				keys = opts.keyspace[numberPer*client*16:]
			} else {
				keys = opts.keyspace[numberPer*client*16 : numberPer*(client+1)*16]
			}
			for o := 0; o < len(keys); o += 16 {
				if oldTimestamp, err := opts.vs.Delete(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), timestamp); err != nil {
					panic(err)
				} else if oldTimestamp > timestamp {
					s++
				}
			}
			if s > 0 {
				atomic.AddUint64(&superseded, s)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	dur := time.Now().Sub(begin)
	fmt.Printf("%s %.0f/s to delete %d values (timestamp %d)\n", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), opts.Number, timestamp)
	if superseded > 0 {
		fmt.Println(superseded, "SUPERCEDED!")
	}
}

func lookup() {
	var missing uint64
	var deleted uint64
	begin := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(opts.Clients)
	for i := 0; i < opts.Clients; i++ {
		go func(client int) {
			number := len(opts.keyspace) / 16
			numberPer := number / opts.Clients
			var keys []byte
			if client == opts.Clients-1 {
				keys = opts.keyspace[numberPer*client*16:]
			} else {
				keys = opts.keyspace[numberPer*client*16 : numberPer*(client+1)*16]
			}
			var m uint64
			var d uint64
			for o := 0; o < len(keys); o += 16 {
				timestamp, _, err := opts.vs.Lookup(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]))
				if err == brimstore.ErrNotFound {
					if timestamp == 0 {
						m++
					} else {
						d++
					}
				} else if err != nil {
					panic(err)
				}
			}
			if m > 0 {
				atomic.AddUint64(&missing, m)
			}
			if d > 0 {
				atomic.AddUint64(&deleted, d)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	dur := time.Now().Sub(begin)
	fmt.Printf("%s %.0f/s to lookup %d values\n", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), opts.Number)
	if missing > 0 {
		fmt.Println(missing, "MISSING!")
	}
	if deleted > 0 {
		fmt.Println(deleted, "DELETED!")
	}
}

func read() {
	var valuesLength uint64
	var missing uint64
	var deleted uint64
	start := []byte("START67890")
	stop := []byte("123456STOP")
	wg := &sync.WaitGroup{}
	wg.Add(opts.Clients)
	begin := time.Now()
	for i := 0; i < opts.Clients; i++ {
		go func(client int) {
			f := func(keys []byte) {
				var vl uint64
				var m uint64
				var d uint64
				for o := 0; o < len(keys); o += 16 {
					timestamp, v, err := opts.vs.Read(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), opts.buffers[client][:0])
					if err == brimstore.ErrNotFound {
						if timestamp == 0 {
							m++
						} else {
							d++
						}
					} else if err != nil {
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
				if m > 0 {
					atomic.AddUint64(&missing, m)
				}
				if d > 0 {
					atomic.AddUint64(&deleted, d)
				}
			}
			number := len(opts.keyspace) / 16
			numberPer := number / opts.Clients
			var keys []byte
			if client == opts.Clients-1 {
				keys = opts.keyspace[numberPer*client*16:]
			} else {
				keys = opts.keyspace[numberPer*client*16 : numberPer*(client+1)*16]
			}
			keysplit := len(keys) / 16 / opts.Clients * client * 16
			f(keys[:keysplit])
			f(keys[keysplit:])
			wg.Done()
		}(i)
	}
	wg.Wait()
	dur := time.Now().Sub(begin)
	fmt.Printf("%s %.0f/s %0.2fG/s to read %d values\n", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), float64(valuesLength)/(float64(dur)/float64(time.Second))/1024/1024/1024, opts.Number)
	if missing > 0 {
		fmt.Println(missing, "MISSING!")
	}
	if deleted > 0 {
		fmt.Println(deleted, "DELETED!")
	}
}

func write() {
	var superseded uint64
	timestamp := opts.Timestamp & 0xfffffffffffffffe
	if timestamp == 0 {
		timestamp = 2
	}
	begin := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(opts.Clients)
	for i := 0; i < opts.Clients; i++ {
		go func(client int) {
			value := make([]byte, opts.Length)
			randomness := value
			if len(value) > 10 {
				copy(value, []byte("START67890"))
				randomness = value[10:]
				if len(value) > 20 {
					copy(value[len(value)-10:], []byte("123456STOP"))
					randomness = value[10 : len(value)-10]
				}
			}
			scr := brimutil.NewScrambled()
			var s uint64
			number := len(opts.keyspace) / 16
			numberPer := number / opts.Clients
			var keys []byte
			if client == opts.Clients-1 {
				keys = opts.keyspace[numberPer*client*16:]
			} else {
				keys = opts.keyspace[numberPer*client*16 : numberPer*(client+1)*16]
			}
			for o := 0; o < len(keys); o += 16 {
				scr.Read(randomness)
				// test putting all keys in a certain range:
				// if oldTimestamp, err := opts.vs.Write(binary.BigEndian.Uint64(keys[o:]) & 0x000fffffffffffff, binary.BigEndian.Uint64(keys[o+8:]), timestamp, value); err != nil {
				if oldTimestamp, err := opts.vs.Write(binary.BigEndian.Uint64(keys[o:]), binary.BigEndian.Uint64(keys[o+8:]), timestamp, value); err != nil {
					panic(err)
				} else if oldTimestamp > timestamp {
					s++
				}
			}
			if s > 0 {
				atomic.AddUint64(&superseded, s)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	dur := time.Now().Sub(begin)
	fmt.Printf("%s %.0f/s %0.2fG/s to write %d values (timestamp %d)\n", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), float64(opts.Number*opts.Length)/(float64(dur)/float64(time.Second))/1024/1024/1024, opts.Number, timestamp)
	if superseded > 0 {
		fmt.Println(superseded, "SUPERCEDED!")
	}
}

func run() {
	<-time.After(5 * time.Minute)
}
