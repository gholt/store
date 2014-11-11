package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
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
	Debug         bool   `long:"debug" description:"Turns on debug output."`
	ExtendedStats bool   `long:"extended-stats" description:"Extended statistics at exit."`
	Length        int    `short:"l" long:"length" description:"Length of values. Default: 0"`
	Number        int    `short:"n" long:"number" description:"Number of keys. Default: 0"`
	Random        int    `long:"random" description:"Random number seed. Default: 0"`
	Replicate     bool   `long:"replicate" description:"Creates a second value store that will test replication."`
	Timestamp     uint64 `long:"timestamp" description:"Timestamp value. Default: current time"`
	TombstoneAge  int    `long:"tombstone-age" description:"Seconds to keep tombstones. Default: 4 hours"`
	Positional    struct {
		Tests []string `name:"tests" description:"background blockprof cpuprof delete lookup read run write"`
	} `positional-args:"yes"`
	blockprofi int
	blockproff *os.File
	cpuprofi   int
	cpuproff   *os.File
	keyspace   []byte
	buffers    [][]byte
	st         runtime.MemStats
	vs         *brimstore.ValueStore
	rvs        *brimstore.ValueStore
}

var opts optsStruct
var parser = flags.NewParser(&opts, flags.Default)

func main() {
	log.Print("init:")
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
		case "blockprof":
		case "cpuprof":
		case "delete":
		case "lookup":
		case "read":
		case "run":
		case "write":
		default:
			log.Printf("unknown test named %#v", arg)
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
	memstat()
	log.Print("start:")
	begin := time.Now()
	vsopts := brimstore.OptList(brimstore.OptCores(opts.Cores))
	if opts.TombstoneAge > 0 {
		vsopts = append(vsopts, brimstore.OptTombstoneAge(opts.TombstoneAge))
	}
	wg := &sync.WaitGroup{}
	if opts.Replicate {
		conn, conn2 := net.Pipe()
		vs2opts := brimstore.OptList(vsopts...)
		vsopts = append(vsopts, brimstore.OptRing(&ring{1}))
		vs2opts = append(vs2opts, brimstore.OptRing(&ring{2}))
		vs2opts = append(vs2opts, brimstore.OptPath("replicated"))
		vs2opts = append(vs2opts, brimstore.OptMsgConn(NewPipeMsgConn(conn2)))
		vs2opts = append(vs2opts, brimstore.OptLogCritical(log.New(os.Stderr, "ReplicatedValueStore ", log.LstdFlags)))
		vs2opts = append(vs2opts, brimstore.OptLogError(log.New(os.Stderr, "ReplicatedValueStore ", log.LstdFlags)))
		vs2opts = append(vs2opts, brimstore.OptLogWarning(log.New(os.Stderr, "ReplicatedValueStore ", log.LstdFlags)))
		vs2opts = append(vs2opts, brimstore.OptLogInfo(log.New(os.Stdout, "ReplicatedValueStore ", log.LstdFlags)))
		if opts.Debug {
			vs2opts = append(vs2opts, brimstore.OptLogDebug(log.New(os.Stderr, "ReplicatedValueStore ", log.LstdFlags)))
		}
		wg.Add(1)
		go func() {
			opts.rvs = brimstore.NewValueStore(vs2opts...)
			wg.Done()
		}()
		vsopts = append(vsopts, brimstore.OptMsgConn(NewPipeMsgConn(conn)))
	}
	if opts.Debug {
		vsopts = append(vsopts, brimstore.OptLogDebug(log.New(os.Stderr, "ValueStore ", log.LstdFlags)))
	}
	opts.vs = brimstore.NewValueStore(vsopts...)
	wg.Wait()
	if opts.rvs != nil {
		opts.rvs.EnableWrites()
	}
	opts.vs.EnableWrites()
	if opts.rvs != nil {
		opts.rvs.EnableBackgroundTasks()
	}
	opts.vs.EnableBackgroundTasks()
	dur := time.Now().Sub(begin)
	log.Println(dur, "to start")
	memstat()
	for _, arg := range opts.Positional.Tests {
		switch arg {
		case "background":
			background()
		case "blockprof":
			if opts.blockproff != nil {
				log.Print("blockprof: off")
				runtime.SetBlockProfileRate(0)
				pprof.Lookup("block").WriteTo(opts.blockproff, 1)
				opts.blockproff.Close()
				opts.blockproff = nil
			} else {
				log.Print("blockprof: on")
				var err error
				opts.blockproff, err = os.Create(fmt.Sprintf("blockprof%d", opts.blockprofi))
				opts.blockprofi++
				if err != nil {
					log.Print(err)
					os.Exit(1)
				}
				runtime.SetBlockProfileRate(1)
			}
		case "cpuprof":
			if opts.cpuproff != nil {
				log.Print("cpuprof: off")
				pprof.StopCPUProfile()
				opts.cpuproff.Close()
				opts.cpuproff = nil
			} else {
				log.Print("cpuprof: on")
				var err error
				opts.cpuproff, err = os.Create(fmt.Sprintf("cpuprof%d", opts.cpuprofi))
				opts.cpuprofi++
				if err != nil {
					log.Print(err)
					os.Exit(1)
				}
				pprof.StartCPUProfile(opts.cpuproff)
			}
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
	log.Print("close:")
	begin = time.Now()
	if opts.rvs != nil {
		wg.Add(1)
		go func() {
			opts.rvs.DisableBackgroundTasks()
			opts.rvs.DisableWrites()
			opts.rvs.Flush()
			wg.Done()
		}()
	}
	opts.vs.DisableBackgroundTasks()
	opts.vs.DisableWrites()
	opts.vs.Flush()
	wg.Wait()
	dur = time.Now().Sub(begin)
	log.Println(dur, "to close")
	memstat()
	log.Print("gather stats:")
	begin = time.Now()
	var statsCount2 uint64
	var statsLength2 uint64
	var stats2 fmt.Stringer
	if opts.rvs != nil {
		wg.Add(1)
		go func() {
			statsCount2, statsLength2, stats2 = opts.rvs.GatherStats(opts.ExtendedStats)
			wg.Done()
		}()
	}
	statsCount, statsLength, stats := opts.vs.GatherStats(opts.ExtendedStats)
	wg.Wait()
	dur = time.Now().Sub(begin)
	log.Println(dur, "to gather stats")
	if opts.ExtendedStats {
		log.Print("ValueStore: stats:\n", stats.String())
	} else {
		log.Println("ValueStore: count", statsCount)
		log.Println("ValueStore: length", statsLength)
	}
	if opts.rvs != nil {
		if opts.ExtendedStats {
			log.Print("ReplicatedValueStore: stats:\n", stats2.String())
		} else {
			log.Println("ReplicatedValueStore: count", statsCount2)
			log.Println("ReplicatedValueStore: length", statsLength2)
		}
	}
	memstat()
	if opts.blockproff != nil {
		runtime.SetBlockProfileRate(0)
		pprof.Lookup("block").WriteTo(opts.blockproff, 0)
		opts.blockproff.Close()
		opts.blockproff = nil
	}
	if opts.cpuproff != nil {
		pprof.StopCPUProfile()
		opts.cpuproff.Close()
		opts.cpuproff = nil
	}
}

func memstat() {
	lastAlloc := opts.st.TotalAlloc
	runtime.ReadMemStats(&opts.st)
	deltaAlloc := opts.st.TotalAlloc - lastAlloc
	lastAlloc = opts.st.TotalAlloc
	log.Printf("%0.2fG total alloc, %0.2fG delta", float64(opts.st.TotalAlloc)/1024/1024/1024, float64(deltaAlloc)/1024/1024/1024)
}

func background() {
	log.Print("disabling background tasks:")
	begin := time.Now()
	wg := &sync.WaitGroup{}
	if opts.rvs != nil {
		wg.Add(1)
		go func() {
			opts.rvs.DisableBackgroundTasks()
			wg.Done()
		}()
	}
	opts.vs.DisableBackgroundTasks()
	wg.Wait()
	dur := time.Now().Sub(begin)
	log.Println(dur, "to disable background tasks")
	log.Print("background tasks:")
	begin = time.Now()
	if opts.rvs != nil {
		wg.Add(1)
		go func() {
			opts.rvs.BackgroundNow()
			wg.Done()
		}()
	}
	opts.vs.BackgroundNow()
	wg.Wait()
	dur = time.Now().Sub(begin)
	log.Println(dur, "to run background tasks")
	log.Print("re-enabling background tasks:")
	begin = time.Now()
	if opts.rvs != nil {
		wg.Add(1)
		go func() {
			opts.rvs.EnableBackgroundTasks()
			wg.Done()
		}()
	}
	opts.vs.EnableBackgroundTasks()
	wg.Wait()
	dur = time.Now().Sub(begin)
	log.Println(dur, "to re-enable background tasks")
}

func delete() {
	log.Print("delete:")
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
	opts.vs.Flush()
	dur := time.Now().Sub(begin)
	log.Printf("%s %.0f/s to delete %d values (timestamp %d)", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), opts.Number, timestamp)
	if superseded > 0 {
		log.Println(superseded, "SUPERCEDED!")
	}
}

func lookup() {
	log.Print("lookup:")
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
	log.Printf("%s %.0f/s to lookup %d values", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), opts.Number)
	if missing > 0 {
		log.Println(missing, "MISSING!")
	}
	if deleted > 0 {
		log.Println(deleted, "DELETED!")
	}
}

func read() {
	log.Print("read:")
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
	log.Printf("%s %.0f/s %0.2fG/s to read %d values", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), float64(valuesLength)/(float64(dur)/float64(time.Second))/1024/1024/1024, opts.Number)
	if missing > 0 {
		log.Println(missing, "MISSING!")
	}
	if deleted > 0 {
		log.Println(deleted, "DELETED!")
	}
}

func write() {
	log.Print("write:")
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
				// if oldTimestamp, err := opts.vs.Write(binary.BigEndian.Uint64(keys[o:]) & 0x000fffffffffffff, binary.BigEndian.Uint64(keys[o+8:]), timestamp, value); err != nil {}
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
	opts.vs.Flush()
	dur := time.Now().Sub(begin)
	log.Printf("%s %.0f/s %0.2fG/s to write %d values (timestamp %d)", dur, float64(opts.Number)/(float64(dur)/float64(time.Second)), float64(opts.Number*opts.Length)/(float64(dur)/float64(time.Second))/1024/1024/1024, opts.Number, timestamp)
	if superseded > 0 {
		log.Println(superseded, "SUPERCEDED!")
	}
}

func run() {
	log.Print("run:")
	begin := time.Now()
	<-time.After(1 * time.Minute)
	dur := time.Now().Sub(begin)
	log.Println(dur, "to run")
}
