// Package valuestore provides a disk-backed data structure for use in storing
// []byte values referenced by 128 bit keys with options for replication.
//
// It can handle billions of keys (as memory allows) and full concurrent access
// across many cores. All location information about each key is stored in
// memory for speed, but values are stored on disk with the exception of
// recently written data being buffered first and batched to disk later.
//
// This has been written with SSDs in mind, but spinning drives should work as
// well; though storing valuestoc files (Table Of Contents, key location
// information) on a separate disk from values files is recommended in that
// case.
//
// Each key is two 64bit values, known as keyA and keyB uint64 values. These
// are usually created by a hashing function of the key name, but that duty is
// left outside this package.
//
// Each modification is marked with a uint64 timestamp that is equivalent to
// uint64(time.Now().UnixNano()). However, the last bit is used to indicate
// deletion versus an actual value. This is important to allow deletions of
// specific values at any time, without having the possibility of deleting any
// newer values (e.g. expiring values).
//
// There are background tasks for:
//
// * Discard: This will discard older tombstones (deletion markers). Tombstones
// are kept for OptTombstoneAge seconds and are used to ensure a replicated
// older value doesn't resurrect a deleted value. But, keeping all tombstones
// for all time is a waste of resources, so they are discarded over time.
// OptTombstoneAge controls how long they should be kept and should be set to
// an amount greater than a few replication passes.
//
// * OutPullReplication: This will continually send out pull replication
// requests for all the partitions a ValueStore is responsible for, as
// determined by the OptMsgRing. The other responsible parties will respond to
// these requests with data they have that was missing from the pull
// replication request. Bloom filters are used to reduce bandwidth which has
// the downside that a very small percentage of items may be missed each pass.
// A moving salt is used with each bloom filter so that after a few passes
// there is an exceptionally high probability that all items will be accounted
// for.
//
// TODO list probably not comprehensive:
//  Push replication
//  Compaction
package valuestore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"path"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/brimtext"
	"github.com/gholt/brimutil"
	"github.com/gholt/ring"
	"github.com/gholt/valuelocmap"
	"github.com/spaolacci/murmur3"
)

// ValueStore is an interface for a disk-backed data structure that stores
// []byte values referenced by 128 bit keys with options for replication.
//
// For documentation on each of these functions, see the DefaultValueStore.
type ValueStore interface {
	Lookup(keyA uint64, keyB uint64) (uint64, uint32, error)
	Read(keyA uint64, keyB uint64, value []byte) (uint64, []byte, error)
	Write(keyA uint64, keyB uint64, timestamp uint64, value []byte) (uint64, error)
	Delete(keyA uint64, keyB uint64, timestamp uint64) (uint64, error)
	EnableDiscard()
	DisableDiscard()
	DiscardPass()
	EnableOutPullReplication()
	DisableOutPullReplication()
	OutPullReplicationPass()
	EnableWrites()
	DisableWrites()
	Flush()
	GatherStats(debug bool) (uint64, uint64, fmt.Stringer)
	MaxValueSize() uint32
}

type config struct {
	logCritical                *log.Logger
	logError                   *log.Logger
	logWarning                 *log.Logger
	logInfo                    *log.Logger
	logDebug                   *log.Logger
	rand                       *rand.Rand
	path                       string
	pathtoc                    string
	vlm                        valuelocmap.ValueLocMap
	workers                    int
	discardInterval            int
	outPullReplicationWorkers  int
	outPullReplicationInterval int
	maxValueSize               int
	pageSize                   int
	minValueAlloc              int
	writePagesPerWorker        int
	tombstoneAge               int
	valuesFileSize             int
	valuesFileReaders          int
	checksumInterval           int
	ring                       ring.MsgRing
	replicationIgnoreRecent    int
}

func resolveConfig(opts ...func(*config)) *config {
	cfg := &config{}
	cfg.path = os.Getenv("VALUESTORE_PATH")
	cfg.pathtoc = os.Getenv("VALUESTORE_PATHTOC")
	cfg.workers = runtime.GOMAXPROCS(0)
	if env := os.Getenv("VALUESTORE_WORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.workers = val
		}
	}
	cfg.discardInterval = 60
	if env := os.Getenv("VALUESTORE_DISCARDINTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.discardInterval = val
		}
	}
	cfg.outPullReplicationWorkers = cfg.workers
	if env := os.Getenv("VALUESTORE_OUTPULLREPLICATIONWORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.outPullReplicationWorkers = val
		}
	}
	cfg.outPullReplicationInterval = 60
	if env := os.Getenv("VALUESTORE_OUTPULLREPLICATIONINTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.outPullReplicationInterval = val
		}
	}
	cfg.maxValueSize = 4 * 1024 * 1024
	if env := os.Getenv("VALUESTORE_MAXVALUESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.maxValueSize = val
		}
	}
	cfg.pageSize = 4 * 1024 * 1024
	if env := os.Getenv("VALUESTORE_PAGESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.pageSize = val
		}
	}
	cfg.writePagesPerWorker = 3
	if env := os.Getenv("VALUESTORE_WRITEPAGESPERWORKER"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.writePagesPerWorker = val
		}
	}
	cfg.tombstoneAge = 4 * 60 * 60
	if env := os.Getenv("VALUESTORE_TOMBSTONEAGE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.tombstoneAge = val
		}
	}
	cfg.valuesFileSize = math.MaxUint32
	if env := os.Getenv("VALUESTORE_VALUESFILESIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.valuesFileSize = val
		}
	}
	cfg.valuesFileReaders = cfg.workers
	if env := os.Getenv("VALUESTORE_VALUESFILEREADERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.valuesFileReaders = val
		}
	}
	cfg.checksumInterval = 65532
	if env := os.Getenv("VALUESTORE_CHECKSUMINTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.checksumInterval = val
		}
	}
	cfg.replicationIgnoreRecent = 60
	if env := os.Getenv("VALUESTORE_REPLICATIONIGNORERECENT"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.replicationIgnoreRecent = val
		}
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.logCritical == nil {
		cfg.logCritical = log.New(os.Stderr, "ValueStore ", log.LstdFlags)
	}
	if cfg.logError == nil {
		cfg.logError = log.New(os.Stderr, "ValueStore ", log.LstdFlags)
	}
	if cfg.logWarning == nil {
		cfg.logWarning = log.New(os.Stderr, "ValueStore ", log.LstdFlags)
	}
	if cfg.logInfo == nil {
		cfg.logInfo = log.New(os.Stdout, "ValueStore ", log.LstdFlags)
	}
	if cfg.rand == nil {
		cfg.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if cfg.path == "" {
		cfg.path = "."
	}
	if cfg.pathtoc == "" {
		cfg.pathtoc = cfg.path
	}
	if cfg.workers < 1 {
		cfg.workers = 1
	}
	if cfg.discardInterval < 1 {
		cfg.discardInterval = 1
	}
	if cfg.outPullReplicationWorkers < 1 {
		cfg.outPullReplicationWorkers = 1
	}
	if cfg.outPullReplicationInterval < 1 {
		cfg.outPullReplicationInterval = 1
	}
	if cfg.maxValueSize < 0 {
		cfg.maxValueSize = 0
	}
	if cfg.maxValueSize > math.MaxUint32 {
		cfg.maxValueSize = math.MaxUint32
	}
	if cfg.checksumInterval < 1 {
		cfg.checksumInterval = 1
	}
	// Ensure each page will have at least checksumInterval worth of data in it
	// so that each page written will at least flush the previous page's data.
	if cfg.pageSize < cfg.maxValueSize+cfg.checksumInterval {
		cfg.pageSize = cfg.maxValueSize + cfg.checksumInterval
	}
	// Absolute minimum: timestamp leader plus at least one TOC entry
	if cfg.pageSize < 40 {
		cfg.pageSize = 40
	}
	if cfg.pageSize > math.MaxUint32 {
		cfg.pageSize = math.MaxUint32
	}
	// Ensure a full TOC page will have an associated data page of at least
	// checksumInterval in size, again so that each page written will at least
	// flush the previous page's data.
	cfg.minValueAlloc = cfg.checksumInterval/(cfg.pageSize/32+1) + 1
	if cfg.writePagesPerWorker < 2 {
		cfg.writePagesPerWorker = 2
	}
	if cfg.tombstoneAge < 0 {
		cfg.tombstoneAge = 0
	}
	if cfg.valuesFileSize < 48+cfg.maxValueSize { // header value trailer
		cfg.valuesFileSize = 48 + cfg.maxValueSize
	}
	if cfg.valuesFileSize > math.MaxUint32 {
		cfg.valuesFileSize = math.MaxUint32
	}
	if cfg.valuesFileReaders < 1 {
		cfg.valuesFileReaders = 1
	}
	if cfg.replicationIgnoreRecent < 0 {
		cfg.replicationIgnoreRecent = 0
	}
	return cfg
}

// OptList returns a slice with the opts given; useful if you want to possibly
// append more options to the list before using it with New(list...).
func OptList(opts ...func(*config)) []func(*config) {
	return opts
}

// OptLogCritical sets the log.Logger to use for critical messages. Defaults
// logging to os.Stderr.
func OptLogCritical(l *log.Logger) func(*config) {
	return func(cfg *config) {
		cfg.logCritical = l
	}
}

// OptLogError sets the log.Logger to use for error messages. Defaults logging
// to os.Stderr.
func OptLogError(l *log.Logger) func(*config) {
	return func(cfg *config) {
		cfg.logError = l
	}
}

// OptLogWarning sets the log.Logger to use for warning messages. Defaults
// logging to os.Stderr.
func OptLogWarning(l *log.Logger) func(*config) {
	return func(cfg *config) {
		cfg.logWarning = l
	}
}

// OptLogInfo sets the log.Logger to use for info messages. Defaults logging to
// os.Stdout.
func OptLogInfo(l *log.Logger) func(*config) {
	return func(cfg *config) {
		cfg.logInfo = l
	}
}

// OptLogDebug sets the log.Logger to use for debug messages. Defaults not
// logging debug messages.
func OptLogDebug(l *log.Logger) func(*config) {
	return func(cfg *config) {
		cfg.logDebug = l
	}
}

// OptRand sets the rand.Rand to use as a random data source. Defaults to a new
// randomizer based on the current time.
func OptRand(r *rand.Rand) func(*config) {
	return func(cfg *config) {
		cfg.rand = r
	}
}

// OptPath sets the path where values files will be written; tocvalues files
// will also be written here unless overridden with OptPathTOC. Defaults to env
// VALUESTORE_PATH or the current working directory.
func OptPath(dirpath string) func(*config) {
	return func(cfg *config) {
		cfg.path = dirpath
	}
}

// OptPathTOC sets the path where tocvalues files will be written. Defaults to
// env VALUESTORE_PATHTOC or the OptPath value.
func OptPathTOC(dirpath string) func(*config) {
	return func(cfg *config) {
		cfg.pathtoc = dirpath
	}
}

// OptValueLocMap allows overriding the default ValueLocMap, an interface used
// by ValueStore for tracking the mappings from keys to the locations of their
// values. Defaults to github.com/gholt/valuelocmap.New().
func OptValueLocMap(vlm valuelocmap.ValueLocMap) func(*config) {
	return func(cfg *config) {
		cfg.vlm = vlm
	}
}

// OptWorkers indicates how many goroutines may be used for various tasks
// (processing incoming writes and batching them to disk, background tasks,
// etc.). Defaults to env VALUESTORE_WORKERS or GOMAXPROCS.
func OptWorkers(count int) func(*config) {
	return func(cfg *config) {
		cfg.workers = count
	}
}

// OptDiscardInterval indicates the minimum number of seconds betweeen the
// starts of discard passes (discarding expired tombstones [deletion markers]).
// If set to 60 seconds and the passes take 10 seconds to run, they will wait
// 50 seconds (with a small amount of randomization) between the stop of one
// run and the start of the next. This is really just meant to keep nearly
// empty structures from using a lot of resources doing nearly nothing.
// Normally, you'd want your discard passes to be running constantly so that
// they are as fast as possible and the load constant. The default of 60
// seconds is almost always fine. Defaults to env VALUESTORE_DISCARDINTERVAL or
// 60.
func OptDiscardInterval(seconds int) func(*config) {
	return func(cfg *config) {
		cfg.discardInterval = seconds
	}
}

// OptOutPullReplicationWorkers indicates how many goroutines may be used for
// an outgoing pull replication pass. Defaults to env
// VALUESTORE_OUTPULLREPLICATIONWORKERS or VALUESTORE_WORKERS.
func OptOutPullReplicationWorkers(workers int) func(*config) {
	return func(cfg *config) {
		cfg.outPullReplicationWorkers = workers
	}
}

// OptOutPullReplicationInterval indicates the minimum number of seconds
// between the starts of outgoing pull replication passes. If set to 60 seconds
// and the passes take 10 seconds to run, they will wait 50 seconds (with a
// small amount of randomization) between the stop of one run and the start of
// the next. This is really just meant to keep nearly empty structures from
// using a lot of resources doing nearly nothing. Normally, you'd want your
// outgoing pull replication passes to be running constantly so that
// replication is as fast as possible and the load constant. The default of 60
// seconds is almost always fine. Defaults to env
// VALUESTORE_OUTPULLREPLICATIONINTERVAL or 60.
func OptOutPullReplicationInterval(seconds int) func(*config) {
	return func(cfg *config) {
		cfg.outPullReplicationInterval = seconds
	}
}

// OptMaxValueSize indicates the maximum number of bytes any given value may
// be. Defaults to env VALUESTORE_MAXVALUESIZE or 4,194,304.
func OptMaxValueSize(bytes int) func(*config) {
	return func(cfg *config) {
		cfg.maxValueSize = bytes
	}
}

// OptPageSize controls the size of each chunk of memory allocated. Defaults to
// env VALUESTORE_PAGESIZE or 4,194,304.
func OptPageSize(bytes int) func(*config) {
	return func(cfg *config) {
		cfg.pageSize = bytes
	}
}

// OptWritePagesPerWorker controls how many pages are created per worker for
// caching recently written values. Defaults to env
// VALUESTORE_WRITEPAGESPERWORKER or 3.
func OptWritePagesPerWorker(number int) func(*config) {
	return func(cfg *config) {
		cfg.writePagesPerWorker = number
	}
}

// OptTombstoneAge indicates how many seconds old a deletion marker may be
// before it is permanently removed. Defaults to env VALUESTORE_TOMBSTONEAGE or
// 14,400 (4 hours).
func OptTombstoneAge(seconds int) func(*config) {
	return func(cfg *config) {
		cfg.tombstoneAge = seconds
	}
}

// OptValuesFileSize indicates how large a values file can be before closing it
// and opening a new one. Defaults to env VALUESTORE_VALUESFILESIZE or
// 4,294,967,295.
func OptValuesFileSize(bytes int) func(*config) {
	return func(cfg *config) {
		cfg.valuesFileSize = bytes
	}
}

// OptValuesFileReaders indicates how many open file descriptors are allowed
// per values file for reading. Defaults to env VALUESTORE_VALUESFILEREADERS or
// the configured number of workers.
func OptValuesFileReaders(bytes int) func(*config) {
	return func(cfg *config) {
		cfg.valuesFileReaders = bytes
	}
}

// OptChecksumInterval indicates how many bytes are output to a file before a
// 4-byte checksum is also output. Defaults to env VALUESTORE_CHECKSUMINTERVAL
// or 65532.
func OptChecksumInterval(bytes int) func(*config) {
	return func(cfg *config) {
		cfg.checksumInterval = bytes
	}
}

// OptMsgRing sets the ring.MsgRing to use for determining the key ranges the
// ValueStore is responsible for as well as providing methods to send messages
// to other nodes.
func OptMsgRing(r ring.MsgRing) func(*config) {
	return func(cfg *config) {
		cfg.ring = r
	}
}

// OptReplicationIgnoreRecent indicates how many seconds old a value should be
// before it is included in replication processing. Defaults to env
// VALUESTORE_REPLICATIONIGNORERECENT or 60.
func OptReplicationIgnoreRecent(seconds int) func(*config) {
	return func(cfg *config) {
		cfg.replicationIgnoreRecent = seconds
	}
}

var ErrNotFound error = errors.New("not found")
var ErrDisabled error = errors.New("disabled")

type pullReplicationMsg struct {
	vs     *DefaultValueStore
	header []byte
	body   []byte
}

type bulkSetMsg struct {
	vs   *DefaultValueStore
	body []byte
}

// DefaultValueStore instances are created with New.
type DefaultValueStore struct {
	logCritical                  *log.Logger
	logError                     *log.Logger
	logWarning                   *log.Logger
	logInfo                      *log.Logger
	logDebug                     *log.Logger
	rand                         *rand.Rand
	freeableVMChans              []chan *valuesMem
	freeVMChan                   chan *valuesMem
	freeVWRChans                 []chan *valueWriteReq
	pendingVWRChans              []chan *valueWriteReq
	vfVMChan                     chan *valuesMem
	freeTOCBlockChan             chan []byte
	pendingTOCBlockChan          chan []byte
	flushedChan                  chan struct{}
	valueLocBlocks               []valueLocBlock
	valueLocBlockIDer            uint64
	path                         string
	pathtoc                      string
	vlm                          valuelocmap.ValueLocMap
	workers                      int
	maxValueSize                 uint32
	pageSize                     uint32
	minValueAlloc                int
	writePagesPerWorker          int
	tombstoneAge                 uint64
	valuesFileSize               uint32
	valuesFileReaders            int
	checksumInterval             uint32
	ring                         ring.MsgRing
	discardInterval              int
	discardNotifyChan            chan *discardNotification
	discardAbort                 uint32
	replicationIgnoreRecent      uint64
	inPullReplicationChan        chan *pullReplicationMsg
	freeInPullReplicationChan    chan *pullReplicationMsg
	outPullReplicationWorkers    int
	outPullReplicationInterval   int
	outPullReplicationNotifyChan chan *outPullReplicationNotification
	outPullReplicationIteration  uint16
	outPullReplicationAbort      uint32
	outPullReplicationChan       chan *pullReplicationMsg
	ktbfs                        []*ktBloomFilter
	inBulkSetChan                chan *bulkSetMsg
	freeInBulkSetChan            chan *bulkSetMsg
	outBulkSetChan               chan *bulkSetMsg
}

type valueWriteReq struct {
	keyA      uint64
	keyB      uint64
	timestamp uint64
	value     []byte
	errChan   chan error
}

var enableValueWriteReq *valueWriteReq = &valueWriteReq{}
var disableValueWriteReq *valueWriteReq = &valueWriteReq{}
var flushValueWriteReq *valueWriteReq = &valueWriteReq{}

type valueLocBlock interface {
	timestamp() int64
	read(keyA uint64, keyB uint64, timestamp uint64, offset uint32, length uint32, value []byte) (uint64, []byte, error)
}

type valueStoreStats struct {
	debug                      bool
	freeableVMChansCap         int
	freeableVMChansIn          int
	freeVMChanCap              int
	freeVMChanIn               int
	freeVWRChans               int
	freeVWRChansCap            int
	freeVWRChansIn             int
	pendingVWRChans            int
	pendingVWRChansCap         int
	pendingVWRChansIn          int
	vfVMChanCap                int
	vfVMChanIn                 int
	freeTOCBlockChanCap        int
	freeTOCBlockChanIn         int
	pendingTOCBlockChanCap     int
	pendingTOCBlockChanIn      int
	maxValueLocBlockID         uint64
	path                       string
	pathtoc                    string
	workers                    int
	discardInterval            int
	outPullReplicationWorkers  int
	outPullReplicationInterval int
	maxValueSize               uint32
	pageSize                   uint32
	minValueAlloc              int
	writePagesPerWorker        int
	tombstoneAge               int
	valuesFileSize             uint32
	valuesFileReaders          int
	checksumInterval           uint32
	replicationIgnoreRecent    int
	vlmCount                   uint64
	vlmLength                  uint64
	vlmDebugInfo               fmt.Stringer
}

type discardNotification struct {
	enable   bool
	disable  bool
	doneChan chan struct{}
}

type outPullReplicationNotification struct {
	enable   bool
	disable  bool
	doneChan chan struct{}
}

const _GLH_IN_PULL_REPLICATION_MSGS = 128
const _GLH_IN_PULL_REPLICATION_HANDLERS = 40
const _GLH_OUT_PULL_REPLICATION_MSGS = 128
const _GLH_IN_BULK_SET_MSGS = 128
const _GLH_IN_BULK_SET_HANDLERS = 40
const _GLH_OUT_BULK_SET_MSGS = 128
const _GLH_OUT_BULK_SET_MSG_SIZE = 16 * 1024 * 1024

// New creates a DefaultValueStore for use in storing []byte values referenced
// by 128 bit keys.
//
// Note that a lot of buffering, multiple cores, and background processes can
// be in use and therefore DisableDiscard() DisableOutPullReplication()
// DisableWrites() and Flush() should be called prior to the process exiting to
// ensure all processing is done and the buffers are flushed.
//
// You can provide Opt* functions for optional configuration items, such as
// OptWorkers:
//
//  vsWithDefaults := valuestore.New()
//  vsWithOptions := valuestore.New(
//      valuestore.OptWorkers(10),
//      valuestore.OptPageSize(8388608),
//  )
//  opts := valuestore.OptList()
//  if commandLineOptionForWorkers {
//      opts = append(opts, valuestore.OptWorkers(commandLineOptionValue))
//  }
//  vsWithOptionsBuiltUp := valuestore.New(opts...)
func New(opts ...func(*config)) *DefaultValueStore {
	cfg := resolveConfig(opts...)
	vlm := cfg.vlm
	if vlm == nil {
		vlm = valuelocmap.New()
	}
	vs := &DefaultValueStore{
		logCritical:                 cfg.logCritical,
		logError:                    cfg.logError,
		logWarning:                  cfg.logWarning,
		logInfo:                     cfg.logInfo,
		logDebug:                    cfg.logDebug,
		rand:                        cfg.rand,
		valueLocBlocks:              make([]valueLocBlock, math.MaxUint16),
		path:                        cfg.path,
		pathtoc:                     cfg.pathtoc,
		vlm:                         vlm,
		workers:                     cfg.workers,
		discardInterval:             cfg.discardInterval,
		replicationIgnoreRecent:     uint64(cfg.replicationIgnoreRecent) * uint64(time.Second),
		outPullReplicationWorkers:   cfg.outPullReplicationWorkers,
		outPullReplicationIteration: uint16(cfg.rand.Uint32()),
		outPullReplicationInterval:  cfg.outPullReplicationInterval,
		maxValueSize:                uint32(cfg.maxValueSize),
		pageSize:                    uint32(cfg.pageSize),
		minValueAlloc:               cfg.minValueAlloc,
		writePagesPerWorker:         cfg.writePagesPerWorker,
		tombstoneAge:                uint64(cfg.tombstoneAge) * uint64(time.Second),
		valuesFileSize:              uint32(cfg.valuesFileSize),
		valuesFileReaders:           cfg.valuesFileReaders,
		checksumInterval:            uint32(cfg.checksumInterval),
		ring:                        cfg.ring,
	}
	vs.freeableVMChans = make([]chan *valuesMem, vs.workers)
	for i := 0; i < cap(vs.freeableVMChans); i++ {
		vs.freeableVMChans[i] = make(chan *valuesMem, vs.workers)
	}
	vs.freeVMChan = make(chan *valuesMem, vs.workers*vs.writePagesPerWorker)
	vs.freeVWRChans = make([]chan *valueWriteReq, vs.workers)
	vs.pendingVWRChans = make([]chan *valueWriteReq, vs.workers)
	vs.vfVMChan = make(chan *valuesMem, vs.workers)
	vs.freeTOCBlockChan = make(chan []byte, vs.workers*2)
	vs.pendingTOCBlockChan = make(chan []byte, vs.workers)
	vs.flushedChan = make(chan struct{}, 1)
	for i := 0; i < cap(vs.freeVMChan); i++ {
		vm := &valuesMem{
			vs:     vs,
			toc:    make([]byte, 0, vs.pageSize),
			values: make([]byte, 0, vs.pageSize),
		}
		vm.id = vs.addValueLocBlock(vm)
		vs.freeVMChan <- vm
	}
	for i := 0; i < len(vs.freeVWRChans); i++ {
		vs.freeVWRChans[i] = make(chan *valueWriteReq, vs.workers*2)
		for j := 0; j < vs.workers*2; j++ {
			vs.freeVWRChans[i] <- &valueWriteReq{errChan: make(chan error, 1)}
		}
	}
	for i := 0; i < len(vs.pendingVWRChans); i++ {
		vs.pendingVWRChans[i] = make(chan *valueWriteReq)
	}
	for i := 0; i < cap(vs.freeTOCBlockChan); i++ {
		vs.freeTOCBlockChan <- make([]byte, 0, vs.pageSize)
	}
	go vs.tocWriter()
	go vs.vfWriter()
	for i := 0; i < len(vs.freeableVMChans); i++ {
		go vs.memClearer(vs.freeableVMChans[i])
	}
	for i := 0; i < len(vs.pendingVWRChans); i++ {
		go vs.memWriter(vs.pendingVWRChans[i])
	}
	vs.recovery()
	if vs.ring != nil {
		vs.ring.SetMsgHandler(ring.MSG_PULL_REPLICATION, vs.newInPullReplicationMsg)
		vs.inPullReplicationChan = make(chan *pullReplicationMsg, _GLH_IN_PULL_REPLICATION_MSGS)
		vs.freeInPullReplicationChan = make(chan *pullReplicationMsg, _GLH_IN_PULL_REPLICATION_MSGS)
		for i := 0; i < cap(vs.freeInPullReplicationChan); i++ {
			vs.freeInPullReplicationChan <- &pullReplicationMsg{
				vs:     vs,
				header: make([]byte, ktBloomFilterHeaderBytes+pullReplicationMsgHeaderBytes),
			}
		}
		for i := 0; i < _GLH_IN_PULL_REPLICATION_HANDLERS; i++ {
			go vs.inPullReplication()
		}
		vs.outPullReplicationChan = make(chan *pullReplicationMsg, _GLH_OUT_PULL_REPLICATION_MSGS)
		vs.ktbfs = []*ktBloomFilter{newKTBloomFilter(_GLH_BLOOM_FILTER_N, _GLH_BLOOM_FILTER_P, 0)}
		for i := 0; i < cap(vs.outPullReplicationChan); i++ {
			vs.outPullReplicationChan <- &pullReplicationMsg{
				vs:     vs,
				header: make([]byte, ktBloomFilterHeaderBytes+pullReplicationMsgHeaderBytes),
				body:   make([]byte, len(vs.ktbfs[0].bits)),
			}
		}
		vs.ring.SetMsgHandler(ring.MSG_BULK_SET, vs.newInBulkSetMsg)
		vs.inBulkSetChan = make(chan *bulkSetMsg, _GLH_IN_BULK_SET_MSGS)
		vs.freeInBulkSetChan = make(chan *bulkSetMsg, _GLH_IN_BULK_SET_MSGS)
		for i := 0; i < cap(vs.freeInBulkSetChan); i++ {
			vs.freeInBulkSetChan <- &bulkSetMsg{vs: vs}
		}
		for i := 0; i < _GLH_IN_BULK_SET_HANDLERS; i++ {
			go vs.inBulkSet()
		}
		vs.outBulkSetChan = make(chan *bulkSetMsg, _GLH_OUT_BULK_SET_MSGS)
		for i := 0; i < cap(vs.outBulkSetChan); i++ {
			vs.outBulkSetChan <- &bulkSetMsg{
				vs:   vs,
				body: make([]byte, _GLH_OUT_BULK_SET_MSG_SIZE),
			}
		}
	}
	vs.discardNotifyChan = make(chan *discardNotification, 1)
	go vs.discardLauncher()
	vs.outPullReplicationNotifyChan = make(chan *outPullReplicationNotification, 1)
	go vs.outPullReplicationLauncher()
	return vs
}

// MaxValueSize returns the maximum length of a value the ValueStore can
// accept.
func (vs *DefaultValueStore) MaxValueSize() uint32 {
	return vs.maxValueSize
}

// DisableWrites will cause any incoming Write or Delete requests to respond
// with ErrDisabled until EnableWrites is called.
func (vs *DefaultValueStore) DisableWrites() {
	for _, c := range vs.pendingVWRChans {
		c <- disableValueWriteReq
	}
}

// EnableWrites will resume accepting incoming Write and Delete requests.
func (vs *DefaultValueStore) EnableWrites() {
	for _, c := range vs.pendingVWRChans {
		c <- enableValueWriteReq
	}
}

// DisableDiscard will stop any discard passes until EnableDiscard is called. A
// discard pass removes expired tombstones (deletion markers).
func (vs *DefaultValueStore) DisableDiscard() {
	atomic.StoreUint32(&vs.discardAbort, 1)
	c := make(chan struct{}, 1)
	vs.discardNotifyChan <- &discardNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableDiscard will resume discard passes. A discard pass removes expired
// tombstones (deletion markers).
func (vs *DefaultValueStore) EnableDiscard() {
	c := make(chan struct{}, 1)
	vs.discardNotifyChan <- &discardNotification{
		enable:   true,
		doneChan: c,
	}
	<-c
}

// DisableOutPullReplication will stop any outgoing pull replication requests
// until EnableOutPullReplication is called.
func (vs *DefaultValueStore) DisableOutPullReplication() {
	atomic.StoreUint32(&vs.outPullReplicationAbort, 1)
	c := make(chan struct{}, 1)
	vs.outPullReplicationNotifyChan <- &outPullReplicationNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableOutPullReplication will resume outgoing pull replication requests.
func (vs *DefaultValueStore) EnableOutPullReplication() {
	c := make(chan struct{}, 1)
	vs.outPullReplicationNotifyChan <- &outPullReplicationNotification{
		enable:   true,
		doneChan: c,
	}
	<-c
}

// Flush will ensure buffered data (at the time of the call) is written to
// disk.
func (vs *DefaultValueStore) Flush() {
	for _, c := range vs.pendingVWRChans {
		c <- flushValueWriteReq
	}
	<-vs.flushedChan
}

// Lookup will return timestamp, length, err for keyA, keyB.
//
// Note that err == ErrNotFound with timestamp == 0 indicates keyA, keyB was
// not known at all whereas err == ErrNotFound with timestamp != 0 (also
// timestamp & 1 == 1) indicates keyA, keyB was known and had a deletion marker
// (aka tombstone).
func (vs *DefaultValueStore) Lookup(keyA uint64, keyB uint64) (uint64, uint32, error) {
	timestamp, id, _, length := vs.vlm.Get(keyA, keyB)
	if id == 0 || timestamp&1 == 1 {
		return timestamp, 0, ErrNotFound
	}
	return timestamp, length, nil
}

// Read will return timestamp, value, err for keyA, keyB; if an incoming value
// is provided, the read value will be appended to it and the whole returned
// (useful to reuse an existing []byte).
//
// Note that err == ErrNotFound with timestamp == 0 indicates keyA, keyB was
// not known at all whereas err == ErrNotFound with timestamp != 0 (also
// timestamp & 1 == 1) indicates keyA, keyB was known and had a deletion marker
// (aka tombstone).
func (vs *DefaultValueStore) Read(keyA uint64, keyB uint64, value []byte) (uint64, []byte, error) {
	timestamp, id, offset, length := vs.vlm.Get(keyA, keyB)
	if id == 0 || timestamp&1 == 1 {
		return timestamp, value, ErrNotFound
	}
	return vs.valueLocBlock(id).read(keyA, keyB, timestamp, offset, length, value)
}

// Write stores timestamp & 0xfffffffffffffffe (lowest bit zeroed), value for
// keyA, keyB or returns any error; a newer timestamp already in place is not
// reported as an error.
func (vs *DefaultValueStore) Write(keyA uint64, keyB uint64, timestamp uint64, value []byte) (uint64, error) {
	i := int(keyA>>1) % len(vs.freeVWRChans)
	vwr := <-vs.freeVWRChans[i]
	vwr.keyA = keyA
	vwr.keyB = keyB
	vwr.timestamp = timestamp & 0xfffffffffffffffe
	vwr.value = value
	vs.pendingVWRChans[i] <- vwr
	err := <-vwr.errChan
	previousTimestamp := vwr.timestamp
	vwr.value = nil
	vs.freeVWRChans[i] <- vwr
	return previousTimestamp, err
}

// Delete stores timestamp | 1 for keyA, keyB or returns any error; a newer
// timestamp already in place is not reported as an error.
func (vs *DefaultValueStore) Delete(keyA uint64, keyB uint64, timestamp uint64) (uint64, error) {
	i := int(keyA>>1) % len(vs.freeVWRChans)
	vwr := <-vs.freeVWRChans[i]
	vwr.keyA = keyA
	vwr.keyB = keyB
	vwr.timestamp = timestamp | 1
	vwr.value = nil
	vs.pendingVWRChans[i] <- vwr
	err := <-vwr.errChan
	previousTimestamp := vwr.timestamp
	vs.freeVWRChans[i] <- vwr
	return previousTimestamp, err
}

// GatherStats returns overall information about the state of the ValueStore.
func (vs *DefaultValueStore) GatherStats(debug bool) (uint64, uint64, fmt.Stringer) {
	stats := &valueStoreStats{}
	if debug {
		stats.debug = debug
		for i := 0; i < len(vs.freeableVMChans); i++ {
			stats.freeableVMChansCap += cap(vs.freeableVMChans[i])
			stats.freeableVMChansIn += len(vs.freeableVMChans[i])
		}
		stats.freeVMChanCap = cap(vs.freeVMChan)
		stats.freeVMChanIn = len(vs.freeVMChan)
		stats.freeVWRChans = len(vs.freeVWRChans)
		for i := 0; i < len(vs.freeVWRChans); i++ {
			stats.freeVWRChansCap += cap(vs.freeVWRChans[i])
			stats.freeVWRChansIn += len(vs.freeVWRChans[i])
		}
		stats.pendingVWRChans = len(vs.pendingVWRChans)
		for i := 0; i < len(vs.pendingVWRChans); i++ {
			stats.pendingVWRChansCap += cap(vs.pendingVWRChans[i])
			stats.pendingVWRChansIn += len(vs.pendingVWRChans[i])
		}
		stats.vfVMChanCap = cap(vs.vfVMChan)
		stats.vfVMChanIn = len(vs.vfVMChan)
		stats.freeTOCBlockChanCap = cap(vs.freeTOCBlockChan)
		stats.freeTOCBlockChanIn = len(vs.freeTOCBlockChan)
		stats.pendingTOCBlockChanCap = cap(vs.pendingTOCBlockChan)
		stats.pendingTOCBlockChanIn = len(vs.pendingTOCBlockChan)
		stats.maxValueLocBlockID = atomic.LoadUint64(&vs.valueLocBlockIDer)
		stats.path = vs.path
		stats.pathtoc = vs.pathtoc
		stats.workers = vs.workers
		stats.discardInterval = vs.discardInterval
		stats.outPullReplicationWorkers = vs.outPullReplicationWorkers
		stats.outPullReplicationInterval = vs.outPullReplicationInterval
		stats.maxValueSize = vs.maxValueSize
		stats.pageSize = vs.pageSize
		stats.minValueAlloc = vs.minValueAlloc
		stats.writePagesPerWorker = vs.writePagesPerWorker
		stats.tombstoneAge = int(vs.tombstoneAge / uint64(time.Second))
		stats.valuesFileSize = vs.valuesFileSize
		stats.valuesFileReaders = vs.valuesFileReaders
		stats.checksumInterval = vs.checksumInterval
		stats.replicationIgnoreRecent = int(vs.replicationIgnoreRecent / uint64(time.Second))
		stats.vlmCount, stats.vlmLength, stats.vlmDebugInfo = vs.vlm.GatherStats(true)
	} else {
		stats.vlmCount, stats.vlmLength, stats.vlmDebugInfo = vs.vlm.GatherStats(false)
	}
	return stats.vlmCount, stats.vlmLength, stats
}

func (vs *DefaultValueStore) valueLocBlock(valueLocBlockID uint32) valueLocBlock {
	return vs.valueLocBlocks[valueLocBlockID]
}

func (vs *DefaultValueStore) addValueLocBlock(block valueLocBlock) uint32 {
	id := atomic.AddUint64(&vs.valueLocBlockIDer, 1)
	if id >= math.MaxUint32 {
		panic("too many valueLocBlocks")
	}
	vs.valueLocBlocks[id] = block
	return uint32(id)
}

func (vs *DefaultValueStore) memClearer(freeableVMChan chan *valuesMem) {
	var tb []byte
	var tbTS int64
	var tbOffset int
	for {
		vm := <-freeableVMChan
		if vm == flushValuesMem {
			if tb != nil {
				vs.pendingTOCBlockChan <- tb
				tb = nil
			}
			vs.pendingTOCBlockChan <- nil
			continue
		}
		vf := vs.valueLocBlock(vm.vfID)
		if tb != nil && tbTS != vf.timestamp() {
			vs.pendingTOCBlockChan <- tb
			tb = nil
		}
		for vmTOCOffset := 0; vmTOCOffset < len(vm.toc); vmTOCOffset += 32 {
			keyA := binary.BigEndian.Uint64(vm.toc[vmTOCOffset:])
			keyB := binary.BigEndian.Uint64(vm.toc[vmTOCOffset+8:])
			timestamp := binary.BigEndian.Uint64(vm.toc[vmTOCOffset+16:])
			vmMemOffset := binary.BigEndian.Uint32(vm.toc[vmTOCOffset+24:])
			length := binary.BigEndian.Uint32(vm.toc[vmTOCOffset+28:])
			previousTimestamp := vs.vlm.Set(keyA, keyB, timestamp, vm.vfID, vm.vfOffset+vmMemOffset, length, true)
			if previousTimestamp != timestamp {
				continue
			}
			if tb != nil && tbOffset+32 > cap(tb) {
				vs.pendingTOCBlockChan <- tb
				tb = nil
			}
			if tb == nil {
				tb = <-vs.freeTOCBlockChan
				tbTS = vf.timestamp()
				tb = tb[:8]
				binary.BigEndian.PutUint64(tb, uint64(tbTS))
				tbOffset = 8
			}
			tb = tb[:tbOffset+32]
			binary.BigEndian.PutUint64(tb[tbOffset:], keyA)
			binary.BigEndian.PutUint64(tb[tbOffset+8:], keyB)
			binary.BigEndian.PutUint64(tb[tbOffset+16:], timestamp)
			binary.BigEndian.PutUint32(tb[tbOffset+24:], vm.vfOffset+vmMemOffset)
			binary.BigEndian.PutUint32(tb[tbOffset+28:], length)
			tbOffset += 32
		}
		vm.discardLock.Lock()
		vm.vfID = 0
		vm.vfOffset = 0
		vm.toc = vm.toc[:0]
		vm.values = vm.values[:0]
		vm.discardLock.Unlock()
		vs.freeVMChan <- vm
	}
}

func (vs *DefaultValueStore) memWriter(pendingVWRChan chan *valueWriteReq) {
	var enabled bool
	var vm *valuesMem
	var vmTOCOffset int
	var vmMemOffset int
	for {
		vwr := <-pendingVWRChan
		if vwr == enableValueWriteReq {
			enabled = true
			continue
		}
		if vwr == disableValueWriteReq {
			enabled = false
			continue
		}
		if vwr == flushValueWriteReq {
			if vm != nil && len(vm.toc) > 0 {
				vs.vfVMChan <- vm
				vm = nil
			}
			vs.vfVMChan <- flushValuesMem
			continue
		}
		if !enabled {
			vwr.errChan <- ErrDisabled
			continue
		}
		length := len(vwr.value)
		if length > int(vs.maxValueSize) {
			vwr.errChan <- fmt.Errorf("value length of %d > %d", length, vs.maxValueSize)
			continue
		}
		alloc := length
		if alloc < vs.minValueAlloc {
			alloc = vs.minValueAlloc
		}
		if vm != nil && (vmTOCOffset+32 > cap(vm.toc) || vmMemOffset+alloc > cap(vm.values)) {
			vs.vfVMChan <- vm
			vm = nil
		}
		if vm == nil {
			vm = <-vs.freeVMChan
			vmTOCOffset = 0
			vmMemOffset = 0
		}
		vm.discardLock.Lock()
		vm.values = vm.values[:vmMemOffset+alloc]
		vm.discardLock.Unlock()
		copy(vm.values[vmMemOffset:], vwr.value)
		if alloc > length {
			for i, j := vmMemOffset+length, vmMemOffset+alloc; i < j; i++ {
				vm.values[i] = 0
			}
		}
		previousTimestamp := vs.vlm.Set(vwr.keyA, vwr.keyB, vwr.timestamp, vm.id, uint32(vmMemOffset), uint32(length), false)
		if previousTimestamp < vwr.timestamp {
			vm.toc = vm.toc[:vmTOCOffset+32]
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset:], vwr.keyA)
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+8:], vwr.keyB)
			binary.BigEndian.PutUint64(vm.toc[vmTOCOffset+16:], vwr.timestamp)
			binary.BigEndian.PutUint32(vm.toc[vmTOCOffset+24:], uint32(vmMemOffset))
			binary.BigEndian.PutUint32(vm.toc[vmTOCOffset+28:], uint32(length))
			vmTOCOffset += 32
			vmMemOffset += alloc
		} else {
			vm.discardLock.Lock()
			vm.values = vm.values[:vmMemOffset]
			vm.discardLock.Unlock()
		}
		vwr.timestamp = previousTimestamp
		vwr.errChan <- nil
	}
}

func (vs *DefaultValueStore) vfWriter() {
	var vf *valuesFile
	memWritersFlushLeft := len(vs.pendingVWRChans)
	var tocLen uint64
	var valueLen uint64
	for {
		vm := <-vs.vfVMChan
		if vm == flushValuesMem {
			memWritersFlushLeft--
			if memWritersFlushLeft > 0 {
				continue
			}
			if vf != nil {
				vf.close()
				vf = nil
			}
			for i := 0; i < len(vs.freeableVMChans); i++ {
				vs.freeableVMChans[i] <- flushValuesMem
			}
			memWritersFlushLeft = len(vs.pendingVWRChans)
			continue
		}
		if vf != nil && (tocLen+uint64(len(vm.toc)) >= uint64(vs.valuesFileSize) || valueLen+uint64(len(vm.values)) > uint64(vs.valuesFileSize)) {
			vf.close()
			vf = nil
		}
		if vf == nil {
			vf = createValuesFile(vs)
			tocLen = 32
			valueLen = 32
		}
		vf.write(vm)
		tocLen += uint64(len(vm.toc))
		valueLen += uint64(len(vm.values))
	}
}

func (vs *DefaultValueStore) tocWriter() {
	memClearersFlushLeft := len(vs.freeableVMChans)
	var btsA uint64
	var writerA io.WriteCloser
	var offsetA uint64
	var btsB uint64
	var writerB io.WriteCloser
	var offsetB uint64
	head := []byte("VALUESTORETOC v0                ")
	binary.BigEndian.PutUint32(head[28:], uint32(vs.checksumInterval))
	term := make([]byte, 16)
	copy(term[12:], "TERM")
	for {
		t := <-vs.pendingTOCBlockChan
		if t == nil {
			memClearersFlushLeft--
			if memClearersFlushLeft > 0 {
				continue
			}
			if writerB != nil {
				binary.BigEndian.PutUint64(term[4:], offsetB)
				if _, err := writerB.Write(term); err != nil {
					panic(err)
				}
				if err := writerB.Close(); err != nil {
					panic(err)
				}
				writerB = nil
				btsB = 0
				offsetB = 0
			}
			if writerA != nil {
				binary.BigEndian.PutUint64(term[4:], offsetA)
				if _, err := writerA.Write(term); err != nil {
					panic(err)
				}
				if err := writerA.Close(); err != nil {
					panic(err)
				}
				writerA = nil
				btsA = 0
				offsetA = 0
			}
			vs.flushedChan <- struct{}{}
			memClearersFlushLeft = len(vs.freeableVMChans)
			continue
		}
		if len(t) > 8 {
			bts := binary.BigEndian.Uint64(t)
			switch bts {
			case btsA:
				if _, err := writerA.Write(t[8:]); err != nil {
					panic(err)
				}
				offsetA += uint64(len(t) - 8)
			case btsB:
				if _, err := writerB.Write(t[8:]); err != nil {
					panic(err)
				}
				offsetB += uint64(len(t) - 8)
			default:
				// An assumption is made here: If the timestamp for this toc
				// block doesn't match the last two seen timestamps then we
				// expect no more toc blocks for the oldest timestamp and can
				// close that toc file.
				if writerB != nil {
					binary.BigEndian.PutUint64(term[4:], offsetB)
					if _, err := writerB.Write(term); err != nil {
						panic(err)
					}
					if err := writerB.Close(); err != nil {
						panic(err)
					}
				}
				btsB = btsA
				writerB = writerA
				offsetB = offsetA
				btsA = bts
				fp, err := os.Create(path.Join(vs.pathtoc, fmt.Sprintf("%d.valuestoc", bts)))
				if err != nil {
					panic(err)
				}
				writerA = brimutil.NewMultiCoreChecksummedWriter(fp, int(vs.checksumInterval), murmur3.New32, vs.workers)
				if _, err := writerA.Write(head); err != nil {
					panic(err)
				}
				if _, err := writerA.Write(t[8:]); err != nil {
					panic(err)
				}
				offsetA = 32 + uint64(len(t)-8)
			}
		}
		vs.freeTOCBlockChan <- t[:0]
	}
}

func (vs *DefaultValueStore) recovery() {
	start := time.Now()
	dfp, err := os.Open(vs.pathtoc)
	if err != nil {
		panic(err)
	}
	names, err := dfp.Readdirnames(-1)
	if err != nil {
		panic(err)
	}
	sort.Strings(names)
	fromDiskCount := 0
	count := int64(0)
	type writeReq struct {
		keyA      uint64
		keyB      uint64
		timestamp uint64
		blockID   uint32
		offset    uint32
		length    uint32
	}
	pendingChan := make(chan []writeReq, vs.workers)
	freeChan := make(chan []writeReq, cap(pendingChan)*2)
	for i := 0; i < cap(freeChan); i++ {
		freeChan <- make([]writeReq, 0, 65536)
	}
	wg := &sync.WaitGroup{}
	wg.Add(cap(pendingChan))
	for i := 0; i < cap(pendingChan); i++ {
		go func() {
			for {
				wrs := <-pendingChan
				if wrs == nil {
					break
				}
				for i := len(wrs) - 1; i >= 0; i-- {
					wr := &wrs[i]
					if vs.vlm.Set(wr.keyA, wr.keyB, wr.timestamp, wr.blockID, wr.offset, wr.length, false) < wr.timestamp {
						atomic.AddInt64(&count, 1)
					}
				}
				freeChan <- wrs[:0]
			}
			wg.Done()
		}()
	}
	wrs := <-freeChan
	wix := 0
	maxwix := cap(wrs) - 1
	wrs = wrs[:maxwix+1]
	for i := len(names) - 1; i >= 0; i-- {
		if !strings.HasSuffix(names[i], ".valuestoc") {
			continue
		}
		bts := int64(0)
		if bts, err = strconv.ParseInt(names[i][:len(names[i])-len(".valuestoc")], 10, 64); err != nil {
			vs.logError.Printf("bad timestamp name: %#v\n", names[i])
			continue
		}
		if bts == 0 {
			vs.logError.Printf("bad timestamp name: %#v\n", names[i])
			continue
		}
		vf := newValuesFile(vs, bts)
		fp, err := os.Open(path.Join(vs.pathtoc, names[i]))
		if err != nil {
			vs.logError.Printf("error opening %s: %s\n", names[i], err)
			continue
		}
		buf := make([]byte, vs.checksumInterval+4)
		checksumFailures := 0
		overflow := make([]byte, 0, 32)
		first := true
		terminated := false
		for {
			n, err := io.ReadFull(fp, buf)
			if n < 4 {
				if err != io.EOF && err != io.ErrUnexpectedEOF {
					vs.logError.Printf("error reading %s: %s\n", names[i], err)
				}
				break
			}
			n -= 4
			if murmur3.Sum32(buf[:n]) != binary.BigEndian.Uint32(buf[n:]) {
				checksumFailures++
			} else {
				i := 0
				if first {
					if !bytes.Equal(buf[:28], []byte("VALUESTORETOC v0            ")) {
						vs.logError.Printf("bad header: %s\n", names[i])
						break
					}
					if binary.BigEndian.Uint32(buf[28:]) != vs.checksumInterval {
						vs.logError.Printf("bad header checksum interval: %s\n", names[i])
						break
					}
					i += 32
					first = false
				}
				if n < int(vs.checksumInterval) {
					if binary.BigEndian.Uint32(buf[n-16:]) != 0 {
						vs.logError.Printf("bad terminator size marker: %s\n", names[i])
						break
					}
					if !bytes.Equal(buf[n-4:n], []byte("TERM")) {
						vs.logError.Printf("bad terminator: %s\n", names[i])
						break
					}
					n -= 16
					terminated = true
				}
				if len(overflow) > 0 {
					i += 32 - len(overflow)
					overflow = append(overflow, buf[i-32+len(overflow):i]...)
					if wrs == nil {
						wrs = (<-freeChan)[:maxwix+1]
						wix = 0
					}
					wr := &wrs[wix]
					wr.keyA = binary.BigEndian.Uint64(overflow)
					wr.keyB = binary.BigEndian.Uint64(overflow[8:])
					wr.timestamp = binary.BigEndian.Uint64(overflow[16:])
					wr.blockID = vf.id
					wr.offset = binary.BigEndian.Uint32(overflow[24:])
					wr.length = binary.BigEndian.Uint32(overflow[28:])
					wix++
					if wix > maxwix {
						pendingChan <- wrs
						wrs = nil
					}
					fromDiskCount++
					overflow = overflow[:0]
				}
				for ; i+32 <= n; i += 32 {
					if wrs == nil {
						wrs = (<-freeChan)[:maxwix+1]
						wix = 0
					}
					wr := &wrs[wix]
					wr.keyA = binary.BigEndian.Uint64(buf[i:])
					wr.keyB = binary.BigEndian.Uint64(buf[i+8:])
					wr.timestamp = binary.BigEndian.Uint64(buf[i+16:])
					wr.blockID = vf.id
					wr.offset = binary.BigEndian.Uint32(buf[i+24:])
					wr.length = binary.BigEndian.Uint32(buf[i+28:])
					wix++
					if wix > maxwix {
						pendingChan <- wrs
						wrs = nil
					}
					fromDiskCount++
				}
				if i != n {
					overflow = overflow[:n-i]
					copy(overflow, buf[i:])
				}
			}
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				vs.logError.Printf("error reading %s: %s\n", names[i], err)
				break
			}
		}
		fp.Close()
		if !terminated {
			vs.logError.Printf("early end of file: %s\n", names[i])
		}
		if checksumFailures > 0 {
			vs.logWarning.Printf("%d checksum failures for %s\n", checksumFailures, names[i])
		}
	}
	if wix > 0 {
		pendingChan <- wrs[:wix]
	}
	for i := 0; i < cap(pendingChan); i++ {
		pendingChan <- nil
	}
	wg.Wait()
	if fromDiskCount > 0 {
		dur := time.Now().Sub(start)
		valueCount, valueLength, _ := vs.GatherStats(false)
		vs.logInfo.Printf("%d key locations loaded in %s, %.0f/s; %d caused change; %d resulting locations referencing %d bytes.\n", fromDiskCount, dur, float64(fromDiskCount)/(float64(dur)/float64(time.Second)), count, valueCount, valueLength)
	}
}

func (vs *DefaultValueStore) inPullReplication() {
	k := make([]uint64, 2*1024*1024)
	v := make([]byte, vs.maxValueSize)
	for {
		prm := <-vs.inPullReplicationChan
		k = k[:0]
		cutoff := prm.timestampCutoff()
		tombstoneCutoff := uint64(time.Now().UnixNano()) - vs.tombstoneAge
		ktbf := prm.ktBloomFilter()
		l := int64(_GLH_OUT_BULK_SET_MSG_SIZE)
		vs.vlm.ScanCallback(prm.rangeStart(), prm.rangeStop(), func(keyA uint64, keyB uint64, timestamp uint64, length uint32) {
			if l > 0 && timestamp < cutoff && !ktbf.mayHave(keyA, keyB, timestamp) {
				if timestamp&1 == 0 || timestamp >= tombstoneCutoff {
					k = append(k, keyA, keyB)
					// bsm: keyA:8, keyB:8, timestamp:8, length:4, value:n
					l -= 28 + int64(length)
				}
			}
		})
		nodeID := prm.nodeID()
		vs.freeInPullReplicationChan <- prm
		if len(k) > 0 {
			bsm := vs.newOutBulkSetMsg()
			var t uint64
			var err error
			for i := 0; i < len(k); i += 2 {
				t, v, err = vs.Read(k[i], k[i+1], v[:0])
				if err == ErrNotFound {
					if t == 0 {
						continue
					}
				} else if err != nil {
					continue
				}
				if !bsm.add(k[i], k[i+1], t, v) {
					break
				}
			}
			vs.ring.MsgToNode(nodeID, bsm)
		}
	}
}

func (vs *DefaultValueStore) inBulkSet() {
	for {
		bsm := <-vs.inBulkSetChan
		bsm.valueStore(vs)
		vs.freeInBulkSetChan <- bsm
	}
}

func (stats *valueStoreStats) String() string {
	if stats.debug {
		return brimtext.Align([][]string{
			[]string{"freeableVMChansCap", fmt.Sprintf("%d", stats.freeableVMChansCap)},
			[]string{"freeableVMChansIn", fmt.Sprintf("%d", stats.freeableVMChansIn)},
			[]string{"freeVMChanCap", fmt.Sprintf("%d", stats.freeVMChanCap)},
			[]string{"freeVMChanIn", fmt.Sprintf("%d", stats.freeVMChanIn)},
			[]string{"freeVWRChans", fmt.Sprintf("%d", stats.freeVWRChans)},
			[]string{"freeVWRChansCap", fmt.Sprintf("%d", stats.freeVWRChansCap)},
			[]string{"freeVWRChansIn", fmt.Sprintf("%d", stats.freeVWRChansIn)},
			[]string{"pendingVWRChans", fmt.Sprintf("%d", stats.pendingVWRChans)},
			[]string{"pendingVWRChansCap", fmt.Sprintf("%d", stats.pendingVWRChansCap)},
			[]string{"pendingVWRChansIn", fmt.Sprintf("%d", stats.pendingVWRChansIn)},
			[]string{"vfVMChanCap", fmt.Sprintf("%d", stats.vfVMChanCap)},
			[]string{"vfVMChanIn", fmt.Sprintf("%d", stats.vfVMChanIn)},
			[]string{"freeTOCBlockChanCap", fmt.Sprintf("%d", stats.freeTOCBlockChanCap)},
			[]string{"freeTOCBlockChanIn", fmt.Sprintf("%d", stats.freeTOCBlockChanIn)},
			[]string{"pendingTOCBlockChanCap", fmt.Sprintf("%d", stats.pendingTOCBlockChanCap)},
			[]string{"pendingTOCBlockChanIn", fmt.Sprintf("%d", stats.pendingTOCBlockChanIn)},
			[]string{"maxValueLocBlockID", fmt.Sprintf("%d", stats.maxValueLocBlockID)},
			[]string{"path", stats.path},
			[]string{"pathtoc", stats.pathtoc},
			[]string{"workers", fmt.Sprintf("%d", stats.workers)},
			[]string{"discardInterval", fmt.Sprintf("%d", stats.discardInterval)},
			[]string{"outPullReplicationWorkers", fmt.Sprintf("%d", stats.outPullReplicationWorkers)},
			[]string{"outPullReplicationInterval", fmt.Sprintf("%d", stats.outPullReplicationInterval)},
			[]string{"maxValueSize", fmt.Sprintf("%d", stats.maxValueSize)},
			[]string{"pageSize", fmt.Sprintf("%d", stats.pageSize)},
			[]string{"minValueAlloc", fmt.Sprintf("%d", stats.minValueAlloc)},
			[]string{"writePagesPerWorker", fmt.Sprintf("%d", stats.writePagesPerWorker)},
			[]string{"tombstoneAge", fmt.Sprintf("%d", stats.tombstoneAge)},
			[]string{"valuesFileSize", fmt.Sprintf("%d", stats.valuesFileSize)},
			[]string{"valuesFileReaders", fmt.Sprintf("%d", stats.valuesFileReaders)},
			[]string{"checksumInterval", fmt.Sprintf("%d", stats.checksumInterval)},
			[]string{"replicationIgnoreRecent", fmt.Sprintf("%d", stats.replicationIgnoreRecent)},
			[]string{"vlmCount", fmt.Sprintf("%d", stats.vlmCount)},
			[]string{"vlmLength", fmt.Sprintf("%d", stats.vlmLength)},
			[]string{"vlmDebugInfo", stats.vlmDebugInfo.String()},
		}, nil)
	} else {
		return brimtext.Align([][]string{
			[]string{"vlmCount", fmt.Sprintf("%d", stats.vlmCount)},
			[]string{"vlmLength", fmt.Sprintf("%d", stats.vlmLength)},
		}, nil)
	}
}

// DiscardPass will immediately execute a pass to discard expired tombstones
// (deletion markers) rather than waiting for the next interval. If a pass is
// currently executing, it will be stopped and restarted so that a call to this
// function ensures one complete pass occurs.
func (vs *DefaultValueStore) DiscardPass() {
	atomic.StoreUint32(&vs.discardAbort, 1)
	c := make(chan struct{}, 1)
	vs.discardNotifyChan <- &discardNotification{doneChan: c}
	<-c
}

func (vs *DefaultValueStore) discardLauncher() {
	var enabled bool
	interval := float64(vs.discardInterval) * float64(time.Second)
	nextRun := time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
	for {
		var notification *discardNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-vs.discardNotifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-vs.discardNotifyChan:
			default:
			}
		}
		nextRun = time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
		if notification != nil {
			if notification.enable {
				enabled = true
				notification.doneChan <- struct{}{}
				continue
			}
			if notification.disable {
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&vs.discardAbort, 0)
			vs.discardPass()
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&vs.discardAbort, 0)
			vs.discardPass()
		}
	}
}

func (vs *DefaultValueStore) discardPass() {
	if vs.logDebug != nil {
		begin := time.Now()
		defer vs.logDebug.Printf("discard pass took %s", time.Now().Sub(begin))
	}
	vs.vlm.DiscardTombstones(uint64(time.Now().UnixNano()) - vs.tombstoneAge)
}

// OutPullReplicationPass will immediately execute an outgoing pull replication
// pass rather than waiting for the next interval. If a pass is currently
// executing, it will be stopped and restarted so that a call to this function
// ensures one complete pass occurs. Note that this pass will send the outgoing
// pull replication requests, but all the responses will almost certainly not
// have been received when this function returns. These requests are stateless,
// and so synchronization at that level is not possible.
func (vs *DefaultValueStore) OutPullReplicationPass() {
	atomic.StoreUint32(&vs.outPullReplicationAbort, 1)
	c := make(chan struct{}, 1)
	vs.outPullReplicationNotifyChan <- &outPullReplicationNotification{doneChan: c}
	<-c
}

func (vs *DefaultValueStore) outPullReplicationLauncher() {
	var enabled bool
	interval := float64(vs.outPullReplicationInterval) * float64(time.Second)
	nextRun := time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
	for {
		var notification *outPullReplicationNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-vs.outPullReplicationNotifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-vs.outPullReplicationNotifyChan:
			default:
			}
		}
		nextRun = time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
		if notification != nil {
			if notification.enable {
				enabled = true
				notification.doneChan <- struct{}{}
				continue
			}
			if notification.disable {
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&vs.outPullReplicationAbort, 0)
			vs.outPullReplicationPass()
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&vs.outPullReplicationAbort, 0)
			vs.outPullReplicationPass()
		}
	}
}

const _GLH_BLOOM_FILTER_N = 1000000
const _GLH_BLOOM_FILTER_P = 0.001

func (vs *DefaultValueStore) outPullReplicationPass() {
	if vs.logDebug != nil {
		begin := time.Now()
		defer vs.logDebug.Printf("out replication pass took %s", time.Now().Sub(begin))
	}
	if vs.ring == nil {
		return
	}
	if vs.outPullReplicationIteration == math.MaxUint16 {
		vs.outPullReplicationIteration = 0
	} else {
		vs.outPullReplicationIteration++
	}
	ringID := vs.ring.ID()
	partitionPower := vs.ring.PartitionPower()
	partitions := uint32(1) << partitionPower
	for len(vs.ktbfs) < vs.outPullReplicationWorkers {
		vs.ktbfs = append(vs.ktbfs, newKTBloomFilter(_GLH_BLOOM_FILTER_N, _GLH_BLOOM_FILTER_P, 0))
	}
	f := func(p uint32, ktbf *ktBloomFilter) {
		start := uint64(p) << uint64(64-partitionPower)
		stop := start + (uint64(1)<<(64-partitionPower) - 1)
		pullSize := uint64(1) << (64 - partitionPower)
		for vs.vlm.ScanCount(start, start+(pullSize-1), _GLH_BLOOM_FILTER_N) >= _GLH_BLOOM_FILTER_N {
			pullSize /= 2
		}
		cutoff := uint64(time.Now().UnixNano()) - vs.replicationIgnoreRecent
		tombstoneCutoff := uint64(time.Now().UnixNano()) - vs.tombstoneAge
		substart := start
		substop := start + (pullSize - 1)
		for {
			ktbf.reset(vs.outPullReplicationIteration)
			vs.vlm.ScanCallback(substart, substop, func(keyA uint64, keyB uint64, timestamp uint64, length uint32) {
				if timestamp < cutoff {
					if timestamp&1 == 0 || timestamp >= tombstoneCutoff {
						ktbf.add(keyA, keyB, timestamp)
					}
				}
			})
			if atomic.LoadUint32(&vs.outPullReplicationAbort) != 0 {
				break
			}
			vs.ring.MsgToOtherReplicas(ringID, p, vs.newOutPullReplicationMsg(ringID, p, cutoff, substart, substop, ktbf))
			substart += pullSize
			substop += pullSize
			if substop > stop || substop < stop {
				break
			}
		}
	}
	sp := uint32(vs.rand.Intn(int(partitions)))
	wg := &sync.WaitGroup{}
	wg.Add(vs.outPullReplicationWorkers)
	for g := 0; g < vs.outPullReplicationWorkers; g++ {
		go func(g uint32) {
			ktbf := vs.ktbfs[g]
			for p := uint32(sp + g); p < partitions && atomic.LoadUint32(&vs.outPullReplicationAbort) == 0; p += uint32(vs.outPullReplicationWorkers) {
				if vs.ring.ID() != ringID {
					break
				}
				if vs.ring.Responsible(p) {
					f(p, ktbf)
				}
			}
			for p := uint32(g); p < sp && atomic.LoadUint32(&vs.outPullReplicationAbort) == 0; p += uint32(vs.outPullReplicationWorkers) {
				if vs.ring.ID() != ringID {
					break
				}
				if vs.ring.Responsible(p) {
					f(p, ktbf)
				}
			}
			wg.Done()
		}(uint32(g))
	}
	wg.Wait()
}

const pullReplicationMsgHeaderBytes = 44
const _GLH_IN_PULL_REPLICATION_MSG_TIMEOUT = 300

var toss []byte = make([]byte, 65536)

func (vs *DefaultValueStore) newInPullReplicationMsg(r io.Reader, l uint64) (uint64, error) {
	var prm *pullReplicationMsg
	select {
	case prm = <-vs.freeInPullReplicationChan:
	case <-time.After(_GLH_IN_PULL_REPLICATION_MSG_TIMEOUT * time.Second):
		var n uint64
		var sn int
		var err error
		for n < l {
			sn, err = r.Read(toss)
			n += uint64(sn)
			if err != nil {
				return n, err
			}
		}
		return n, nil
	}
	bl := l - pullReplicationMsgHeaderBytes - uint64(ktBloomFilterHeaderBytes)
	if uint64(cap(prm.body)) < bl {
		prm.body = make([]byte, bl)
	}
	prm.body = prm.body[:bl]
	var n int
	var sn int
	var err error
	for n != len(prm.header) {
		if err != nil {
			return uint64(n), err
		}
		sn, err = r.Read(prm.header[n:])
		n += sn
	}
	n = 0
	for n != len(prm.body) {
		if err != nil {
			return uint64(len(prm.header)) + uint64(n), err
		}
		sn, err = r.Read(prm.body[n:])
		n += sn
	}
	vs.inPullReplicationChan <- prm
	return l, nil
}

func (vs *DefaultValueStore) newOutPullReplicationMsg(ringID uint64, partition uint32, timestampCutoff uint64, rangeStart uint64, rangeStop uint64, ktbf *ktBloomFilter) *pullReplicationMsg {
	prm := <-vs.outPullReplicationChan
	binary.BigEndian.PutUint64(prm.header, vs.ring.NodeID())
	binary.BigEndian.PutUint64(prm.header[8:], ringID)
	binary.BigEndian.PutUint32(prm.header[16:], partition)
	binary.BigEndian.PutUint64(prm.header[20:], timestampCutoff)
	binary.BigEndian.PutUint64(prm.header[28:], rangeStart)
	binary.BigEndian.PutUint64(prm.header[36:], rangeStop)
	ktbf.toMsg(prm, pullReplicationMsgHeaderBytes)
	return prm
}

func (prm *pullReplicationMsg) MsgType() ring.MsgType {
	return ring.MSG_PULL_REPLICATION
}

func (prm *pullReplicationMsg) MsgLength() uint64 {
	return uint64(len(prm.header)) + uint64(len(prm.body))
}

func (prm *pullReplicationMsg) nodeID() uint64 {
	return binary.BigEndian.Uint64(prm.header)
}

func (prm *pullReplicationMsg) ringID() uint64 {
	return binary.BigEndian.Uint64(prm.header[8:])
}

func (prm *pullReplicationMsg) partition() uint32 {
	return binary.BigEndian.Uint32(prm.header[16:])
}

func (prm *pullReplicationMsg) timestampCutoff() uint64 {
	return binary.BigEndian.Uint64(prm.header[20:])
}

func (prm *pullReplicationMsg) rangeStart() uint64 {
	return binary.BigEndian.Uint64(prm.header[28:])
}

func (prm *pullReplicationMsg) rangeStop() uint64 {
	return binary.BigEndian.Uint64(prm.header[36:])
}

func (prm *pullReplicationMsg) ktBloomFilter() *ktBloomFilter {
	return newKTBloomFilterFromMsg(prm, pullReplicationMsgHeaderBytes)
}

func (prm *pullReplicationMsg) WriteContent(w io.Writer) (uint64, error) {
	var n int
	var sn int
	var err error
	sn, err = w.Write(prm.header)
	n += sn
	if err != nil {
		return uint64(n), err
	}
	sn, err = w.Write(prm.body)
	n += sn
	return uint64(n), err
}

func (prm *pullReplicationMsg) Done() {
	prm.vs.outPullReplicationChan <- prm
}

const _GLH_IN_BULK_SET_MSG_TIMEOUT = 300

func (vs *DefaultValueStore) newInBulkSetMsg(r io.Reader, l uint64) (uint64, error) {
	var bsm *bulkSetMsg
	select {
	case bsm = <-vs.freeInBulkSetChan:
	case <-time.After(_GLH_IN_BULK_SET_MSG_TIMEOUT * time.Second):
		var n uint64
		var sn int
		var err error
		for n < l {
			sn, err = r.Read(toss)
			n += uint64(sn)
			if err != nil {
				return n, err
			}
		}
		return n, nil
	}
	if l > uint64(cap(bsm.body)) {
		bsm.body = make([]byte, l)
	}
	bsm.body = bsm.body[:l]
	var n int
	var sn int
	var err error
	for n != len(bsm.body) {
		if err != nil {
			return uint64(n), err
		}
		sn, err = r.Read(bsm.body[n:])
		n += sn
	}
	vs.inBulkSetChan <- bsm
	return l, nil
}

func (vs *DefaultValueStore) newOutBulkSetMsg() *bulkSetMsg {
	bsm := <-vs.outBulkSetChan
	bsm.body = bsm.body[:0]
	return bsm
}

func (bsm *bulkSetMsg) MsgType() ring.MsgType {
	return ring.MSG_BULK_SET
}

func (bsm *bulkSetMsg) MsgLength() uint64 {
	return uint64(len(bsm.body))
}

func (bsm *bulkSetMsg) WriteContent(w io.Writer) (uint64, error) {
	n, err := w.Write(bsm.body)
	return uint64(n), err
}

func (bsm *bulkSetMsg) Done() {
	bsm.vs.outBulkSetChan <- bsm
}

func (bsm *bulkSetMsg) add(keyA uint64, keyB uint64, timestamp uint64, value []byte) bool {
	o := len(bsm.body)
	if o+len(value)+28 >= cap(bsm.body) {
		return false
	}
	bsm.body = bsm.body[:o+len(value)+28]
	binary.BigEndian.PutUint64(bsm.body[o:], keyA)
	binary.BigEndian.PutUint64(bsm.body[o+8:], keyB)
	binary.BigEndian.PutUint64(bsm.body[o+16:], timestamp)
	binary.BigEndian.PutUint32(bsm.body[o+24:], uint32(len(value)))
	copy(bsm.body[o+28:], value)
	return true
}

func (bsm *bulkSetMsg) valueStore(vs *DefaultValueStore) {
	body := bsm.body
	for len(body) > 0 {
		keyA := binary.BigEndian.Uint64(body)
		keyB := binary.BigEndian.Uint64(body[8:])
		timestamp := binary.BigEndian.Uint64(body[16:])
		l := binary.BigEndian.Uint32(body[24:])
		if timestamp&1 == 0 {
			vs.Write(keyA, keyB, timestamp, body[28:28+l])
		} else {
			vs.Delete(keyA, keyB, timestamp)
		}
		body = body[28+l:]
	}
}
