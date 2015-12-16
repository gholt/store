package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spaolacci/murmur3"
)

type groupCompactionState struct {
	interval     int
	workerCount  int
	ageThreshold int64
	abort        uint32
	threshold    float64
	notifyChan   chan *backgroundNotification
}

func (store *DefaultGroupStore) compactionConfig(cfg *GroupStoreConfig) {
	store.compactionState.interval = cfg.CompactionInterval
	store.compactionState.threshold = cfg.CompactionThreshold
	store.compactionState.ageThreshold = int64(cfg.CompactionAgeThreshold * 1000000000)
	store.compactionState.notifyChan = make(chan *backgroundNotification, 1)
	store.compactionState.workerCount = cfg.CompactionWorkers
}

func (store *DefaultGroupStore) compactionLaunch() {
	go store.compactionLauncher()
}

// DisableCompaction will stop any compaction passes until
// EnableCompaction is called. A compaction pass searches for files
// with a percentage of XX deleted entries.
func (store *DefaultGroupStore) DisableCompaction() {
	c := make(chan struct{}, 1)
	store.compactionState.notifyChan <- &backgroundNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableCompaction will resume compaction passes.
// A compaction pass searches for files with a percentage of XX deleted
// entries.
func (store *DefaultGroupStore) EnableCompaction() {
	c := make(chan struct{}, 1)
	store.compactionState.notifyChan <- &backgroundNotification{
		enable:   true,
		doneChan: c,
	}
	<-c
}

// CompactionPass will immediately execute a compaction pass to compact stale files.
func (store *DefaultGroupStore) CompactionPass() {
	atomic.StoreUint32(&store.compactionState.abort, 1)
	c := make(chan struct{}, 1)
	store.compactionState.notifyChan <- &backgroundNotification{doneChan: c}
	<-c
}

func (store *DefaultGroupStore) compactionLauncher() {
	var enabled bool
	interval := float64(store.compactionState.interval) * float64(time.Second)
	store.randMutex.Lock()
	nextRun := time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
	store.randMutex.Unlock()
	for {
		var notification *backgroundNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-store.compactionState.notifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-store.compactionState.notifyChan:
			default:
			}
		}
		store.randMutex.Lock()
		nextRun = time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
		store.randMutex.Unlock()
		if notification != nil {
			if notification.enable {
				enabled = true
				notification.doneChan <- struct{}{}
				continue
			}
			if notification.disable {
				atomic.StoreUint32(&store.compactionState.abort, 1)
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&store.compactionState.abort, 0)
			store.compactionPass()
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&store.compactionState.abort, 0)
			store.compactionPass()
		}
	}
}

type groupCompactionJob struct {
	name             string
	candidateBlockID uint32
}

func (store *DefaultGroupStore) compactionPass() {
	if store.logDebug != nil {
		begin := time.Now()
		defer func() {
			store.logDebug("compaction pass took %s\n", time.Now().Sub(begin))
		}()
	}
	fp, err := os.Open(store.pathtoc)
	if err != nil {
		store.logError("%s\n", err)
		return
	}
	names, err := fp.Readdirnames(-1)
	fp.Close()
	if err != nil {
		store.logError("%s\n", err)
		return
	}
	sort.Strings(names)
	jobChan := make(chan *groupCompactionJob, len(names))
	wg := &sync.WaitGroup{}
	for i := 0; i < store.compactionState.workerCount; i++ {
		wg.Add(1)
		go store.compactionWorker(jobChan, wg)
	}
	for _, name := range names {
		namets, valid := store.compactionCandidate(path.Join(store.pathtoc, name))
		if valid {
			jobChan <- &groupCompactionJob{path.Join(store.pathtoc, name), store.locBlockIDFromTimestampnano(namets)}
		}
	}
	close(jobChan)
	wg.Wait()
}

// compactionCandidate verifies that the given toc is a valid candidate for
// compaction and also returns the extracted namets.
func (store *DefaultGroupStore) compactionCandidate(name string) (int64, bool) {
	if !strings.HasSuffix(name, ".grouptoc") {
		return 0, false
	}
	var namets int64
	_, n := path.Split(name)
	namets, err := strconv.ParseInt(n[:len(n)-len(".grouptoc")], 10, 64)
	if err != nil {
		store.logError("bad timestamp in name: %#v\n", name)
		return 0, false
	}
	if namets == 0 {
		store.logError("bad timestamp in name: %#v\n", name)
		return namets, false
	}
	if namets == int64(atomic.LoadUint64(&store.activeTOCA)) || namets == int64(atomic.LoadUint64(&store.activeTOCB)) {
		return namets, false
	}
	if namets >= time.Now().UnixNano()-store.compactionState.ageThreshold {
		return namets, false
	}
	return namets, true
}

func (store *DefaultGroupStore) compactionWorker(jobChan chan *groupCompactionJob, wg *sync.WaitGroup) {
	for c := range jobChan {
		total, err := groupTOCStat(c.name, os.Stat, osOpenReadSeeker)
		if err != nil {
			store.logError("Unable to stat %s because: %v\n", c.name, err)
			continue
		}
		// TODO: This 1000 should be in the Config.
		// If total is less than 100, it'll automatically get compacted.
		if total >= 1000 {
			toCheck := uint32(total)
			// If there are more than a million entries, we'll just check the
			// first million and extrapolate.
			if toCheck > 1000000 {
				toCheck = 1000000
			}
			checked, stale, err := store.sampleTOC(c.name, c.candidateBlockID, toCheck)
			if err != nil {
				store.logError("Unable to sample %s: %s", c.name, err)
				continue
			}
			if store.logDebug != nil {
				store.logDebug("Compaction sample result: %s had %d entries; checked %d entries, %d were stale\n", c.name, total, checked, stale)
			}
			if stale <= uint32(float64(checked)*store.compactionState.threshold) {
				continue
			}
		}
		atomic.AddInt32(&store.smallFileCompactions, 1)
		result, err := store.compactFile(c.name, c.candidateBlockID)
		if err != nil {
			store.logCritical("%s\n", err)
			continue
		}
		if err = os.Remove(c.name); err != nil {
			store.logCritical("Unable to remove %s %s\n", c.name, err)
		}
		if err = os.Remove(c.name[:len(c.name)-len("toc")]); err != nil {
			store.logCritical("Unable to remove %s %s\n", c.name[:len(c.name)-len("toc")], err)
		}
		if err = store.closeLocBlock(c.candidateBlockID); err != nil {
			store.logCritical("error closing in-memory block for %s: %s\n", c.name, err)
		}
		if store.logDebug != nil {
			store.logDebug("Compacted %s (total %d, rewrote %d, stale %d)\n", c.name, result.count, result.rewrote, result.stale)
		}
	}
	wg.Done()
}

func (store *DefaultGroupStore) sampleTOC(name string, candidateBlockID uint32, toCheck uint32) (uint32, uint32, error) {
	stale := uint32(0)
	checked := uint32(0)
	// Compaction workers work on one file each; maybe we'll expand the workers
	// under a compaction worker sometime, but for now, limit it.
	workers := uint64(1)
	pendingBatchChans := make([]chan []groupTOCEntry, workers)
	freeBatchChans := make([]chan []groupTOCEntry, len(pendingBatchChans))
	for i := 0; i < len(pendingBatchChans); i++ {
		pendingBatchChans[i] = make(chan []groupTOCEntry, 3)
		freeBatchChans[i] = make(chan []groupTOCEntry, cap(pendingBatchChans[i]))
		for j := 0; j < cap(freeBatchChans[i]); j++ {
			freeBatchChans[i] <- make([]groupTOCEntry, store.recoveryBatchSize)
		}
	}
	controlChan := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(len(pendingBatchChans))
	for i := 0; i < len(pendingBatchChans); i++ {
		go func(pendingBatchChan chan []groupTOCEntry, freeBatchChan chan []groupTOCEntry) {
			skipRest := false
			for {
				batch := <-pendingBatchChan
				if batch == nil {
					break
				}
				if skipRest {
					continue
				}
				for j := 0; j < len(batch); j++ {
					wr := &batch[j]
					timestampBits, blockID, _, _ := store.lookup(wr.KeyA, wr.KeyB, wr.NameKeyA, wr.NameKeyB)
					if timestampBits != wr.TimestampBits || blockID != wr.BlockID {
						atomic.AddUint32(&stale, 1)
					}
					if c := atomic.AddUint32(&checked, 1); c == toCheck {
						skipRest = true
						close(controlChan)
						break
					} else if c > toCheck {
						skipRest = true
						break
					}
				}
				freeBatchChan <- batch
			}
			wg.Done()
		}(pendingBatchChans[i], freeBatchChans[i])
	}
	fpr, err := osOpenReadSeeker(name)
	if err != nil {
		return 0, 0, err
	}
	_, errs := groupReadTOCEntriesBatched(fpr, candidateBlockID, freeBatchChans, pendingBatchChans, controlChan)
	for _, err := range errs {
		store.logError("Compaction check error with %s: %s", name, err)
	}
	closeIfCloser(fpr)
	for i := 0; i < len(pendingBatchChans); i++ {
		pendingBatchChans[i] <- nil
	}
	wg.Wait()
	return checked, stale, nil
}

type groupCompactionResult struct {
	checksumFailures int
	count            int
	rewrote          int
	stale            int
}

func (store *DefaultGroupStore) compactFile(name string, candidateBlockID uint32) (*groupCompactionResult, error) {
	cr := &groupCompactionResult{}
	fromDiskBuf := make([]byte, store.checksumInterval+4)
	fromDiskOverflow := make([]byte, 0, _GROUP_FILE_ENTRY_SIZE)
	fp, err := os.Open(name)
	if err != nil {
		return cr, fmt.Errorf("error opening %s: %s", name, err)
	}
	first := true
	terminated := false
	fromDiskOverflow = fromDiskOverflow[:0]
	for {
		n, err := io.ReadFull(fp, fromDiskBuf)
		if n < 4 {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				fp.Close()
				return cr, fmt.Errorf("error reading %s: %s", name, err)
			}
			break
		}
		n -= 4
		if murmur3.Sum32(fromDiskBuf[:n]) != binary.BigEndian.Uint32(fromDiskBuf[n:]) {
			cr.checksumFailures++
		} else {
			j := 0
			if first {
				if !bytes.Equal(fromDiskBuf[:_GROUP_FILE_HEADER_SIZE-4], []byte("GROUPSTORETOC v0            ")) {
					fp.Close()
					return cr, fmt.Errorf("bad header %s: %s", name, err)
				}
				if binary.BigEndian.Uint32(fromDiskBuf[_GROUP_FILE_HEADER_SIZE-4:]) != store.checksumInterval {
					fp.Close()
					return cr, fmt.Errorf("bad header checksum interval %s: %s", name, err)
				}
				j += _GROUP_FILE_HEADER_SIZE
				first = false
			}
			if n < int(store.checksumInterval) {
				if !bytes.Equal(fromDiskBuf[n-_GROUP_FILE_TRAILER_SIZE:], []byte("TERM v0 ")) {
					fp.Close()
					return cr, fmt.Errorf("bad terminator marker %s: %s", name, err)
				}
				n -= _GROUP_FILE_TRAILER_SIZE
				terminated = true
			}
			if len(fromDiskOverflow) > 0 {
				j += _GROUP_FILE_ENTRY_SIZE - len(fromDiskOverflow)
				fromDiskOverflow = append(fromDiskOverflow, fromDiskBuf[j-_GROUP_FILE_ENTRY_SIZE+len(fromDiskOverflow):j]...)

				keyA := binary.BigEndian.Uint64(fromDiskOverflow)
				keyB := binary.BigEndian.Uint64(fromDiskOverflow[8:])
				nameKeyA := binary.BigEndian.Uint64(fromDiskOverflow[16:])
				nameKeyB := binary.BigEndian.Uint64(fromDiskOverflow[24:])
				timestampbits := binary.BigEndian.Uint64(fromDiskOverflow[32:])

				fromDiskOverflow = fromDiskOverflow[:0]
				tsm, blockid, _, _ := store.lookup(keyA, keyB, nameKeyA, nameKeyB)
				if tsm>>_TSB_UTIL_BITS != timestampbits>>_TSB_UTIL_BITS && blockid != candidateBlockID || tsm&_TSB_DELETION != 0 {
					cr.count++
					cr.stale++
				} else {
					var value []byte
					_, value, err := store.read(keyA, keyB, nameKeyA, nameKeyB, value)
					if err != nil {
						fp.Close()
						return cr, fmt.Errorf("error on read for compaction rewrite: %s", err)
					}
					_, err = store.write(keyA, keyB, nameKeyA, nameKeyB, timestampbits|_TSB_COMPACTION_REWRITE, value, true)
					if err != nil {
						fp.Close()
						return cr, fmt.Errorf("error on write for compaction rewrite: %s", err)
					}
					cr.count++
					cr.rewrote++
				}
			}
			for ; j+_GROUP_FILE_ENTRY_SIZE <= n; j += _GROUP_FILE_ENTRY_SIZE {

				keyA := binary.BigEndian.Uint64(fromDiskBuf[j:])
				keyB := binary.BigEndian.Uint64(fromDiskBuf[j+8:])
				nameKeyA := binary.BigEndian.Uint64(fromDiskBuf[j+16:])
				nameKeyB := binary.BigEndian.Uint64(fromDiskBuf[j+24:])
				timestampbits := binary.BigEndian.Uint64(fromDiskBuf[j+32:])

				tsm, blockid, _, _ := store.lookup(keyA, keyB, nameKeyA, nameKeyB)
				if tsm>>_TSB_UTIL_BITS != timestampbits>>_TSB_UTIL_BITS && blockid != candidateBlockID || tsm&_TSB_DELETION != 0 {
					cr.count++
					cr.stale++
				} else {
					var value []byte
					_, value, err := store.read(keyA, keyB, nameKeyA, nameKeyB, value)
					if err != nil {
						fp.Close()
						return cr, fmt.Errorf("error on read for compaction rewrite: %s", err)
					}
					_, err = store.write(keyA, keyB, nameKeyA, nameKeyB, timestampbits|_TSB_COMPACTION_REWRITE, value, true)
					if err != nil {
						fp.Close()
						return cr, fmt.Errorf("error on write for compaction rewrite: %s", err)
					}
					cr.count++
					cr.rewrote++
				}
			}
			if j != n {
				fromDiskOverflow = fromDiskOverflow[:n-j]
				copy(fromDiskOverflow, fromDiskBuf[j:])
			}
		}
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			fp.Close()
			return cr, fmt.Errorf("EOF while reading toc: %s", err)
		}
	}
	fp.Close()
	if !terminated {
		store.logError("early end of file: %s\n", name)
		return cr, nil

	}
	if cr.checksumFailures > 0 {
		store.logWarning("%d checksum failures for %s\n", cr.checksumFailures, name)
		return cr, nil

	}
	return cr, nil
}
