package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
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
// TODO: This doesn't need to be its own func anymore
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
		fstat, err := os.Stat(c.name)
		if err != nil {
			store.logError("Unable to stat %s because: %v\n", c.name, err)
			continue
		}
		total := int(fstat.Size()) / 34
		// TODO: This 100 should be in the Config.
		if total < 100 {
			atomic.AddInt32(&store.smallFileCompactions, 1)
			result, err := store.compactFile(c.name, c.candidateBlockID)
			if err != nil {
				store.logCritical("%s\n", err)
				continue
			}
			if (result.rewrote + result.stale) == result.count {
				err = os.Remove(c.name)
				if err != nil {
					store.logCritical("Unable to remove %s %s\n", c.name, err)
					continue
				}
				err = os.Remove(c.name[:len(c.name)-len("toc")])
				if err != nil {
					store.logCritical("Unable to remove %s %s\n", c.name[:len(c.name)-len("toc")], err)
					continue
				}
				err = store.closeLocBlock(c.candidateBlockID)
				if err != nil {
					store.logCritical("error closing in-memory block for %s: %s\n", c.name, err)
				}
				if store.logDebug != nil {
					store.logDebug("Compacted %s (total %d, rewrote %d, stale %d)\n", c.name, result.count, result.rewrote, result.stale)
				}
			}
		} else {
			rand.Seed(time.Now().UnixNano())
			skipOffset := rand.Intn(int(float64(total) * 0.01)) //randomly skip up to the first 1% of entries
			skipTotal := total - skipOffset
			staleTarget := int(float64(skipTotal) * store.compactionState.threshold)
			skip := skipTotal/staleTarget - 1
			count, stale, err := store.sampleTOC(c.name, c.candidateBlockID, skipOffset, skip)
			if err != nil {
				continue
			}
			if store.logDebug != nil {
				store.logDebug("%s sample result: %d %d %d\n", c.name, count, stale, staleTarget)
			}
			if stale >= staleTarget {
				atomic.AddInt32(&store.compactions, 1)
				if store.logDebug != nil {
					store.logDebug("Triggering compaction for %s with %d entries.\n", c.name, count)
				}
				result, err := store.compactFile(c.name, c.candidateBlockID)
				if err != nil {
					store.logCritical("%s\n", err)
					continue
				}
				if (result.rewrote + result.stale) == result.count {
					err = os.Remove(c.name)
					if err != nil {
						store.logCritical("Unable to remove %s %s\n", c.name, err)
						continue
					}
					err = os.Remove(c.name[:len(c.name)-len("toc")])
					if err != nil {
						store.logCritical("Unable to remove %s %s\n", c.name[:len(c.name)-len("toc")], err)
						continue
					}
					err = store.closeLocBlock(c.candidateBlockID)
					if err != nil {
						store.logCritical("error closing in-memory block for %s: %s\n", c.name, err)
					}
					if store.logDebug != nil {
						store.logDebug("Compacted %s: (total %d, rewrote %d, stale %d)\n", c.name, result.count, result.rewrote, result.stale)
					}
				}
			}
		}
	}
	wg.Done()
}

func (store *DefaultGroupStore) sampleTOC(name string, candidateBlockID uint32, skipOffset, skipCount int) (int, int, error) {
	count := 0
	stale := 0
	fromDiskBuf := make([]byte, store.checksumInterval+4)
	fromDiskOverflow := make([]byte, 0, _GROUP_FILE_ENTRY_SIZE)
	fp, err := os.Open(name)
	if err != nil {
		store.logError("error opening %s: %s\n", name, err)
		return 0, 0, err
	}
	checksumFailures := 0
	first := true
	terminated := false
	fromDiskOverflow = fromDiskOverflow[:0]
	skipCounter := 0 - skipOffset
	for {
		n, err := io.ReadFull(fp, fromDiskBuf)
		if n < 4 {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				store.logError("error reading %s: %s\n", name, err)
			}
			break
		}
		n -= 4
		if murmur3.Sum32(fromDiskBuf[:n]) != binary.BigEndian.Uint32(fromDiskBuf[n:]) {
			checksumFailures++
		} else {
			j := 0
			if first {
				if !bytes.Equal(fromDiskBuf[:_GROUP_FILE_HEADER_SIZE-4], []byte("GROUPSTORETOC v0            ")) {
					store.logError("bad header: %s\n", name)
					break
				}
				if binary.BigEndian.Uint32(fromDiskBuf[_GROUP_FILE_HEADER_SIZE-4:]) != store.checksumInterval {
					store.logError("bad header checksum interval: %s\n", name)
					break
				}
				j += _GROUP_FILE_HEADER_SIZE
				first = false
			}
			if n < int(store.checksumInterval) {
				if binary.BigEndian.Uint32(fromDiskBuf[n-_GROUP_FILE_TRAILER_SIZE:]) != 0 {
					store.logError("bad terminator size marker: %s\n", name)
					break
				}
				if !bytes.Equal(fromDiskBuf[n-4:n], []byte("TERM")) {
					store.logError("bad terminator: %s\n", name)
					break
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
				count++
				if skipCounter == skipCount {
					tsm, blockid, _, _ := store.lookup(keyA, keyB, nameKeyA, nameKeyB)
					if tsm>>_TSB_UTIL_BITS != timestampbits>>_TSB_UTIL_BITS && blockid != candidateBlockID || tsm&_TSB_DELETION != 0 {
						stale++
					}
					skipCounter = 0
				} else {
					skipCounter++
				}

			}
			for ; j+_GROUP_FILE_ENTRY_SIZE <= n; j += _GROUP_FILE_ENTRY_SIZE {

				keyA := binary.BigEndian.Uint64(fromDiskBuf[j:])
				keyB := binary.BigEndian.Uint64(fromDiskBuf[j+8:])
				nameKeyA := binary.BigEndian.Uint64(fromDiskBuf[j+16:])
				nameKeyB := binary.BigEndian.Uint64(fromDiskBuf[j+24:])
				timestampbits := binary.BigEndian.Uint64(fromDiskBuf[j+32:])

				tsm, blockid, _, _ := store.lookup(keyA, keyB, nameKeyA, nameKeyB)
				count++
				if skipCounter == skipCount {
					if tsm>>_TSB_UTIL_BITS != timestampbits>>_TSB_UTIL_BITS && blockid != candidateBlockID || tsm&_TSB_DELETION != 0 {
						stale++
					}
					skipCounter = 0
				} else {
					skipCounter++
				}
			}
			if j != n {
				fromDiskOverflow = fromDiskOverflow[:n-j]
				copy(fromDiskOverflow, fromDiskBuf[j:])
			}
		}
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			store.logError("error reading %s: %s\n", name, err)
			break
		}
	}
	fp.Close()
	if !terminated {
		store.logError("early end of file: %s\n", name)
	}
	if checksumFailures > 0 {
		store.logWarning("%d checksum failures for %s\n", checksumFailures, name)
	}
	return count, stale, nil

}

type groupCompactionResult struct {
	checksumFailures int
	count            int
	rewrote          int
	stale            int
}

func (store *DefaultGroupStore) compactFile(name string, candidateBlockID uint32) (groupCompactionResult, error) {
	var cr groupCompactionResult
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
				if binary.BigEndian.Uint32(fromDiskBuf[n-_GROUP_FILE_TRAILER_SIZE:]) != 0 {
					fp.Close()
					return cr, fmt.Errorf("bad terminator size %s: %s", name, err)
				}
				if !bytes.Equal(fromDiskBuf[n-4:n], []byte("TERM")) {
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
