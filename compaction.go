package valuestore

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

type compactionState struct {
	interval     int
	workerCount  int
	ageThreshold int64
	abort        uint32
	threshold    float64
	notifyChan   chan *backgroundNotification
}

func (vs *DefaultValueStore) compactionConfig(cfg *Config) {
	vs.compactionState.interval = cfg.CompactionInterval
	vs.compactionState.threshold = cfg.CompactionThreshold
	vs.compactionState.ageThreshold = int64(cfg.CompactionAgeThreshold * 1000000000)
	vs.compactionState.notifyChan = make(chan *backgroundNotification, 1)
	vs.compactionState.workerCount = cfg.CompactionWorkers
}

func (vs *DefaultValueStore) compactionLaunch() {
	go vs.compactionLauncher()
}

// DisableCompaction will stop any compaction passes until
// EnableCompaction is called. A compaction pass searches for files
// with a percentage of XX deleted entries.
func (vs *DefaultValueStore) DisableCompaction() {
	c := make(chan struct{}, 1)
	vs.compactionState.notifyChan <- &backgroundNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableCompaction will resume compaction passes.
// A compaction pass searches for files with a percentage of XX deleted
// entries.
func (vs *DefaultValueStore) EnableCompaction() {
	c := make(chan struct{}, 1)
	vs.compactionState.notifyChan <- &backgroundNotification{
		enable:   true,
		doneChan: c,
	}
	<-c
}

// CompactionPass will immediately execute a compaction pass to compact stale files.
func (vs *DefaultValueStore) CompactionPass() {
	atomic.StoreUint32(&vs.compactionState.abort, 1)
	c := make(chan struct{}, 1)
	vs.compactionState.notifyChan <- &backgroundNotification{doneChan: c}
	<-c
}

func (vs *DefaultValueStore) compactionLauncher() {
	var enabled bool
	interval := float64(vs.compactionState.interval) * float64(time.Second)
	vs.randMutex.Lock()
	nextRun := time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
	vs.randMutex.Unlock()
	for {
		var notification *backgroundNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-vs.compactionState.notifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-vs.compactionState.notifyChan:
			default:
			}
		}
		vs.randMutex.Lock()
		nextRun = time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
		vs.randMutex.Unlock()
		if notification != nil {
			if notification.enable {
				enabled = true
				notification.doneChan <- struct{}{}
				continue
			}
			if notification.disable {
				atomic.StoreUint32(&vs.compactionState.abort, 1)
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&vs.compactionState.abort, 0)
			vs.compactionPass()
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&vs.compactionState.abort, 0)
			vs.compactionPass()
		}
	}
}

type compactionJob struct {
	name             string
	candidateBlockID uint32
}

func (vs *DefaultValueStore) compactionPass() {
	if vs.logDebug != nil {
		begin := time.Now()
		defer func() {
			vs.logDebug("compaction pass took %s\n", time.Now().Sub(begin))
		}()
	}
	fp, err := os.Open(vs.pathtoc)
	if err != nil {
		vs.logError("%s\n", err)
		return
	}
	names, err := fp.Readdirnames(-1)
	fp.Close()
	if err != nil {
		vs.logError("%s\n", err)
		return
	}
	sort.Strings(names)
	jobChan := make(chan *compactionJob, len(names))
	wg := &sync.WaitGroup{}
	for i := 0; i < vs.compactionState.workerCount; i++ {
		wg.Add(1)
		go vs.compactionWorker(jobChan, wg)
	}
	for _, name := range names {
		namets, valid := vs.compactionCandidate(path.Join(vs.pathtoc, name))
		if valid {
			jobChan <- &compactionJob{path.Join(vs.pathtoc, name), vs.valueLocBlockIDFromTimestampnano(namets)}
		}
	}
	close(jobChan)
	wg.Wait()
}

// compactionCandidate verifies that the given toc is a valid candidate for
// compaction and also returns the extracted namets.
// TODO: This doesn't need to be its own func anymore
func (vs *DefaultValueStore) compactionCandidate(name string) (int64, bool) {
	if !strings.HasSuffix(name, ".valuestoc") {
		return 0, false
	}
	var namets int64
	_, n := path.Split(name)
	namets, err := strconv.ParseInt(n[:len(n)-len(".valuestoc")], 10, 64)
	if err != nil {
		vs.logError("bad timestamp in name: %#v\n", name)
		return 0, false
	}
	if namets == 0 {
		vs.logError("bad timestamp in name: %#v\n", name)
		return namets, false
	}
	if namets == int64(atomic.LoadUint64(&vs.activeTOCA)) || namets == int64(atomic.LoadUint64(&vs.activeTOCB)) {
		return namets, false
	}
	if namets >= time.Now().UnixNano()-vs.compactionState.ageThreshold {
		return namets, false
	}
	return namets, true
}

func (vs *DefaultValueStore) compactionWorker(jobChan chan *compactionJob, wg *sync.WaitGroup) {
	for c := range jobChan {
		fstat, err := os.Stat(c.name)
		if err != nil {
			vs.logError("Unable to stat %s because: %v\n", c.name, err)
			continue
		}
		total := int(fstat.Size()) / 34
		// TODO: This 100 should be in the Config.
		if total < 100 {
			atomic.AddInt32(&vs.smallFileCompactions, 1)
			result, err := vs.compactFile(c.name, c.candidateBlockID)
			if err != nil {
				vs.logCritical("%s\n", err)
				continue
			}
			if (result.rewrote + result.stale) == result.count {
				err = os.Remove(c.name)
				if err != nil {
					vs.logCritical("Unable to remove %s %s\n", c.name, err)
					continue
				}
				err = os.Remove(c.name[:len(c.name)-len("toc")])
				if err != nil {
					vs.logCritical("Unable to remove %s values %s\n", c.name, err)
					continue
				}
				vs.closeValueLocBlock(c.candidateBlockID)
				if vs.logDebug != nil {
					vs.logDebug("Compacted %s (total %d, rewrote %d, stale %d)\n", c.name, result.count, result.rewrote, result.stale)
				}
			}
		} else {
			rand.Seed(time.Now().UnixNano())
			skipOffset := rand.Intn(int(float64(total) * 0.01)) //randomly skip up to the first 1% of entries
			skipTotal := total - skipOffset
			staleTarget := int(float64(skipTotal) * vs.compactionState.threshold)
			skip := skipTotal/staleTarget - 1
			count, stale, err := vs.sampleTOC(c.name, c.candidateBlockID, skipOffset, skip)
			if err != nil {
				continue
			}
			if vs.logDebug != nil {
				vs.logDebug("%s sample result: %d %d %d\n", c.name, count, stale, staleTarget)
			}
			if stale >= staleTarget {
				atomic.AddInt32(&vs.compactions, 1)
				if vs.logDebug != nil {
					vs.logDebug("Triggering compaction for %s with %d entries.\n", c.name, count)
				}
				result, err := vs.compactFile(c.name, c.candidateBlockID)
				if err != nil {
					vs.logCritical("%s\n", err)
					continue
				}
				if (result.rewrote + result.stale) == result.count {
					err = os.Remove(c.name)
					if err != nil {
						vs.logCritical("Unable to remove %s %s\n", c.name, err)
						continue
					}
					err = os.Remove(c.name[:len(c.name)-len("toc")])
					if err != nil {
						vs.logCritical("Unable to remove %s values %s\n", c.name, err)
						continue
					}
					vs.closeValueLocBlock(c.candidateBlockID)
					if vs.logDebug != nil {
						vs.logDebug("Compacted %s: (total %d, rewrote %d, stale %d)\n", c.name, result.count, result.rewrote, result.stale)
					}
				}
			}
		}
	}
	wg.Done()
}

func (vs *DefaultValueStore) sampleTOC(name string, candidateBlockID uint32, skipOffset, skipCount int) (int, int, error) {
	count := 0
	stale := 0
	fromDiskBuf := make([]byte, vs.checksumInterval+4)
	fromDiskOverflow := make([]byte, 0, 32)
	fp, err := os.Open(name)
	if err != nil {
		vs.logError("error opening %s: %s\n", name, err)
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
				vs.logError("error reading %s: %s\n", name, err)
			}
			break
		}
		n -= 4
		if murmur3.Sum32(fromDiskBuf[:n]) != binary.BigEndian.Uint32(fromDiskBuf[n:]) {
			checksumFailures++
		} else {
			j := 0
			if first {
				if !bytes.Equal(fromDiskBuf[:28], []byte("VALUESTORETOC v0            ")) {
					vs.logError("bad header: %s\n", name)
					break
				}
				if binary.BigEndian.Uint32(fromDiskBuf[28:]) != vs.checksumInterval {
					vs.logError("bad header checksum interval: %s\n", name)
					break
				}
				j += 32
				first = false
			}
			if n < int(vs.checksumInterval) {
				if binary.BigEndian.Uint32(fromDiskBuf[n-16:]) != 0 {
					vs.logError("bad terminator size marker: %s\n", name)
					break
				}
				if !bytes.Equal(fromDiskBuf[n-4:n], []byte("TERM")) {
					vs.logError("bad terminator: %s\n", name)
					break
				}
				n -= 16
				terminated = true
			}
			if len(fromDiskOverflow) > 0 {
				j += 32 - len(fromDiskOverflow)
				fromDiskOverflow = append(fromDiskOverflow, fromDiskBuf[j-32+len(fromDiskOverflow):j]...)
				keyB := binary.BigEndian.Uint64(fromDiskOverflow[8:])
				keyA := binary.BigEndian.Uint64(fromDiskOverflow)
				timestampbits := binary.BigEndian.Uint64(fromDiskOverflow[16:])
				fromDiskOverflow = fromDiskOverflow[:0]
				count++
				if skipCounter == skipCount {
					tsm, blockid, _, _ := vs.lookup(keyA, keyB)
					if tsm>>_TSB_UTIL_BITS != timestampbits>>_TSB_UTIL_BITS && blockid != candidateBlockID || tsm&_TSB_DELETION != 0 {
						stale++
					}
					skipCounter = 0
				} else {
					skipCounter++
				}

			}
			for ; j+32 <= n; j += 32 {
				keyB := binary.BigEndian.Uint64(fromDiskBuf[j+8:])
				keyA := binary.BigEndian.Uint64(fromDiskBuf[j:])
				timestampbits := binary.BigEndian.Uint64(fromDiskBuf[j+16:])
				tsm, blockid, _, _ := vs.lookup(keyA, keyB)
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
			vs.logError("error reading %s: %s\n", name, err)
			break
		}
	}
	fp.Close()
	if !terminated {
		vs.logError("early end of file: %s\n", name)
	}
	if checksumFailures > 0 {
		vs.logWarning("%d checksum failures for %s\n", checksumFailures, name)
	}
	return count, stale, nil

}

type compactionResult struct {
	checksumFailures int
	count            int
	rewrote          int
	stale            int
}

func (vs *DefaultValueStore) compactFile(name string, candidateBlockID uint32) (compactionResult, error) {
	var cr compactionResult
	fromDiskBuf := make([]byte, vs.checksumInterval+4)
	fromDiskOverflow := make([]byte, 0, 32)
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
				if !bytes.Equal(fromDiskBuf[:28], []byte("VALUESTORETOC v0            ")) {
					fp.Close()
					return cr, fmt.Errorf("bad header %s: %s", name, err)
				}
				if binary.BigEndian.Uint32(fromDiskBuf[28:]) != vs.checksumInterval {
					fp.Close()
					return cr, fmt.Errorf("bad header checksum interval %s: %s", name, err)
				}
				j += 32
				first = false
			}
			if n < int(vs.checksumInterval) {
				if binary.BigEndian.Uint32(fromDiskBuf[n-16:]) != 0 {
					fp.Close()
					return cr, fmt.Errorf("bad terminator size %s: %s", name, err)
				}
				if !bytes.Equal(fromDiskBuf[n-4:n], []byte("TERM")) {
					fp.Close()
					return cr, fmt.Errorf("bad terminator marker %s: %s", name, err)
				}
				n -= 16
				terminated = true
			}
			if len(fromDiskOverflow) > 0 {
				j += 32 - len(fromDiskOverflow)
				fromDiskOverflow = append(fromDiskOverflow, fromDiskBuf[j-32+len(fromDiskOverflow):j]...)
				keyB := binary.BigEndian.Uint64(fromDiskOverflow[8:])
				keyA := binary.BigEndian.Uint64(fromDiskOverflow)
				timestampbits := binary.BigEndian.Uint64(fromDiskOverflow[16:])
				fromDiskOverflow = fromDiskOverflow[:0]
				tsm, blockid, _, _ := vs.lookup(keyA, keyB)
				if tsm>>_TSB_UTIL_BITS != timestampbits>>_TSB_UTIL_BITS && blockid != candidateBlockID || tsm&_TSB_DELETION != 0 {
					cr.count++
					cr.stale++
				} else {
					var value []byte
					_, value, err := vs.read(keyA, keyB, value)
					if err != nil {
						fp.Close()
						return cr, fmt.Errorf("error on read for compaction rewrite: %s", err)
					}
					_, err = vs.write(keyA, keyB, timestampbits|_TSB_COMPACTION_REWRITE, value)
					if err != nil {
						fp.Close()
						return cr, fmt.Errorf("error on write for compaction rewrite: %s", err)
					}
					cr.count++
					cr.rewrote++
				}
			}
			for ; j+32 <= n; j += 32 {
				keyB := binary.BigEndian.Uint64(fromDiskBuf[j+8:])
				keyA := binary.BigEndian.Uint64(fromDiskBuf[j:])
				timestampbits := binary.BigEndian.Uint64(fromDiskBuf[j+16:])
				tsm, blockid, _, _ := vs.lookup(keyA, keyB)
				if tsm>>_TSB_UTIL_BITS != timestampbits>>_TSB_UTIL_BITS && blockid != candidateBlockID || tsm&_TSB_DELETION != 0 {
					cr.count++
					cr.stale++
				} else {
					var value []byte
					_, value, err := vs.read(keyA, keyB, value)
					if err != nil {
						fp.Close()
						return cr, fmt.Errorf("error on read for compaction rewrite: %s", err)
					}
					_, err = vs.write(keyA, keyB, timestampbits|_TSB_COMPACTION_REWRITE, value)
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
		vs.logError("early end of file: %s\n", name)
		return cr, nil

	}
	if cr.checksumFailures > 0 {
		vs.logWarning("%d checksum failures for %s\n", cr.checksumFailures, name)
		return cr, nil

	}
	return cr, nil
}
