package store

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
	fullPath         string
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
		namets, valid := store.compactionCandidate(name)
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
		total, err := groupTOCStat(c.fullPath, os.Stat, osOpenReadSeeker)
		if err != nil {
			store.logError("Unable to stat %s because: %v\n", c.fullPath, err)
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
			checked, stale, err := store.sampleTOC(c.fullPath, c.candidateBlockID, toCheck)
			if err != nil {
				store.logError("Unable to sample %s: %s", c.fullPath, err)
				continue
			}
			if store.logDebug != nil {
				store.logDebug("Compaction sample result: %s had %d entries; checked %d entries, %d were stale\n", c.fullPath, total, checked, stale)
			}
			if stale <= uint32(float64(checked)*store.compactionState.threshold) {
				continue
			}
		}
		atomic.AddInt32(&store.smallFileCompactions, 1)
		result, err := store.compactFile(c.fullPath, c.candidateBlockID)
		if err != nil {
			store.logCritical("%s\n", err)
			continue
		}
		if err = os.Remove(c.fullPath); err != nil {
			store.logCritical("Unable to remove %s %s\n", c.fullPath, err)
		}
		if err = os.Remove(c.fullPath[:len(c.fullPath)-len("toc")]); err != nil {
			store.logCritical("Unable to remove %s %s\n", c.fullPath[:len(c.fullPath)-len("toc")], err)
		}
		if err = store.closeLocBlock(c.candidateBlockID); err != nil {
			store.logCritical("error closing in-memory block for %s: %s\n", c.fullPath, err)
		}
		if store.logDebug != nil {
			store.logDebug("Compacted %s (total %d, rewrote %d, stale %d)\n", c.fullPath, result.count, result.rewrote, result.stale)
		}
	}
	wg.Done()
}

func (store *DefaultGroupStore) sampleTOC(fullPath string, candidateBlockID uint32, toCheck uint32) (uint32, uint32, error) {
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
	fpr, err := osOpenReadSeeker(fullPath)
	if err != nil {
		return 0, 0, err
	}
	_, errs := groupReadTOCEntriesBatched(fpr, candidateBlockID, freeBatchChans, pendingBatchChans, controlChan)
	for _, err := range errs {
		store.logError("Compaction check error with %s: %s", fullPath, err)
	}
	closeIfCloser(fpr)
	for i := 0; i < len(pendingBatchChans); i++ {
		pendingBatchChans[i] <- nil
	}
	wg.Wait()
	return checked, stale, nil
}

type groupCompactionResult struct {
	errorCount uint32
	count      uint32
	rewrote    uint32
	stale      uint32
}

func (store *DefaultGroupStore) compactFile(fullPath string, candidateBlockID uint32) (*groupCompactionResult, error) {
	cr := &groupCompactionResult{}
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
	wg := &sync.WaitGroup{}
	wg.Add(len(pendingBatchChans))
	for i := 0; i < len(pendingBatchChans); i++ {
		go func(pendingBatchChan chan []groupTOCEntry, freeBatchChan chan []groupTOCEntry) {
			var value []byte
			for {
				batch := <-pendingBatchChan
				if batch == nil {
					break
				}
				if atomic.LoadUint32(&cr.errorCount) > 0 {
					continue
				}
				for j := 0; j < len(batch); j++ {
					atomic.AddUint32(&cr.count, 1)
					wr := &batch[j]
					timestampBits, _, _, _ := store.lookup(wr.KeyA, wr.KeyB, wr.NameKeyA, wr.NameKeyB)
					if timestampBits > wr.TimestampBits {
						atomic.AddUint32(&cr.stale, 1)
						continue
					}
					timestampBits, value, err := store.read(wr.KeyA, wr.KeyB, wr.NameKeyA, wr.NameKeyB, value[:0])
					if timestampBits > wr.TimestampBits {
						atomic.AddUint32(&cr.stale, 1)
						continue
					}
					_, err = store.write(wr.KeyA, wr.KeyB, wr.NameKeyA, wr.NameKeyB, wr.TimestampBits|_TSB_COMPACTION_REWRITE, value, true)
					if err != nil {
						store.logError("Compaction error with %s: %s", fullPath, err)
						atomic.AddUint32(&cr.errorCount, 1)
						break
					}
					atomic.AddUint32(&cr.rewrote, 1)
				}
				freeBatchChan <- batch
			}
			wg.Done()
		}(pendingBatchChans[i], freeBatchChans[i])
	}
	fpr, err := osOpenReadSeeker(fullPath)
	if err != nil {
		return cr, fmt.Errorf("Compaction error opening %s: %s\n", fullPath, err)
	}
	fdc, errs := groupReadTOCEntriesBatched(fpr, candidateBlockID, freeBatchChans, pendingBatchChans, make(chan struct{}))
	for _, err := range errs {
		store.logError("Compaction error with %s: %s", fullPath, err)
	}
	if len(errs) > 0 {
		if fdc == 0 {
			return cr, fmt.Errorf("Compaction errors with %s and no entries were read; file will be retried later.", fullPath)
		} else {
			store.logError("Compaction errors with %s but some entries were read; assuming the recovery was as good as it could get and removing file.", fullPath)
		}
	}
	closeIfCloser(fpr)
	for i := 0; i < len(pendingBatchChans); i++ {
		pendingBatchChans[i] <- nil
	}
	wg.Wait()
	return cr, nil
}
