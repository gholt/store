package store

import (
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type groupCompactionState struct {
	interval     int
	threshold    float64
	ageThreshold int64
	workerCount  int

	startupShutdownLock sync.Mutex
	notifyChan          chan *bgNotification

	compactionLock              sync.Mutex
	compactionPendingBatchChans []chan []groupTOCEntry
	compactionFreeBatchChans    []chan []groupTOCEntry
}

func (store *defaultGroupStore) compactionConfig(cfg *GroupStoreConfig) {
	store.compactionState.interval = cfg.CompactionInterval
	store.compactionState.threshold = cfg.CompactionThreshold
	store.compactionState.ageThreshold = int64(cfg.CompactionAgeThreshold * 1000000000)
	store.compactionState.workerCount = cfg.CompactionWorkers
}

func (store *defaultGroupStore) compactionStartup() {
	store.compactionState.startupShutdownLock.Lock()
	if store.compactionState.notifyChan == nil {
		store.compactionState.notifyChan = make(chan *bgNotification, 1)
		go store.compactionLauncher(store.compactionState.notifyChan)
	}
	store.compactionState.compactionLock.Lock()
	store.compactionState.compactionPendingBatchChans = make([]chan []groupTOCEntry, store.compactionState.workerCount)
	store.compactionState.compactionFreeBatchChans = make([]chan []groupTOCEntry, len(store.compactionState.compactionPendingBatchChans))
	for i := 0; i < len(store.compactionState.compactionPendingBatchChans); i++ {
		store.compactionState.compactionPendingBatchChans[i] = make(chan []groupTOCEntry, 3)
		store.compactionState.compactionFreeBatchChans[i] = make(chan []groupTOCEntry, cap(store.compactionState.compactionPendingBatchChans[i]))
		for j := 0; j < cap(store.compactionState.compactionFreeBatchChans[i]); j++ {
			store.compactionState.compactionFreeBatchChans[i] <- make([]groupTOCEntry, store.recoveryBatchSize)
		}
	}
	store.compactionState.compactionLock.Unlock()
	store.compactionState.startupShutdownLock.Unlock()
}

func (store *defaultGroupStore) compactionShutdown() {
	store.compactionState.startupShutdownLock.Lock()
	if store.compactionState.notifyChan != nil {
		c := make(chan struct{}, 1)
		store.compactionState.notifyChan <- &bgNotification{
			action:   _BG_DISABLE,
			doneChan: c,
		}
		<-c
		store.compactionState.notifyChan = nil
	}
	store.compactionState.compactionLock.Lock()
	store.compactionState.compactionPendingBatchChans = nil
	store.compactionState.compactionFreeBatchChans = nil
	store.compactionState.compactionLock.Unlock()
	store.compactionState.startupShutdownLock.Unlock()
}

func (store *defaultGroupStore) compactionLauncher(notifyChan chan *bgNotification) {
	interval := float64(store.compactionState.interval) * float64(time.Second)
	store.randMutex.Lock()
	nextRun := time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
	store.randMutex.Unlock()
	var notification *bgNotification
	running := true
	for running {
		if notification == nil {
			sleep := nextRun.Sub(time.Now())
			if sleep > 0 {
				select {
				case notification = <-notifyChan:
				case <-time.After(sleep):
				}
			} else {
				select {
				case notification = <-notifyChan:
				default:
				}
			}
		}
		store.randMutex.Lock()
		nextRun = time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
		store.randMutex.Unlock()
		if notification != nil {
			var nextNotification *bgNotification
			switch notification.action {
			case _BG_PASS:
				nextNotification = store.compactionPass(notifyChan)
			case _BG_DISABLE:
				running = false
			default:
				// Critical because there was a coding error that needs to be
				// fixed by a person.
				store.logger.Error("invalid action requested", zap.String("setion", "compaction"), zap.Int("action", int(notification.action)))
			}
			notification.doneChan <- struct{}{}
			notification = nextNotification
		} else {
			notification = store.compactionPass(notifyChan)
		}
	}
}

type groupCompactionJob struct {
	nametoc          string
	candidateBlockID uint32
}

func (store *defaultGroupStore) compactionPass(notifyChan chan *bgNotification) *bgNotification {
	begin := time.Now()
	defer func() {
		elapsed := time.Now().Sub(begin)
		store.logger.Debug("pass completed", zap.String("name", store.loggerPrefix+"compaction"), zap.Duration("elapsed", elapsed))
		atomic.StoreInt64(&store.compactionNanoseconds, elapsed.Nanoseconds())
	}()
	names, err := store.readdirnames(store.pathtoc)
	if err != nil {
		store.logger.Error("error from readdirnames", zap.String("name", store.loggerPrefix+"compaction"), zap.Error(err))
		return nil
	}
	sort.Strings(names)
	jobChan := make(chan *groupCompactionJob, len(names))
	controlChan := make(chan struct{})
	wg := &sync.WaitGroup{}
	for i := 0; i < store.compactionState.workerCount; i++ {
		wg.Add(1)
		go store.compactionWorker(jobChan, controlChan, wg)
	}
	waitChan := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		close(waitChan)
	}()
	for _, name := range names {
		select {
		case notification := <-notifyChan:
			close(controlChan)
			<-waitChan
			return notification
		default:
		}
		if namets, valid := store.compactionCandidate(name); valid {
			jobChan <- &groupCompactionJob{name, store.locBlockIDFromTimestampnano(namets)}
		}
	}
	close(jobChan)
	for {
		select {
		case notification := <-notifyChan:
			close(controlChan)
			<-waitChan
			return notification
		case <-waitChan:
			return nil
		}
	}
}

// compactionCandidate verifies that the given file name is a valid candidate
// for compaction and also returns the extracted namets.
func (store *defaultGroupStore) compactionCandidate(name string) (int64, bool) {
	if !strings.HasSuffix(name, ".grouptoc") {
		return 0, false
	}
	var namets int64
	namets, err := strconv.ParseInt(name[:len(name)-len(".grouptoc")], 10, 64)
	if err != nil {
		store.logger.Warn("bad timestamp in name", zap.String("name", store.loggerPrefix+"compaction"), zap.String("filename", name))
		return 0, false
	}
	if namets == 0 {
		store.logger.Warn("bad timestamp in name", zap.String("name", store.loggerPrefix+"compaction"), zap.String("filename", name))
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

func (store *defaultGroupStore) compactionWorker(jobChan chan *groupCompactionJob, controlChan chan struct{}, wg *sync.WaitGroup) {
	for c := range jobChan {
		select {
		case <-controlChan:
			break
		default:
		}
		total, err := groupTOCStat(path.Join(store.pathtoc, c.nametoc), store.stat, store.openReadSeeker)
		if err != nil {
			store.logger.Warn("unable to stat", zap.String("name", store.loggerPrefix+"compaction"), zap.String("path", path.Join(store.pathtoc, c.nametoc)), zap.Error(err))
			continue
		}
		// TODO: This 1000 should be in the Config.
		// If total is less than 1000, it'll automatically get compacted.
		if total < 1000 {
			atomic.AddInt32(&store.smallFileCompactions, 1)
		} else {
			toCheck := uint32(total)
			// If there are more than a million entries, we'll just check the
			// first million and extrapolate.
			if toCheck > 1000000 {
				toCheck = 1000000
			}
			if !store.needsCompaction(c.nametoc, c.candidateBlockID, total, toCheck) {
				continue
			}
			atomic.AddInt32(&store.compactions, 1)
		}
		store.compactFile(c.nametoc, c.candidateBlockID, controlChan, "compactionWorker")
	}
	wg.Done()
}

func (store *defaultGroupStore) needsCompaction(nametoc string, candidateBlockID uint32, total int, toCheck uint32) bool {
	// This currently just reads the first store.recoveryBatchSize entries to
	// determine whether to compact the file or not. It would likely be better
	// to sample throughout the file instead, but this was simpler for now and,
	// hopefully, the writes will be scattered enough to not make much of a
	// difference.
	stale := uint32(0)
	checked := uint32(0)
	store.compactionState.compactionLock.Lock()
	defer store.compactionState.compactionLock.Unlock()
	controlChan := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(len(store.compactionState.compactionPendingBatchChans))
	for i := 0; i < len(store.compactionState.compactionPendingBatchChans); i++ {
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
					timestampBits, blockID, _, _ := store.lookup(wr.KeyA, wr.KeyB, wr.ChildKeyA, wr.ChildKeyB)
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
		}(store.compactionState.compactionPendingBatchChans[i], store.compactionState.compactionFreeBatchChans[i])
	}
	fpr, err := store.openReadSeeker(path.Join(store.pathtoc, nametoc))
	if err != nil {
		// Critical level since future recoveries, compactions, and audits will
		// keep hitting this file until a person corrects the file system
		// issue.
		store.logger.Error("cannot open", zap.String("name", store.loggerPrefix+"compaction"), zap.String("filename", nametoc), zap.Error(err))
		return false
	}
	_, errs := groupReadTOCEntriesBatched(fpr, candidateBlockID, store.compactionState.compactionFreeBatchChans, store.compactionState.compactionPendingBatchChans, controlChan)
	for _, err := range errs {
		store.logger.Warn("error from ReadTOCEntriesBatched", zap.String("name", store.loggerPrefix+"compaction"), zap.String("filename", nametoc), zap.Error(err))
	}
	closeIfCloser(fpr)
	for i := 0; i < len(store.compactionState.compactionPendingBatchChans); i++ {
		store.compactionState.compactionPendingBatchChans[i] <- nil
	}
	wg.Wait()
	if len(errs) > 0 {
		store.logger.Warn("since there were errors while reading, compaction is needed", zap.String("name", store.loggerPrefix+"compaction"), zap.String("filename", nametoc))
		return true
	}
	store.logger.Debug("sample result", zap.String("name", store.loggerPrefix+"compaction"), zap.String("filename", nametoc), zap.Int("total", total), zap.Uint64("checked", uint64(checked)), zap.Uint64("stale", uint64(stale)))
	return stale > uint32(float64(checked)*store.compactionState.threshold)
}

func (store *defaultGroupStore) compactFile(nametoc string, blockID uint32, controlChan chan struct{}, removemeCaller string) {
	var readErrorCount uint32
	var writeErrorCount uint32
	var count uint32
	var rewrote uint32
	var stale uint32
	store.compactionState.compactionLock.Lock()
	defer store.compactionState.compactionLock.Unlock()
	wg := &sync.WaitGroup{}
	wg.Add(len(store.compactionState.compactionPendingBatchChans))
	for i := 0; i < len(store.compactionState.compactionPendingBatchChans); i++ {
		go func(pendingBatchChan chan []groupTOCEntry, freeBatchChan chan []groupTOCEntry) {
			var value []byte
			for {
				batch := <-pendingBatchChan
				if batch == nil {
					break
				}
				if atomic.LoadUint32(&writeErrorCount) > 0 {
					continue
				}
				for j := 0; j < len(batch); j++ {
					atomic.AddUint32(&count, 1)
					wr := &batch[j]
					timestampBits, _, _, _ := store.lookup(wr.KeyA, wr.KeyB, wr.ChildKeyA, wr.ChildKeyB)
					if timestampBits > wr.TimestampBits {
						atomic.AddUint32(&stale, 1)
						continue
					}
					// Must assume entry was deleted and the tombstone has
					// expired; must not resurrect and if there had been a read
					// error on initial startup but now compaction can read the
					// value, it's better to just let replication from another
					// node take place anyway.
					if timestampBits == 0 { // not found
						atomic.AddUint32(&stale, 1)
						continue
					}
					timestampBits, value, err := store.read(wr.KeyA, wr.KeyB, wr.ChildKeyA, wr.ChildKeyB, value[:0])
					// Same as above note about timestampBits == 0.
					if IsNotFound(err) {
						atomic.AddUint32(&stale, 1)
						continue
					}
					if err != nil {
						store.logger.Warn("error reading while compacting", zap.String("name", store.loggerPrefix+"compactFile"), zap.String("filename", nametoc), zap.Error(err))
						atomic.AddUint32(&readErrorCount, 1)
						// Keeps going, but the readErrorCount will let it know
						// to *not* remove the original file. This is "for
						// now". TODO: In the future, I'd like this to remove
						// the entry from the locmap so replication will bring
						// it back from other nodes, but that code can wait for
						// the moment.
						continue
					}
					if timestampBits > wr.TimestampBits {
						atomic.AddUint32(&stale, 1)
						continue
					}
					_, err = store.write(wr.KeyA, wr.KeyB, wr.ChildKeyA, wr.ChildKeyB, wr.TimestampBits|_TSB_COMPACTION_REWRITE, value, true)
					if err != nil {
						store.logger.Error("error writing while compacting", zap.String("name", store.loggerPrefix+"compactFile"), zap.String("filename", nametoc), zap.Error(err))
						atomic.AddUint32(&writeErrorCount, 1)
						// TODO: Write errors are pretty bad and we should quit
						// writing new data if we get a write error. For now,
						// this quits writing during this compaction, but
						// doesn't disable the whole service from writes, or
						// the next compaction pass.
						break
					}
					atomic.AddUint32(&rewrote, 1)
				}
				freeBatchChan <- batch
			}
			wg.Done()
		}(store.compactionState.compactionPendingBatchChans[i], store.compactionState.compactionFreeBatchChans[i])
	}
	fullpath := path.Join(store.path, nametoc[:len(nametoc)-3])
	fullpathtoc := path.Join(store.pathtoc, nametoc)
	spindown := func(remove bool) {
		if remove {
			if err := store.remove(fullpathtoc); err != nil {
				store.logger.Warn("unable to remove", zap.String("name", store.loggerPrefix+"compactFile"), zap.String("path", fullpathtoc), zap.Error(err))
				if err = store.rename(fullpathtoc, fullpathtoc+".renamed"); err != nil {
					// Critical level since future recoveries, compactions, and
					// audits will keep hitting this file until a person
					// corrects the file system issue.
					store.logger.Error("also could not rename", zap.String("name", store.loggerPrefix+"compactFile"), zap.String("path", fullpathtoc), zap.Error(err))
				}
			}
			if err := store.remove(fullpath); err != nil {
				store.logger.Warn("unable to remove", zap.String("name", store.loggerPrefix+"compactFile"), zap.String("path", fullpath), zap.Error(err))
				if err = store.rename(fullpath, fullpath+".renamed"); err != nil {
					store.logger.Warn("also could not rename", zap.String("name", store.loggerPrefix+"compactFile"), zap.String("path", fullpath), zap.Error(err))
				}
			}
			if blockID != 0 {
				if err := store.closeLocBlock(blockID); err != nil {
					store.logger.Warn("error closing in-memory block", zap.String("name", store.loggerPrefix+"compactFile"), zap.String("filename", nametoc), zap.Error(err))
				}
			}
		}
		store.logger.Debug("stats", zap.String("name", store.loggerPrefix+"compactFile"), zap.String("filename", nametoc), zap.Uint64("count", uint64(atomic.LoadUint32(&count))), zap.Uint64("rewrote", uint64(atomic.LoadUint32(&rewrote))), zap.Uint64("stale", uint64(atomic.LoadUint32(&stale))))
	}
	fpr, err := store.openReadSeeker(fullpathtoc)
	if err != nil {
		store.logger.Warn("error opening", zap.String("name", store.loggerPrefix+"compactFile"), zap.String("path", fullpathtoc), zap.Error(err))
		spindown(false)
		return
	}
	fdc, errs := groupReadTOCEntriesBatched(fpr, blockID, store.compactionState.compactionFreeBatchChans, store.compactionState.compactionPendingBatchChans, controlChan)
	closeIfCloser(fpr)
	for _, err := range errs {
		store.logger.Warn("error from ReadTOCEntriesBatched", zap.String("name", store.loggerPrefix+"compactFile"), zap.String("filename", nametoc), zap.Error(err))
	}
	select {
	case <-controlChan:
		store.logger.Debug("canceled compaction", zap.String("name", store.loggerPrefix+"compactFile"), zap.String("filename", nametoc))
		spindown(false)
		return
	default:
	}
	for i := 0; i < len(store.compactionState.compactionPendingBatchChans); i++ {
		store.compactionState.compactionPendingBatchChans[i] <- nil
	}
	wg.Wait()
	if rec := atomic.LoadUint32(&readErrorCount); rec > 0 {
		// TODO: Eventually, as per the note above, this should remove the
		// unable-to-be-read entries from the locmap so replication can repair
		// them, and then remove the original bad file.
		store.logger.Error("data read errors; file will be retried later", zap.String("name", store.loggerPrefix+"compactFile"), zap.Uint64("errorCount", uint64(rec)), zap.String("filename", nametoc))
		spindown(false)
		return
	}
	if wec := atomic.LoadUint32(&writeErrorCount); wec > 0 {
		// TODO: Eventually, as per the note above, this should disable writes
		// until a person can look at the problem and bring the service back
		// online.
		store.logger.Error("data write errors; will retry later", zap.String("name", store.loggerPrefix+"compactFile"), zap.Uint64("errorCount", uint64(wec)), zap.String("filename", nametoc))
		spindown(false)
		return
	}
	if len(errs) > 0 {
		if fdc == 0 {
			store.logger.Warn("errors and no entries were read; file will be retried later", zap.String("name", store.loggerPrefix+"compactFile"), zap.String("filename", nametoc))
			spindown(false)
			return
		} else {
			store.logger.Warn("errors but some entries were read; assuming the recovery was as good as it could get and removing file", zap.String("name", store.loggerPrefix+"compactFile"), zap.String("filename", nametoc))
		}
	}
	spindown(true)
}
