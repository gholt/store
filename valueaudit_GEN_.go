package store

import (
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type valueAuditState struct {
	interval     int
	ageThreshold int64
	notifyChan   chan *backgroundNotification
	abort        uint32
}

func (store *DefaultValueStore) auditConfig(cfg *ValueStoreConfig) {
	store.auditState.interval = cfg.AuditInterval
	store.auditState.ageThreshold = int64(cfg.AuditAgeThreshold) * int64(time.Second)
	store.auditState.notifyChan = make(chan *backgroundNotification, 1)
}

func (store *DefaultValueStore) auditLaunch() {
	go store.auditLauncher()
}

// DisableAudit will stop any audit passes until EnableAudit is called. An
// audit pass checks on-disk data for errors.
func (store *DefaultValueStore) DisableAudit() {
	c := make(chan struct{}, 1)
	store.auditState.notifyChan <- &backgroundNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableAudit will resume audit passes. An audit pass checks on-disk data for
// errors.
func (store *DefaultValueStore) EnableAudit() {
	c := make(chan struct{}, 1)
	store.auditState.notifyChan <- &backgroundNotification{
		enable:   true,
		doneChan: c,
	}
	<-c
}

// AuditPass will immediately execute a pass at full speed to check the on-disk
// data for errors rather than waiting for the next interval to run the
// standard slow-audit pass. If a pass is currently executing, it will be
// stopped and restarted so that a call to this function ensures one complete
// pass occurs.
func (store *DefaultValueStore) AuditPass() {
	atomic.StoreUint32(&store.auditState.abort, 1)
	c := make(chan struct{}, 1)
	store.auditState.notifyChan <- &backgroundNotification{doneChan: c}
	<-c
}

func (store *DefaultValueStore) auditLauncher() {
	var enabled bool
	interval := float64(store.auditState.interval) * float64(time.Second)
	store.randMutex.Lock()
	nextRun := time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
	store.randMutex.Unlock()
	for {
		var notification *backgroundNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-store.auditState.notifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-store.auditState.notifyChan:
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
				atomic.StoreUint32(&store.auditState.abort, 1)
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&store.auditState.abort, 0)
			store.auditPass(true)
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&store.auditState.abort, 0)
			store.auditPass(false)
		}
	}
}

// NOTE: For now, there is no difference between speed=true and speed=false;
// eventually the background audits will try to slow themselves down to finish
// in approximately the store.auditState.interval.
func (store *DefaultValueStore) auditPass(speed bool) {
	if store.logDebug != nil {
		begin := time.Now()
		defer func() {
			store.logDebug("audit: took %s\n", time.Now().Sub(begin))
		}()
	}
	fp, err := os.Open(store.pathtoc)
	if err != nil {
		store.logError("audit: %s\n", err)
		return
	}
	names, err := fp.Readdirnames(-1)
	fp.Close()
	if err != nil {
		store.logError("audit: %s\n", err)
		return
	}
	shuffledNames := make([]string, len(names))
	store.randMutex.Lock()
	for x, y := range store.rand.Perm(len(names)) {
		shuffledNames[x] = names[y]
	}
	store.randMutex.Unlock()
	names = shuffledNames
	for i := 0; i < len(names); i++ {
		if !strings.HasSuffix(names[i], ".valuetoc") {
			continue
		}
		namets := int64(0)
		if namets, err = strconv.ParseInt(names[i][:len(names[i])-len(".valuetoc")], 10, 64); err != nil {
			store.logError("audit: bad timestamp in name: %#v\n", names[i])
			continue
		}
		if namets == 0 {
			store.logError("audit: bad timestamp in name: %#v\n", names[i])
			continue
		}
		if namets == int64(atomic.LoadUint64(&store.activeTOCA)) || namets == int64(atomic.LoadUint64(&store.activeTOCB)) {
			continue
		}
		if namets >= time.Now().UnixNano()-store.auditState.ageThreshold {
			continue
		}
		failedAudit := uint32(0)
		dataName := names[i][:len(names[i])-3]
		fpr, err := osOpenReadSeeker(dataName)
		if err != nil {
			atomic.AddUint32(&failedAudit, 1)
			if os.IsNotExist(err) {
				if store.logDebug != nil {
					store.logDebug("audit: error opening %s: %s\n", dataName, err)
				}
			} else {
				store.logError("audit: error opening %s: %s\n", dataName, err)
			}
		} else {
			corruptions, errs := valueChecksumVerify(fpr)
			closeIfCloser(fpr)
			for _, err := range errs {
				if err != io.EOF && err != io.ErrUnexpectedEOF {
					store.logError("audit: error with %s: %s", dataName, err)
				}
			}
			workers := uint64(1)
			pendingBatchChans := make([]chan []valueTOCEntry, workers)
			freeBatchChans := make([]chan []valueTOCEntry, len(pendingBatchChans))
			for i := 0; i < len(pendingBatchChans); i++ {
				pendingBatchChans[i] = make(chan []valueTOCEntry, 3)
				freeBatchChans[i] = make(chan []valueTOCEntry, cap(pendingBatchChans[i]))
				for j := 0; j < cap(freeBatchChans[i]); j++ {
					freeBatchChans[i] <- make([]valueTOCEntry, store.recoveryBatchSize)
				}
			}
			controlChan := make(chan struct{})
			wg := &sync.WaitGroup{}
			wg.Add(len(pendingBatchChans))
			for i := 0; i < len(pendingBatchChans); i++ {
				go func(pendingBatchChan chan []valueTOCEntry, freeBatchChan chan []valueTOCEntry) {
					for {
						batch := <-pendingBatchChan
						if batch == nil {
							break
						}
						// TODO: if atomic.LoadUint32(&failedAudit) == 0 {
						for j := 0; j < len(batch); j++ {
							wr := &batch[j]
							if wr.TimestampBits&_TSB_DELETION != 0 {
								continue
							}
							if valueInCorruptRange(wr.Offset, wr.Length, corruptions) {
								/* TODO: v := */ atomic.AddUint32(&failedAudit, 1)
								/* TODO: Later we'll abort on first error; for now, I want to test how many it detects.
								   if v == 0 {
								       close(controlChan)
								   }
								   break
								*/
							}
						}
						// TODO: }
						freeBatchChan <- batch
					}
					wg.Done()
				}(pendingBatchChans[i], freeBatchChans[i])
			}
			fpr, err = osOpenReadSeeker(names[i])
			if err != nil {
				atomic.AddUint32(&failedAudit, 1)
				if !os.IsNotExist(err) {
					store.logError("audit: error opening %s: %s\n", names[i], err)
				}
			} else {
				// NOTE: The block ID is unimportant in this context, so it's
				// just set 1 and ignored elsewhere.
				_, errs := valueReadTOCEntriesBatched(fpr, 1, freeBatchChans, pendingBatchChans, controlChan)
				closeIfCloser(fpr)
				if len(errs) > 0 {
					atomic.AddUint32(&failedAudit, 1)
					for _, err := range errs {
						store.logError("audit: error with %s: %s", names[i], err)
					}
				}
			}
			for i := 0; i < len(pendingBatchChans); i++ {
				pendingBatchChans[i] <- nil
			}
			wg.Wait()
		}
		if atomic.LoadUint32(&failedAudit) == 0 {
			if store.logDebug != nil {
				store.logDebug("audit: passed %s", names[i])
			}
		} else {
			store.logError("audit: failed %s, %d affected", names[i], atomic.LoadUint32(&failedAudit)) // TODO: Remove the affected part
			// TODO: Actually do something to recover from the issue as best as
			// possible. I'm thinking this will act like compaction, rewriting
			// all the good entries it can, but also deliberately removing any
			// known bad entries from the locmap so that replication can get
			// them back in place from other servers. Also, once done
			// recovering the entries from the file set as best as possible, a
			// reload of the whole store is needed in case some entries weren't
			// even discoverable. A full reload of the store will mean the new
			// locmap won't have the completely missing entries allowing
			// replication to kick in.
		}
	}
}
