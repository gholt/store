package store

import (
	"errors"
	"io"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"golang.org/x/net/context"
)

type valueAuditState struct {
	interval     int
	ageThreshold int64

	startupShutdownLock sync.Mutex
	notifyChan          chan *bgNotification
}

func (store *defaultValueStore) auditConfig(cfg *ValueStoreConfig) {
	store.auditState.interval = cfg.AuditInterval
	store.auditState.ageThreshold = int64(cfg.AuditAgeThreshold) * int64(time.Second)
}

func (store *defaultValueStore) auditStartup() {
	store.auditState.startupShutdownLock.Lock()
	if store.auditState.notifyChan == nil {
		store.auditState.notifyChan = make(chan *bgNotification, 1)
		go store.auditLauncher(store.auditState.notifyChan)
	}
	store.auditState.startupShutdownLock.Unlock()
}

func (store *defaultValueStore) auditShutdown() {
	store.auditState.startupShutdownLock.Lock()
	if store.auditState.notifyChan != nil {
		c := make(chan struct{}, 1)
		store.auditState.notifyChan <- &bgNotification{
			action:   _BG_DISABLE,
			doneChan: c,
		}
		<-c
		store.auditState.notifyChan = nil
	}
	store.auditState.startupShutdownLock.Unlock()
}

func (store *defaultValueStore) AuditPass(ctx context.Context) error {
	store.auditState.startupShutdownLock.Lock()
	if store.auditState.notifyChan == nil {
		store.auditPass(true, make(chan *bgNotification))
	} else {
		c := make(chan struct{}, 1)
		store.auditState.notifyChan <- &bgNotification{
			action:   _BG_PASS,
			doneChan: c,
		}
		<-c
	}
	store.auditState.startupShutdownLock.Unlock()
	return nil
}

func (store *defaultValueStore) auditLauncher(notifyChan chan *bgNotification) {
	interval := float64(store.auditState.interval) * float64(time.Second)
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
				nextNotification = store.auditPass(true, notifyChan)
			case _BG_DISABLE:
				running = false
			default:
				// Critical because there was a coding error that needs to be
				// fixed by a person.
				store.logger.Error("invalid action requested", zap.String("name", store.loggerPrefix+"audit"), zap.Int("action", int(notification.action)))
			}
			notification.doneChan <- struct{}{}
			notification = nextNotification
		} else {
			notification = store.auditPass(false, notifyChan)
		}
	}
}

// NOTE: For now, there is no difference between speed=true and speed=false;
// eventually the background audits will try to slow themselves down to finish
// in approximately the store.auditState.interval.
func (store *defaultValueStore) auditPass(speed bool, notifyChan chan *bgNotification) *bgNotification {
	begin := time.Now()
	defer func() {
		elapsed := time.Now().Sub(begin)
		store.logger.Debug("pass completed", zap.String("name", store.loggerPrefix+"audit"), zap.Duration("elapsed", elapsed))
		atomic.StoreInt64(&store.auditNanoseconds, elapsed.Nanoseconds())
	}()
	names, err := store.readdirnames(store.pathtoc)
	if err != nil {
		store.logger.Warn("error with readdirnames", zap.String("name", store.loggerPrefix+"audit"), zap.String("path", store.pathtoc), zap.Error(err))
		return nil
	}
	shuffledNames := make([]string, len(names))
	store.randMutex.Lock()
	for x, y := range store.rand.Perm(len(names)) {
		shuffledNames[x] = names[y]
	}
	store.randMutex.Unlock()
	names = shuffledNames
	for i := 0; i < len(names); i++ {
		select {
		case notification := <-notifyChan:
			return notification
		default:
		}
		if !strings.HasSuffix(names[i], ".valuetoc") {
			continue
		}
		namets := int64(0)
		if namets, err = strconv.ParseInt(names[i][:len(names[i])-len(".valuetoc")], 10, 64); err != nil {
			store.logger.Warn("bad timestamp in name", zap.String("name", store.loggerPrefix+"audit"), zap.String("name", names[i]))
			continue
		}
		if namets == 0 {
			store.logger.Warn("bad timestamp in name", zap.String("name", store.loggerPrefix+"audit"), zap.String("name", names[i]))
			continue
		}
		if namets == int64(atomic.LoadUint64(&store.activeTOCA)) || namets == int64(atomic.LoadUint64(&store.activeTOCB)) {
			store.logger.Debug("skipping current", zap.String("name", store.loggerPrefix+"audit"), zap.String("name", names[i]))
			continue
		}
		if namets >= time.Now().UnixNano()-store.auditState.ageThreshold {
			store.logger.Debug("skipping young", zap.String("name", store.loggerPrefix+"audit"), zap.String("name", names[i]))
			continue
		}
		store.logger.Debug("checking", zap.String("name", store.loggerPrefix+"audit"), zap.String("name", names[i]))
		failedAudit := uint32(0)
		canceledAudit := uint32(0)
		dataName := names[i][:len(names[i])-3]
		fpr, err := store.openReadSeeker(path.Join(store.path, dataName))
		if err != nil {
			atomic.AddUint32(&failedAudit, 1)
			if store.isNotExist(err) {
				store.logger.Debug("error opening", zap.String("name", store.loggerPrefix+"audit"), zap.String("filename", dataName), zap.Error(err))
			} else {
				store.logger.Warn("error opening", zap.String("name", store.loggerPrefix+"audit"), zap.String("filename", dataName), zap.Error(err))
			}
		} else {
			corruptions, errs := valueChecksumVerify(fpr)
			closeIfCloser(fpr)
			for _, err := range errs {
				if err != io.EOF && err != io.ErrUnexpectedEOF {
					store.logger.Warn("ChecksumVerify error", zap.String("name", store.loggerPrefix+"audit"), zap.String("filename", dataName), zap.Error(err))
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
			nextNotificationChan := make(chan *bgNotification, 1)
			controlChan := make(chan struct{})
			go func() {
				select {
				case n := <-notifyChan:
					if atomic.AddUint32(&canceledAudit, 1) == 0 {
						close(controlChan)
					}
					nextNotificationChan <- n
				case <-controlChan:
					nextNotificationChan <- nil
				}
			}()
			wg := &sync.WaitGroup{}
			wg.Add(len(pendingBatchChans))
			for i := 0; i < len(pendingBatchChans); i++ {
				go func(pendingBatchChan chan []valueTOCEntry, freeBatchChan chan []valueTOCEntry) {
					for {
						batch := <-pendingBatchChan
						if batch == nil {
							break
						}
						if atomic.LoadUint32(&failedAudit) == 0 {
							for j := 0; j < len(batch); j++ {
								wr := &batch[j]
								if wr.TimestampBits&_TSB_DELETION != 0 {
									continue
								}
								if valueInCorruptRange(wr.Offset, wr.Length, corruptions) {
									if atomic.AddUint32(&failedAudit, 1) == 0 {
										close(controlChan)
									}
									break
								}
							}
						}
						freeBatchChan <- batch
					}
					wg.Done()
				}(pendingBatchChans[i], freeBatchChans[i])
			}
			fpr, err = store.openReadSeeker(path.Join(store.pathtoc, names[i]))
			if err != nil {
				atomic.AddUint32(&failedAudit, 1)
				if !store.isNotExist(err) {
					store.logger.Warn("error opening", zap.String("name", store.loggerPrefix+"audit"), zap.String("filename", names[i]), zap.Error(err))
				}
			} else {
				// NOTE: The block ID is unimportant in this context, so it's
				// just set 1 and ignored elsewhere.
				_, errs := valueReadTOCEntriesBatched(fpr, 1, freeBatchChans, pendingBatchChans, controlChan)
				closeIfCloser(fpr)
				if len(errs) > 0 {
					atomic.AddUint32(&failedAudit, 1)
					for _, err := range errs {
						store.logger.Warn("ReadTOCEntriesBatched error", zap.String("name", store.loggerPrefix+"audit"), zap.String("filename", names[i]), zap.Error(err))
					}
				}
			}
			for i := 0; i < len(pendingBatchChans); i++ {
				pendingBatchChans[i] <- nil
			}
			wg.Wait()
			close(controlChan)
			if n := <-nextNotificationChan; n != nil {
				return n
			}
		}
		if atomic.LoadUint32(&canceledAudit) != 0 {
			store.logger.Debug("canceled during", zap.String("name", store.loggerPrefix+"audit"), zap.String("filename", names[i]))
		} else if atomic.LoadUint32(&failedAudit) == 0 {
			store.logger.Debug("passed", zap.String("name", store.loggerPrefix+"audit"), zap.String("filename", names[i]))
		} else {
			store.logger.Warn("failed", zap.String("name", store.loggerPrefix+"audit"), zap.String("filename", names[i]))
			nextNotificationChan := make(chan *bgNotification, 1)
			controlChan := make(chan struct{})
			controlChan2 := make(chan struct{})
			go func() {
				select {
				case n := <-notifyChan:
					close(controlChan)
					nextNotificationChan <- n
				case <-controlChan2:
					nextNotificationChan <- nil
				}
			}()
			store.compactFile(names[i], store.locBlockIDFromTimestampnano(namets), controlChan, "auditPass")
			close(controlChan2)
			if n := <-nextNotificationChan; n != nil {
				return n
			}
			go func() {
				store.logger.Warn("all audit actions require store restarts at this time", zap.String("name", store.loggerPrefix+"audit"))
				store.Shutdown(context.Background())
				store.restartChan <- errors.New("audit failure occurred requiring a restart")
			}()
			return &bgNotification{
				action:   _BG_DISABLE,
				doneChan: make(chan struct{}, 1),
			}
		}
	}
	return nil
}
