package store

import (
	"sync/atomic"
	"time"
)

type valueFlusherState struct {
	flusherThreshold int32
}

func (store *DefaultValueStore) flusherConfig(cfg *ValueStoreConfig) {
	store.flusherState.flusherThreshold = cfg.FlusherThreshold
}

func (store *DefaultValueStore) flusherLaunch() {
	go store.flusher()
}

func (store *DefaultValueStore) flusher() {
	for {
		time.Sleep(time.Minute)
		m := atomic.LoadInt32(&store.modifications)
		atomic.AddInt32(&store.modifications, -m)
		if m < store.flusherState.flusherThreshold {
			store.Flush()
		}
	}
}
