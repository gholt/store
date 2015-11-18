package store

import (
	"sync/atomic"
	"time"
)

type groupFlusherState struct {
	flusherThreshold int32
}

func (store *DefaultGroupStore) flusherConfig(cfg *GroupStoreConfig) {
	store.flusherState.flusherThreshold = cfg.FlusherThreshold
}

func (store *DefaultGroupStore) flusherLaunch() {
	go store.flusher()
}

func (store *DefaultGroupStore) flusher() {
	for {
		time.Sleep(time.Minute)
		m := atomic.LoadInt32(&store.modifications)
		atomic.AddInt32(&store.modifications, -m)
		if m < store.flusherState.flusherThreshold {
			store.Flush()
		}
	}
}
