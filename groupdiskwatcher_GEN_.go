package store

import (
	"sync/atomic"
	"time"

	"github.com/ricochet2200/go-disk-usage/du"
)

type groupDiskWatcherState struct {
	freeDisableThreshold   uint64
	freeReenableThreshold  uint64
	usageDisableThreshold  float32
	usageReenableThreshold float32
	free                   uint64
	used                   uint64
	size                   uint64
	freetoc                uint64
	usedtoc                uint64
	sizetoc                uint64
}

func (store *DefaultGroupStore) diskWatcherConfig(cfg *GroupStoreConfig) {
	store.diskWatcherState.freeDisableThreshold = cfg.FreeDisableThreshold
	store.diskWatcherState.freeReenableThreshold = cfg.FreeReenableThreshold
	store.diskWatcherState.usageDisableThreshold = cfg.UsageDisableThreshold
	store.diskWatcherState.usageReenableThreshold = cfg.UsageReenableThreshold
}

func (store *DefaultGroupStore) diskWatcherLaunch() {
	go store.diskWatcher()
}

func (store *DefaultGroupStore) diskWatcher() {
	disabled := false
	for {
		time.Sleep(time.Minute)
		u := du.NewDiskUsage(store.path)
		utoc := u
		if store.pathtoc != store.path {
			utoc = du.NewDiskUsage(store.pathtoc)
		}
		free := u.Free()
		used := u.Used()
		size := u.Size()
		usage := u.Usage()
		freetoc := utoc.Free()
		usedtoc := utoc.Used()
		sizetoc := utoc.Size()
		usagetoc := utoc.Usage()
		atomic.StoreUint64(&store.diskWatcherState.free, free)
		atomic.StoreUint64(&store.diskWatcherState.used, used)
		atomic.StoreUint64(&store.diskWatcherState.size, size)
		atomic.StoreUint64(&store.diskWatcherState.freetoc, freetoc)
		atomic.StoreUint64(&store.diskWatcherState.usedtoc, usedtoc)
		atomic.StoreUint64(&store.diskWatcherState.sizetoc, sizetoc)
		if disabled {
			if (store.diskWatcherState.freeReenableThreshold == 0 || (free >= store.diskWatcherState.freeReenableThreshold && freetoc >= store.diskWatcherState.freeReenableThreshold)) && (store.diskWatcherState.usageReenableThreshold == 0 || (usage <= store.diskWatcherState.usageReenableThreshold && usagetoc <= store.diskWatcherState.usageReenableThreshold)) {
				store.logCritical("passed the free/usage threshold for automatic re-enabling\n")
				store.enableWrites(false) // false indicates non-user call
				disabled = false
			}
		} else {
			if (store.diskWatcherState.freeDisableThreshold != 0 && (free <= store.diskWatcherState.freeDisableThreshold || freetoc <= store.diskWatcherState.freeDisableThreshold)) || (store.diskWatcherState.usageDisableThreshold != 0 && (usage >= store.diskWatcherState.usageDisableThreshold || usagetoc >= store.diskWatcherState.usageDisableThreshold)) {
				store.logCritical("passed the free/usage threshold for automatic disabling\n")
				store.disableWrites(false) // false indicates non-user call
				disabled = true
			}
		}
	}
}
