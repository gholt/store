package valuestore

import (
	"time"

	"github.com/ricochet2200/go-disk-usage/du"
)

type valueDiskWatcherState struct {
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

func (store *DefaultValueStore) diskWatcherConfig(cfg *ValueStoreConfig) {
	store.diskWatcherState.freeDisableThreshold = cfg.FreeDisableThreshold
	store.diskWatcherState.freeReenableThreshold = cfg.FreeReenableThreshold
	store.diskWatcherState.usageDisableThreshold = cfg.UsageDisableThreshold
	store.diskWatcherState.usageReenableThreshold = cfg.UsageReenableThreshold
}

func (store *DefaultValueStore) diskWatcherLaunch() {
	go store.diskWatcher()
}

func (store *DefaultValueStore) diskWatcher() {
	disabled := false
	for {
		time.Sleep(time.Minute)
		u := du.NewDiskUsage(store.path)
		utoc := u
		if store.pathtoc != store.path {
			utoc = du.NewDiskUsage(store.pathtoc)
		}
		store.diskWatcherState.free = u.Free()
		store.diskWatcherState.used = u.Used()
		store.diskWatcherState.size = u.Size()
		usage := u.Usage()
		store.diskWatcherState.freetoc = utoc.Free()
		store.diskWatcherState.usedtoc = utoc.Used()
		store.diskWatcherState.sizetoc = utoc.Size()
		usagetoc := utoc.Usage()
		if disabled {
			if (store.diskWatcherState.freeReenableThreshold == 0 || (store.diskWatcherState.free >= store.diskWatcherState.freeReenableThreshold && store.diskWatcherState.freetoc >= store.diskWatcherState.freeReenableThreshold)) && (store.diskWatcherState.usageReenableThreshold == 0 || (usage <= store.diskWatcherState.usageReenableThreshold && usagetoc <= store.diskWatcherState.usageReenableThreshold)) {
				store.logCritical("passed the free/usage threshold for automatic re-enabling\n")
				store.enableWrites(false) // false indicates non-user call
				disabled = false
			}
		} else {
			if (store.diskWatcherState.freeDisableThreshold != 0 && (store.diskWatcherState.free <= store.diskWatcherState.freeDisableThreshold || store.diskWatcherState.freetoc <= store.diskWatcherState.freeDisableThreshold)) || (store.diskWatcherState.usageDisableThreshold != 0 && (usage >= store.diskWatcherState.usageDisableThreshold || usagetoc >= store.diskWatcherState.usageDisableThreshold)) {
				store.logCritical("passed the free/usage threshold for automatic disabling\n")
				store.disableWrites(false) // false indicates non-user call
				disabled = true
			}
		}
	}
}
