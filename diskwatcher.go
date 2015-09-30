package valuestore

import (
	"time"

	"github.com/ricochet2200/go-disk-usage/du"
)

type diskWatcherState struct {
	freeDisableThreshold   uint64
	freeReenableThreshold  uint64
	usageDisableThreshold  float32
	usageReenableThreshold float32
}

func (vs *DefaultValueStore) diskWatcherConfig(cfg *Config) {
	vs.diskWatcherState.freeDisableThreshold = cfg.FreeDisableThreshold
	vs.diskWatcherState.freeReenableThreshold = cfg.FreeReenableThreshold
	vs.diskWatcherState.usageDisableThreshold = cfg.UsageDisableThreshold
	vs.diskWatcherState.usageReenableThreshold = cfg.UsageReenableThreshold
}

func (vs *DefaultValueStore) diskWatcherLaunch() {
	go vs.diskWatcher()
}

func (vs *DefaultValueStore) diskWatcher() {
	disabled := false
	for {
		time.Sleep(time.Minute)
		if disabled {
			if vs.diskWatcherReenableCheck(vs.path) || vs.diskWatcherReenableCheck(vs.path) {
				vs.enableWrites(false) // false indicates non-user call
				disabled = false
			}
		} else {
			if vs.diskWatcherDisableCheck(vs.path) || vs.diskWatcherDisableCheck(vs.path) {
				vs.disableWrites(false) // false indicates non-user call
				disabled = true
			}
		}
	}
}

func (vs *DefaultValueStore) diskWatcherDisableCheck(path string) bool {
	if vs.diskWatcherState.freeDisableThreshold == 0 && vs.diskWatcherState.usageDisableThreshold == 0 {
		return false
	}
	du := du.NewDiskUsage(path)
	if vs.diskWatcherState.freeDisableThreshold != 0 && du.Free() <= vs.diskWatcherState.freeDisableThreshold {
		vs.logCritical("%s has passed the free threshold %d for automatic disabling; %d left free\n", path, vs.diskWatcherState.freeDisableThreshold, du.Free())
		return true
	}
	if vs.diskWatcherState.usageDisableThreshold != 0 && du.Usage() >= vs.diskWatcherState.usageDisableThreshold {
		vs.logCritical("%s has passed the usage threshold %.02f%% for automatic disabling; %.02f%% current usage\n", path, 100*vs.diskWatcherState.usageDisableThreshold, 100*du.Free())
		return true
	}
	return false
}

func (vs *DefaultValueStore) diskWatcherReenableCheck(path string) bool {
	if vs.diskWatcherState.freeReenableThreshold == 0 && vs.diskWatcherState.usageReenableThreshold == 0 {
		return false
	}
	du := du.NewDiskUsage(path)
	if vs.diskWatcherState.freeReenableThreshold != 0 && du.Free() >= vs.diskWatcherState.freeReenableThreshold {
		vs.logCritical("%s has passed the free threshold %d for automatic re-enabling; %d left free\n", path, vs.diskWatcherState.freeReenableThreshold, du.Free())
		return true
	}
	if vs.diskWatcherState.usageReenableThreshold != 0 && du.Usage() <= vs.diskWatcherState.usageReenableThreshold {
		vs.logCritical("%s has passed the usage threshold %.02f%% for automatic re-enabling; %.02f%% current usage\n", path, 100*vs.diskWatcherState.usageReenableThreshold, 100*du.Free())
		return true
	}
	return false
}
