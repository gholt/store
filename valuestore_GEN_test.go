package store

import "github.com/gholt/locmap"

func lowMemValueStoreConfig() *ValueStoreConfig {
	locmap := locmap.NewValueLocMap(&locmap.ValueLocMapConfig{
		Roots:    1,
		PageSize: 1,
	})
	return &ValueStoreConfig{
		ValueCap:                  1024,
		Workers:                   2,
		ChecksumInterval:          1024,
		PageSize:                  1,
		WritePagesPerWorker:       1,
		ValueLocMap:               locmap,
		MsgCap:                    1,
		FileCap:                   1024 * 1024,
		FileReaders:               2,
		RecoveryBatchSize:         1024,
		TombstoneDiscardBatchSize: 1024,
		OutPullReplicationBloomN:  1000,
	}
}
