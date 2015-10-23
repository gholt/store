package valuestore

import "github.com/gholt/valuelocmap"

func lowMemGroupStoreConfig() *GroupStoreConfig {
	locmap := valuelocmap.NewGroupLocMap(&valuelocmap.GroupLocMapConfig{
		Roots:    1,
		PageSize: 1,
	})
	return &GroupStoreConfig{
		ValueCap:                  1024,
		Workers:                   2,
		ChecksumInterval:          1024,
		PageSize:                  1,
		WritePagesPerWorker:       1,
		GroupLocMap:               locmap,
		MsgCap:                    1,
		FileCap:                   1024 * 1024,
		FileReaders:               2,
		RecoveryBatchSize:         1024,
		TombstoneDiscardBatchSize: 1024,
		OutPullReplicationBloomN:  1000,
	}
}
