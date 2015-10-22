// Package valuestore provides a disk-backed data structure for use in storing
// []byte values referenced by 128 bit keys with options for replication.
//
// It can handle billions of keys (as memory allows) and full concurrent access
// across many cores. All location information about each key is stored in
// memory for speed, but values are stored on disk with the exception of
// recently written data being buffered first and batched to disk later.
//
// This has been written with SSDs in mind, but spinning drives should work
// also; though storing valuestoc files (Table Of Contents, key location
// information) on a separate disk from values files is recommended in that
// case.
//
// Each key is two 64bit values, known as keyA and keyB uint64 values. These
// are usually created by a hashing function of the key name, but that duty is
// left outside this package.
//
// Each modification is recorded with an int64 timestamp that is the number of
// microseconds since the Unix epoch (see
// github.com/gholt/brimtime.TimeToUnixMicro). With a write and delete for the
// exact same timestamp, the delete wins. This allows a delete to be issued for
// a specific write without fear of deleting any newer write.
//
// Internally, each modification is stored with a uint64 timestamp that is
// equivalent to (brimtime.TimeToUnixMicro(time.Now())<<8) with the lowest 8
// bits used to indicate deletions and other bookkeeping items. This means that
// the allowable time range is 1970-01-01 00:00:00 +0000 UTC (+1 microsecond
// because all zeroes indicates a missing item) to 4253-05-31 22:20:37.927935
// +0000 UTC. There are constants TIMESTAMPMICRO_MIN and TIMESTAMPMICRO_MAX
// available for bounding usage.
//
// There are background tasks for:
//
// * TombstoneDiscard: This will discard older tombstones (deletion markers).
// Tombstones are kept for Config.TombstoneAge seconds and are used to ensure a
// replicated older value doesn't resurrect a deleted value. But, keeping all
// tombstones for all time is a waste of resources, so they are discarded over
// time. Config.TombstoneAge controls how long they should be kept and should
// be set to an amount greater than several replication passes.
//
// * PullReplication: This will continually send out pull replication requests
// for all the partitions the ValueStore is responsible for, as determined by
// the Config.MsgRing. The other responsible parties will respond to these
// requests with data they have that was missing from the pull replication
// request. Bloom filters are used to reduce bandwidth which has the downside
// that a very small percentage of items may be missed each pass. A moving salt
// is used with each bloom filter so that after a few passes there is an
// exceptionally high probability that all items will be accounted for.
//
// * PushReplication: This will continually send out any data for any
// partitions the ValueStore is *not* responsible for, as determined by the
// Config.MsgRing. The responsible parties will respond to these requests with
// acknowledgements of the data they received, allowing the requester to
// discard the out of place data.
package valuestore

// got is at https://github.com/gholt/got
//go:generate got store.got valuestore_GEN_.go TT=VALUE T=Value t=value
//go:generate got store.got groupstore_GEN_.go TT=GROUP T=Group t=group
//go:generate got config.got valueconfig_GEN_.go TT=VALUE T=Value t=value
//go:generate got config.got groupconfig_GEN_.go TT=GROUP T=Group t=group
//go:generate got mem.got valuemem_GEN_.go TT=VALUE T=Value t=value
//go:generate got mem.got groupmem_GEN_.go TT=GROUP T=Group t=group
//go:generate got mem_test.got valuemem_GEN_test.go TT=VALUE T=Value t=value
//go:generate got mem_test.got groupmem_GEN_test.go TT=GROUP T=Group t=group
//go:generate got file.got valuefile_GEN_.go TT=VALUE T=Value t=value
//go:generate got file.got groupfile_GEN_.go TT=GROUP T=Group t=group
//go:generate got file_test.got valuefile_GEN_test.go TT=VALUE T=Value t=value
//go:generate got file_test.got groupfile_GEN_test.go TT=GROUP T=Group t=group
//go:generate got bulkset.got valuebulkset_GEN_.go TT=VALUE T=Value t=value
//go:generate got bulkset.got groupbulkset_GEN_.go TT=GROUP T=Group t=group
//go:generate got bulkset_test.got valuebulkset_GEN_test.go TT=VALUE T=Value t=value
//go:generate got bulkset_test.got groupbulkset_GEN_test.go TT=GROUP T=Group t=group
//go:generate got bulksetack.got valuebulksetack_GEN_.go TT=VALUE T=Value t=value
//go:generate got bulksetack.got groupbulksetack_GEN_.go TT=GROUP T=Group t=group
//go:generate got bulksetack_test.got valuebulksetack_GEN_test.go TT=VALUE T=Value t=value
//go:generate got bulksetack_test.got groupbulksetack_GEN_test.go TT=GROUP T=Group t=group
//go:generate got pullreplication.got valuepullreplication_GEN_.go TT=VALUE T=Value t=value
//go:generate got pullreplication.got grouppullreplication_GEN_.go TT=GROUP T=Group t=group
//go:generate got pullreplication_test.got valuepullreplication_GEN_test.go TT=VALUE T=Value t=value
//go:generate got pullreplication_test.got grouppullreplication_GEN_test.go TT=GROUP T=Group t=group
//go:generate got ktbloomfilter.got valuektbloomfilter_GEN_.go TT=VALUE T=Value t=value
//go:generate got ktbloomfilter.got groupktbloomfilter_GEN_.go TT=GROUP T=Group t=group
//go:generate got ktbloomfilter_test.got valuektbloomfilter_GEN_test.go TT=VALUE T=Value t=value
//go:generate got ktbloomfilter_test.got groupktbloomfilter_GEN_test.go TT=GROUP T=Group t=group
//go:generate got pushreplication.got valuepushreplication_GEN_.go TT=VALUE T=Value t=value
//go:generate got pushreplication.got grouppushreplication_GEN_.go TT=GROUP T=Group t=group
//go:generate got tombstonediscard.got valuetombstonediscard_GEN_.go TT=VALUE T=Value t=value
//go:generate got tombstonediscard.got grouptombstonediscard_GEN_.go TT=GROUP T=Group t=group
//go:generate got compaction.got valuecompaction_GEN_.go TT=VALUE T=Value t=value
//go:generate got compaction.got groupcompaction_GEN_.go TT=GROUP T=Group t=group
//go:generate got diskwatcher.got valuediskwatcher_GEN_.go TT=VALUE T=Value t=value
//go:generate got diskwatcher.got groupdiskwatcher_GEN_.go TT=GROUP T=Group t=group
//go:generate got stats.got valuestats_GEN_.go TT=VALUE T=Value t=value
//go:generate got stats.got groupstats_GEN_.go TT=GROUP T=Group t=group

import (
	"errors"
	"io"
	"math"
	"os"
)

const (
	_TSB_UTIL_BITS = 8
	_TSB_INACTIVE  = 0xff
	_TSB_DELETION  = 0x80
	// _TSB_COMPACTION_REWRITE indicates an item is being rewritten as part of
	// compaction to the last disk file.
	_TSB_COMPACTION_REWRITE = 0x01
	// _TSB_LOCAL_REMOVAL indicates an item to be removed locally due to push
	// replication (local store wasn't considered responsible for the item
	// according to the ring) or a deletion marker expiration. An item marked
	// for local removal will be retained in memory until the local removal
	// marker is written to disk.
	_TSB_LOCAL_REMOVAL = 0x02
)

const (
	TIMESTAMPMICRO_MIN = int64(uint64(1) << _TSB_UTIL_BITS)
	TIMESTAMPMICRO_MAX = int64(uint64(math.MaxUint64) >> _TSB_UTIL_BITS)
)

var ErrNotFound error = errors.New("not found")
var ErrDisabled error = errors.New("disabled")

var toss []byte = make([]byte, 65536)

func osOpenReadSeeker(name string) (io.ReadSeeker, error) {
	return os.Open(name)
}

func osCreateWriteCloser(name string) (io.WriteCloser, error) {
	return os.Create(name)
}

type LogFunc func(format string, v ...interface{})

type backgroundNotification struct {
	enable   bool
	disable  bool
	doneChan chan struct{}
}
