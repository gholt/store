package valuestore

import (
	"bytes"
	"encoding/binary"
	//"github.com/gholt/brimtime"
	"github.com/spaolacci/murmur3"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// TODO: stop/pause/resume (blockid lookup)
// TODO: fan out lookups
// TODO: on the fly adjustable aggressiveness
// TODO: alerting/logging on bad checksum's or corrupt files

type compactionState struct {
	interval   int
	notifyChan chan *backgroundNotification
	abort      uint32
}

func (vs *DefaultValueStore) compactionInit(cfg *config) {
	vs.compactionState.interval = cfg.compactionInterval
	vs.compactionState.notifyChan = make(chan *backgroundNotification, 1)
	go vs.compactionLauncher()
}

// DisableCompaction will stop any compaction passes until
// EnableCompaction is called. A compaction pass searches for files
// with a percentage of XX deleted entries.
func (vs *DefaultValueStore) DisableCompaction() {
	c := make(chan struct{}, 1)
	vs.compactionState.notifyChan <- &backgroundNotification{
		disable:  true,
		doneChan: c,
	}
	<-c
}

// EnableCompaction will resume compaction passes.
// A compaction pass searches for files with a percentage of XX deleted
// entries.
func (vs *DefaultValueStore) EnableCompaction() {
	c := make(chan struct{}, 1)
	vs.compactionState.notifyChan <- &backgroundNotification{
		enable:   true,
		doneChan: c,
	}
	<-c
}

// CompactionPass will immediately execute a compaction pass to compact stale files.
func (vs *DefaultValueStore) CompactionPass() {
	atomic.StoreUint32(&vs.compactionState.abort, 1)
	c := make(chan struct{}, 1)
	vs.compactionState.notifyChan <- &backgroundNotification{doneChan: c}
	<-c
}

func (vs *DefaultValueStore) compactionLauncher() {
	var enabled bool
	interval := float64(vs.compactionState.interval) * float64(time.Second)
	nextRun := time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
	for {
		var notification *backgroundNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-vs.compactionState.notifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-vs.compactionState.notifyChan:
			default:
			}
		}
		nextRun = time.Now().Add(time.Duration(interval + interval*vs.rand.NormFloat64()*0.1))
		if notification != nil {
			if notification.enable {
				enabled = true
				notification.doneChan <- struct{}{}
				continue
			}
			if notification.disable {
				atomic.StoreUint32(&vs.compactionState.abort, 1)
				enabled = false
				notification.doneChan <- struct{}{}
				continue
			}
			atomic.StoreUint32(&vs.compactionState.abort, 0)
			vs.compactionPass()
			notification.doneChan <- struct{}{}
		} else if enabled {
			atomic.StoreUint32(&vs.compactionState.abort, 0)
			vs.compactionPass()
		}
	}
}

func (vs *DefaultValueStore) compactionPass() {
	if vs.logDebug != nil {
		begin := time.Now()
		defer func() {
			vs.logDebug.Printf("compaction pass took %s", time.Now().Sub(begin))
		}()
	}
	vs.doCompaction()
}

func (vs *DefaultValueStore) sampleTOC(name string) (int, int, error) {
	// TODO: scan for random samples, rather than reading the *whole* thing
	count := 0
	stale := 0
	fromDiskBuf := make([]byte, vs.checksumInterval+4)
	fromDiskOverflow := make([]byte, 0, 32)
	fp, err := os.Open(path.Join(vs.pathtoc, name))
	if err != nil {
		vs.logError.Printf("error opening %s: %s\n", name, err)
		return 0, 0, err
	}
	checksumFailures := 0
	first := true
	terminated := false
	fromDiskOverflow = fromDiskOverflow[:0]
	for {
		n, err := io.ReadFull(fp, fromDiskBuf)
		if n < 4 {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				vs.logError.Printf("error reading %s: %s\n", name, err)
			}
			break
		}
		n -= 4
		if murmur3.Sum32(fromDiskBuf[:n]) != binary.BigEndian.Uint32(fromDiskBuf[n:]) {
			checksumFailures++
		} else {
			j := 0
			if first {
				if !bytes.Equal(fromDiskBuf[:28], []byte("VALUESTORETOC v0            ")) {
					vs.logError.Printf("bad header: %s\n", name)
					break
				}
				if binary.BigEndian.Uint32(fromDiskBuf[28:]) != vs.checksumInterval {
					vs.logError.Printf("bad header checksum interval: %s\n", name)
					break
				}
				j += 32
				first = false
			}
			if n < int(vs.checksumInterval) {
				if binary.BigEndian.Uint32(fromDiskBuf[n-16:]) != 0 {
					vs.logError.Printf("bad terminator size marker: %s\n", name)
					break
				}
				if !bytes.Equal(fromDiskBuf[n-4:n], []byte("TERM")) {
					vs.logError.Printf("bad terminator: %s\n", name)
					break
				}
				n -= 16
				terminated = true
			}
			if len(fromDiskOverflow) > 0 {
				j += 32 - len(fromDiskOverflow)
				fromDiskOverflow = append(fromDiskOverflow, fromDiskBuf[j-32+len(fromDiskOverflow):j]...)
				keyB := binary.BigEndian.Uint64(fromDiskOverflow[8:])
				keyA := binary.BigEndian.Uint64(fromDiskOverflow)
				timestampbits := binary.BigEndian.Uint64(fromDiskOverflow[16:])
				fromDiskOverflow = fromDiskOverflow[:0]
				tsm, _, _, _ := vs.lookup(keyA, keyB)
				if int64(tsm>>_TSB_UTIL_BITS) != int64(timestampbits>>_TSB_UTIL_BITS) {
					count++
					stale++
				} else {
					count++
				}
			}
			for ; j+32 <= n; j += 32 {
				keyB := binary.BigEndian.Uint64(fromDiskBuf[j+8:])
				keyA := binary.BigEndian.Uint64(fromDiskBuf[j:])
				timestampbits := binary.BigEndian.Uint64(fromDiskBuf[j+16:])
				tsm, _, _, _ := vs.lookup(keyA, keyB)
				if int64(tsm>>_TSB_UTIL_BITS) != int64(timestampbits>>_TSB_UTIL_BITS) {
					count++
					stale++
				} else {
					count++
				}
			}
			if j != n {
				fromDiskOverflow = fromDiskOverflow[:n-j]
				copy(fromDiskOverflow, fromDiskBuf[j:])
			}
		}
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			vs.logError.Printf("error reading %s: %s\n", name, err)
			break
		}

	}
	fp.Close()
	if !terminated {
		vs.logError.Printf("early end of file: %s\n", name)
	}
	if checksumFailures > 0 {
		vs.logWarning.Printf("%d checksum failures for %s\n", checksumFailures, name)
	}
	return count, stale, nil

}

func (vs *DefaultValueStore) compactFile(name string) bool {
	fromDiskBuf := make([]byte, vs.checksumInterval+4)
	fromDiskOverflow := make([]byte, 0, 32)
	fp, err := os.Open(path.Join(vs.pathtoc, name))
	if err != nil {
		vs.logError.Printf("error opening %s: %s\n", name, err)
		return false
	}
	checksumFailures := 0
	count := 0
	rewrote := 0
	stale := 0
	first := true
	terminated := false
	fromDiskOverflow = fromDiskOverflow[:0]
	for {
		n, err := io.ReadFull(fp, fromDiskBuf)
		if n < 4 {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				vs.logError.Printf("error reading %s: %s\n", name, err)
				return false
			}
			break
		}
		n -= 4
		if murmur3.Sum32(fromDiskBuf[:n]) != binary.BigEndian.Uint32(fromDiskBuf[n:]) {
			checksumFailures++
		} else {
			j := 0
			if first {
				if !bytes.Equal(fromDiskBuf[:28], []byte("VALUESTORETOC v0            ")) {
					vs.logError.Printf("bad header: %s\n", name)
					return false
				}
				if binary.BigEndian.Uint32(fromDiskBuf[28:]) != vs.checksumInterval {
					vs.logError.Printf("bad header checksum interval: %s\n", name)
					return false
				}
				j += 32
				first = false
			}
			if n < int(vs.checksumInterval) {
				if binary.BigEndian.Uint32(fromDiskBuf[n-16:]) != 0 {
					vs.logError.Printf("bad terminator size marker: %s\n", name)
					return false
				}
				if !bytes.Equal(fromDiskBuf[n-4:n], []byte("TERM")) {
					vs.logError.Printf("bad terminator: %s\n", name)
					return false
				}
				n -= 16
				terminated = true
			}
			if len(fromDiskOverflow) > 0 {
				j += 32 - len(fromDiskOverflow)
				fromDiskOverflow = append(fromDiskOverflow, fromDiskBuf[j-32+len(fromDiskOverflow):j]...)
				keyB := binary.BigEndian.Uint64(fromDiskOverflow[8:])
				keyA := binary.BigEndian.Uint64(fromDiskOverflow)
				timestampbits := binary.BigEndian.Uint64(fromDiskOverflow[16:])
				fromDiskOverflow = fromDiskOverflow[:0]
				tsm, _, _, _ := vs.lookup(keyA, keyB)
				if int64(tsm>>_TSB_UTIL_BITS) != int64(timestampbits>>_TSB_UTIL_BITS) {
					count++
					stale++
				} else {
					var value []byte
					_, value, err := vs.read(keyA, keyB, value)
					if err != nil {
						vs.logCritical.Println("Error on rewrite read", err.Error())
						return false
					}
					_, err = vs.write(keyA, keyB, timestampbits|_TSB_COMPACTION_REWRITE, value)
					if err != nil {
						vs.logCritical.Println("Error on rewrite", err.Error())
						return false
					}
					count++
					rewrote++
				}
			}
			for ; j+32 <= n; j += 32 {
				keyB := binary.BigEndian.Uint64(fromDiskBuf[j+8:])
				keyA := binary.BigEndian.Uint64(fromDiskBuf[j:])
				timestampbits := binary.BigEndian.Uint64(fromDiskBuf[j+16:])
				tsm, _, _, _ := vs.lookup(keyA, keyB)
				if int64(tsm>>_TSB_UTIL_BITS) != int64(timestampbits>>_TSB_UTIL_BITS) {
					count++
					stale++
				} else {
					var value []byte
					_, value, err := vs.read(keyA, keyB, value)
					if err != nil {
						vs.logCritical.Println("Error on rewrite read", err.Error())
						return false
					}
					_, err = vs.write(keyA, keyB, timestampbits|_TSB_COMPACTION_REWRITE, value)
					if err != nil {
						vs.logCritical.Println("Error on rewrite", err.Error())
						return false
					}
					count++
					rewrote++
				}
			}
			if j != n {
				fromDiskOverflow = fromDiskOverflow[:n-j]
				copy(fromDiskOverflow, fromDiskBuf[j:])
			}
		}
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			vs.logError.Printf("error reading %s: %s\n", name, err)
			return false
		}
	}
	fp.Close()
	if !terminated {
		vs.logError.Printf("early end of file: %s\n", name)
		return false
	}
	if checksumFailures > 0 {
		vs.logWarning.Printf("%d checksum failures for %s\n", checksumFailures, name)
		return false
	}
	vs.logInfo.Println("count:", count, "rewrote:", rewrote, "stale:", stale)
	return true
}

func (vs *DefaultValueStore) doCompaction() {
	fp, err := os.Open(vs.pathtoc)
	if err != nil {
		panic(err)
	}
	names, err := fp.Readdirnames(-1)
	fp.Close()
	if err != nil {
		panic(err)
	}
	sort.Strings(names)

	for i := 0; i < len(names); i++ {
		if !strings.HasSuffix(names[i], ".valuestoc") {
			continue
		}
		namets := int64(0)
		namets, err = strconv.ParseInt(names[i][:len(names[i])-len(".valuestoc")], 10, 64)
		if err != nil {
			vs.logError.Printf("bad timestamp in name: %#v\n", names[i])
			continue
		}
		if namets == 0 {
			vs.logError.Printf("bad timestamp in name: %#v\n", names[i])
			continue
		}
		if namets == int64(atomic.LoadUint64(&vs.activeTOCA)) {
			//vs.logDebug.Printf("skipping active toc: %s\n", names[i])
			continue
		}
		if namets == int64(atomic.LoadUint64(&vs.activeTOCB)) {
			//vs.logDebug.Printf("skipping active toc: %s\n", names[i])
			continue
		}
		count, stale, _ := vs.sampleTOC(names[i])
		if stale > (count / 2) {
			vs.logInfo.Printf("%s count: %d stale %d\n", names[i], count, stale)
			vs.logInfo.Println("Triggering compaction for", names[i])
			compacted := vs.compactFile(names[i])
			if compacted {
				err = os.Remove(names[i])
				if err != nil {
					vs.logCritical.Println("Unable to remove", names[i], "toc because", err.Error())
					continue
				}
				err = os.Remove(names[i][:len(names[i])-len("toc")])
				if err != nil {
					vs.logCritical.Println("Unable to remove", names[i], "values because", err.Error())
					continue
				}
				vs.logInfo.Println("Compacted", names[i])
			}
		}

	}
}
