//+linux
package bar

import (
	"os"
	"hash/crc32"
	"time"
	"unsafe"
	"sync/atomic"
)

var pid = os.Getpid()

func (db *DB) vacuumeDeamon (done <-chan struct{}, finished chan <- struct{}) {
	defer close(finished)

	for {
		vacTime := db.head.vacTime
		after := db.randomVaccumeTime(vacTime)
		
		select {
		case <-done:
			return
		case <-after.C:
			db.vacuume(vacTime)
			after.Stop()
		}
	}
}

func (db *DB) randomVaccumeTime(vacTime int64) (*time.Timer) {
	// use the current vacuum time and the pid to randomly
	// create a delay
	buffer := make([]byte, 8 + 4)

	copy(buffer, (*[8]byte)(unsafe.Pointer(&vacTime))[:])
	copy(buffer[8:], (*[4]byte)(unsafe.Pointer(&pid))[:])

	// to be honest this isn't anywhere near strong randomness
	// but we are just trying to get an approximation here
	dwell := crc32.ChecksumIEEE(buffer) >> (32 - db.vaccumeScale)

	timeSinceLastVaccume := time.Now().Unix() - vacTime
	timeUntilNextVaccume := int64(dwell) - timeSinceLastVaccume

	waitTime := int64(1)
	if timeUntilNextVaccume > 1 {
		waitTime = timeUntilNextVaccume
	}
	
	return time.NewTimer(time.Duration(waitTime) * time.Second)
}

// vacuume do regular maintenance operations 
func (db *DB) vacuume(vacTime int64) {
	if !atomic.CompareAndSwapInt64(&db.head.vacTime, vacTime, time.Now().Unix()) {
		// vacuume has run since we went to sleep. Do nothing
		return
	}

	if db.head.vacuume == db.head.volatile {
		// there has been no changes since the last vacuume
		// don't bother
		return
	}

	// advance the vacuume head to the current volatile head
	vaccumeHead := db.head.vacuume
	snapshot := db.head.volatile
	if !atomic.CompareAndSwapInt64(&db.head.vacuume, vaccumeHead, snapshot) {
		return
	}

	nodeSize, allocatedSize, deepestNode := db.fragmentStats(snapshot)

	// TODO: force the volatile head to be durable
	
	_ = nodeSize
	_ = allocatedSize
	_ = deepestNode

	// TODO: work out how fragmented the tree is and apply defragmentation

	// TODO: work out the deepest node of the vaccumeHead and update lowtide mark

	// TODO: punch hole from page up to the lowtide mark
}

func (db *DB) fragmentStats (offset int64) (int64, int64, int64) {
	return 0, 0, 0
}
