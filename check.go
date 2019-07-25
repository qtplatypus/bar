//+linux
package bar

import (
	"hash/crc32"
	"sync/atomic"
	"unsafe"

	"github.com/qtplatypus/bar/internal/magic"
)

func (db *DB) makeConsistent () error {
	snapshot := db.head.volatile

	if db.checkHead(snapshot) {
		return nil
	}

	return db.durableRollback(snapshot)
}

// durableRollback to the durable head
func (db *DB) durableRollback(snapshot int64) error {
	if atomic.CompareAndSwapInt64(&db.head.volatile, snapshot, db.head.durable) {
		return nil
	}

	return db.makeConsistent()
}

func (db *DB) checkHead (snapshot int64) bool {
	node, nodeType, err := db.readNode(snapshot)

	if err != nil {
		return false
	}

	if nodeType != magic.Head {
		return false
	}

	index := *((*index)(unsafe.Pointer(&node[0])))

	if !checkChecksumIndex(index) {
		return false
	}

	length := index.size | ^magic.TypeMask
	count := uint16(1)

	for addressLoc := indexSize; addressLoc < int(length); addressLoc += 8 {
		count += db.checkNode(*((*int64)(unsafe.Pointer(&node[addressLoc]))))
	}
	
	return index.count == count
}

func (db *DB) checkNode (snapshot int64) uint16 {
	node, nodeType, err := db.readNode(snapshot)

	if err != nil {
		return 0
	}

	switch nodeType {
	case magic.Index:
		return db.checkIndex(node)
	case magic.Data:
		return db.checkData(node)
	case magic.Bigdata:
		return db.checkBigdata(node)
	case magic.Bucket:
		return db.checkBucket(node)
	case magic.Head:
		return 0
	default:
		return 0
	}
}

func (db *DB) checkIndex(node []byte) uint16 {
	index := *((*index)(unsafe.Pointer(&node[0])))
	
	if !checkChecksumIndex(index) {
		return 0
	}

	length := index.size | ^magic.TypeMask
	count := uint16(1)

	for addressLoc := indexSize; addressLoc < int(length); addressLoc += 8 {
		count += db.checkNode(*((*int64)(unsafe.Pointer(&node[addressLoc]))))
	}

	if index.count == count {
		return count
	}
	
	return 0
}

func (db *DB) checkData(node []byte) uint16 {
	data := *((*data)(unsafe.Pointer(&node[0])))

	if !checkChecksumData(data) {
		return 0
	}
	
	return 1
}

func (db *DB) checkBigdata(node []byte) uint16 {
	data := *((*data)(unsafe.Pointer(&node[0])))

	if !checkChecksumData(data) {
		return 0
	}

	length := data.size | ^magic.TypeMask
	count := uint16(1)

	for addressLoc := dataSize; addressLoc < int(length); addressLoc += 8 {
		count += db.checkNode(*((*int64)(unsafe.Pointer(&node[addressLoc]))))
	}

	if data.count == count {
		return count
	}
	
	return 0
}

func (db *DB) checkBucket(node []byte) uint16 {
	bucket := *((*bucket)(unsafe.Pointer(&node[0])))

	if !checkChecksumBucket(bucket) {
		return 0
	}

	length := bucket.size | ^magic.TypeMask
	count := uint16(1)

	for addressLoc := bucketSize; addressLoc < int(length); addressLoc += 8 {
		count += db.checkNode(*((*int64)(unsafe.Pointer(&node[addressLoc]))))
	}

	if bucket.count == count {
		return count
	}

	return 0
}

func addChecksumIndex(i *index) {
	i.checksum = crc32.ChecksumIEEE(
		(*[indexChecksumSize]byte)(unsafe.Pointer(i))[:],
	)
}

func checkChecksumIndex(i index) bool {
	checksum := crc32.ChecksumIEEE(
		(*[indexChecksumSize]byte)(unsafe.Pointer(&i))[:],
	)

	return checksum == i.checksum
}

func addChecksumData(d data) data {
	d.checksum = crc32.ChecksumIEEE(
		(*[dataChecksumSize]byte)(unsafe.Pointer(&d))[:],
	)

	return d
}

func checkChecksumData(d data) bool {
	checksum := crc32.ChecksumIEEE(
		(*[dataChecksumSize]byte)(unsafe.Pointer(&d))[:],
	)

	return checksum == d.checksum
}

func checkChecksumBucket(b bucket) bool {
	checksum := crc32.ChecksumIEEE(
		(*[bucketChecksumSize]byte)(unsafe.Pointer(&b))[:],
	)

	return checksum == b.checksum
}
	
