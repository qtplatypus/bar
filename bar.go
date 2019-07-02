//+linux
// Package bar is shared transactional key value store
package bar

import (
	"time"
	"os"
	"golang.org/x/sys/unix"
	"unsafe"
	"sync/atomic"
)

const page = 4096

// DB 
type DB struct {
	file *os.File
	head *head
}

// file structure
// 0 - 4096 Memory Mapped Area
//   0 - 48 HEAD
// 4096 - highTide Appending area
//   4096 - lowTide : This area will have a hole punched through it
//   vacuum   : oldest snapshot
//   durable  : all children of this head have been saved to disk
//   volatile : most recent head
//   volatile - highTide : new nodes

// Options
type Options struct {}

// head contains the offsets of all of the heads
type head struct {
	// magic number
	magic [8]byte
	// highTide is the end of the file
	highTide int64
	// lowTide is the start of the data
	lowTide int64
	// volatile head is the most recent head.  Might not be on the filesystem
	volatile int64
	// durable head is the head we know has been saved to the filesystem
	durable int64
	// vacuum head any snapshot older then this might be vacuumed
	vacuum int64
	// vacTime the unix epoch time of the last vacuum
	vacTime int64
}

// index node
type index struct {
	// size and type
	size int16
	// count of the number of nodes in this tree
	count int16
	// address and mask of this node
	address int32
	// bitmap map to child node offsets
	bitmap int32
	// checksum crc32 of the index
	checksum int32
}

// data node
type data struct {
	// size and type
	size int16
	// count of the number of nodes in this tree
	count int16
}

// Open creates and opens a database at the given path. If the file does not exist then it will be created automatically.
func Open(path string, options *Options) (*DB, error) {
	db, err := newDB(path, options)
	if err == nil {
		return db, nil
	}
	
	return nil, nil
}

// newDB create a file it it doesn't exist otherwise cause an error
func newDB(path string, options *Options) (*DB, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0755)
	if err != nil {
		return nil, err
	}

	err = unix.Fallocate(int(file.Fd()), 0, 0, page)
	if err != nil {
		return nil, err
	}

	headerBuffer, err := unix.Mmap(int(file.Fd()), 0, int(unsafe.Sizeof(head{})), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)

	db := &DB{
		file: file,
		head: (*head)(unsafe.Pointer(&headerBuffer[0])),
	}

	db.head.highTide = page
	db.head.lowTide  = page
	db.head.volatile = page
	db.head.durable  = page
	db.head.vacuum   = page
	db.head.vacTime  = time.Now().Unix()
	
	
	return db, nil
}

func (db *DB) allocate(size int64) (int64) {
	topOfAllocation := atomic.AddInt64(&db.head.highTide, size)

	return topOfAllocation - size
}
