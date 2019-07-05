//+linux
package bar

import (
	"os"
)

const page = 4096

// DB 
type DB struct {
	file *os.File
	head *head
	headerBuffer []byte
	done chan struct{}
	vaccumeScale uint8
}

// Options
type Options struct {
	// VacuumFrequency
	// 0 = disabled
	// otherwise at most there will be 2^VacuumFrequency seconds
	// between vacuums up to a max of 32
	VacuumFrequency uint8
}

// PH
type PH struct {
	Volatile bool
	Durable bool
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
	vacuume int64
	// vacTime the unix epoch time of the last vacuum
	vacTime int64
}

// index node
type index struct {
	// size and type
	size uint16
	// count of the number of nodes in this tree
	count uint16
	// address and mask of this node
	address uint32
	// bitmap map to child node offsets
	bitmap uint32
	// checksum crc32 of the index
	checksum uint32
}

// indexChecksumSize the size of the index that is checksumed
const (
	indexChecksumSize = 2 + 2 + 4 + 4
	indexSize = indexChecksumSize + 4
)

// bucket node
type bucket struct {
	// size and type
	size uint16
	// count of the number of nodes in this tree
	count uint16
	// address and mask of this node
	address uint32
	// checksum crc32 of the bucket
	checksum uint32
	// head of this buckets index
	head index
}

const (
	bucketChecksumSize = 2 + 2 + 4
	bucketSize = bucketChecksumSize + 4 + indexSize
)

// data node
type data struct {
	// size and type
	size uint16
	// count of the number of nodes in this tree
	count uint16
	// address of this node
	address uint32
	// checksum crc32 of the data header
	checksum uint32
}

const (
	dataChecksumSize = 2 + 2 + 4
	dataSize = dataChecksumSize + 4
)

type rollback struct {
	error
}
