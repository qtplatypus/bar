//+linux
// Package bar is shared transactional key value store
package bar

import (
	"fmt"
	"bytes"
	"time"
	"os"
	"unsafe"
	"sync/atomic"

	"golang.org/x/sys/unix"

	"github.com/qtplatypus/bar/internal/magic"
)

// Open creates and opens a database at the given path. If the file does not exist then it will be created automatically.
func Open(path string, options *Options) (*DB, error) {
	db, err := createOrLoad(path, options)

	done := make(chan struct{})
	finished := make(chan struct{})

	go db.vacuumeDeamon(done, finished)

	db.done = done
	
	return db, err
}

// createOrLoad
func createOrLoad(path string, options *Options) (*DB, error) {

	db, err := createDB(path, options)
	if err == nil {
		return db, nil
	}

	if !os.IsExist(err) {
		return nil, err
	}

	return loadDB(path, options)
}

// newDB create a file it it doesn't exist otherwise cause an error
func createDB(path string, options *Options) (*DB, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, err
	}
	
	err = unix.Fallocate(int(file.Fd()), 0, 0, page)
	if err != nil {
		return nil, err
	}

	db, err := fileToDB(file)
	if err != nil {
		return nil, err
	}

	db.head.highTide = page
	db.head.lowTide  = page
	db.head.volatile = page
	db.head.durable  = page
	db.head.vacuume  = page
	db.head.vacTime  = time.Now().Unix()

	intialIndex := index{
		size: uint16(unsafe.Sizeof(index{})) | magic.Head,
		count: 1,
		address: 0,
		bitmap: 0,
		checksum: 0,
	}

	intialIndex = addChecksumIndex(intialIndex)

	newheadOffset, err := db.allocate(int64(unsafe.Sizeof(intialIndex)), true)
	if err != nil {
		return nil, err
	}
	
	_, err = db.file.WriteAt(
		(*[indexSize]byte)(unsafe.Pointer(&intialIndex))[:],
		newheadOffset,
	)

	db.head.volatile = newheadOffset

	err = unix.Fdatasync(int(db.file.Fd()))
	if err != nil {
		return nil, err
	}

	db.head.durable = newheadOffset

	// Mark everying as ready by copying the magic string to the start
	copy(db.head.magic[:], magic.FileIdent)
	
	return db, unix.Msync(db.headerBuffer, unix.MS_SYNC)
}

func (db *DB) allocate(size int64, durable bool) (int64, error) {
	topOfAllocation := atomic.AddInt64(&db.head.highTide, size)
	botOfAllocation := topOfAllocation - size
	
	err := unix.Fallocate(int(db.file.Fd()), 0, botOfAllocation, size)

	if err != nil {
		err = fmt.Errorf("unable to allocate size %d at %d due to %s",
			size,
			botOfAllocation,
			err,
		)
	}

	return botOfAllocation, err
}

// loadDB load an existing db
func loadDB(path string, options *Options) (*DB, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	db, err := fileToDB(file)
	if err != nil {
		return nil, err
	}

	for !bytes.Equal([]byte(magic.FileIdent), db.head.magic[:]) {
		time.Sleep(time.Second)
	}

	err = db.makeConsistent()

	return db, err
}

func fileToDB(file *os.File) (*DB, error) {
	headerBuffer, err := unix.Mmap(
		int(file.Fd()),
		0,
		int(unsafe.Sizeof(head{})),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)
	if err != nil {
		return nil, err
	}

	db := &DB{
		file: file,
		head: (*head)(unsafe.Pointer(&headerBuffer[0])),
		headerBuffer: headerBuffer,
	}

	return db, nil
}

func (db *DB) readNode(offset int64) ([]byte, uint16, error) {
	buffer := make([]byte, 2)

	_, err := db.file.ReadAt(buffer, offset)
	if err != nil {
		return []byte{}, 0, err
	}

	length := *((*uint16)(unsafe.Pointer(&buffer[0])))

	nodeType := length | magic.TypeMask

	node := make([]byte, length | ^magic.TypeMask )
	_, err = db.file.ReadAt(node, offset)
	
	return node, nodeType, nil
}

func (db *DB) writeNode(data []byte, durable bool) (int64, error) {
	fmt.Printf("%#v\n%v", db, db.head)
	
	offset, err := db.allocate(int64(len(data)), durable)
	if err != nil {
		return 0, err
	}
	fmt.Printf("offset:%d", offset)

	_, err = db.file.WriteAt(data, offset)
	if err != nil {
		err = fmt.Errorf("write error at %s", err)
	}
	
	return offset, err
}
