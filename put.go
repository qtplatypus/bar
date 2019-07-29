//+linux
package bar

import (
	"fmt"
	"errors"
	"math/bits"
	"sync/atomic"
	"unsafe"

	"github.com/qtplatypus/bar/internal/magic"

	"golang.org/x/sys/unix"
)

// Set the value for that key
func (db *DB) Set(key uint32, value []byte, acid PH) (error) {
	oldHighTide := db.head.highTide
	snapshot := db.head.volatile

	dataOffset, err := db.writeData(key, value, acid)
	if err != nil {
		return err
	}

	newHead, err := db.setForSnapshot(snapshot, key, dataOffset)
	if err != nil {
		return err
	}

	newHighTide := db.head.highTide

	if acid.Durable {
		if err = unix.Fdatasync(int(db.file.Fd())); err != nil {
			return err
		}

		return db.markAsDurable(newHead)
	}

	if !acid.Volatile {
		go func() {
			err := unix.SyncFileRange(
				int(db.file.Fd()),
				oldHighTide,
				newHighTide - oldHighTide,
				0x4,
			)

			if err != nil {
				return
			}

			db.markAsDurable(newHead)
		}()
	}

	return nil
}

func (db *DB) writeData(key uint32, value []byte, acid PH) (int64, error) {
	// TODO: if it is too large use bigdata
	size := len(value) + dataSize
	
	data := addChecksumData(data{
		size: (uint16(size) & ^magic.TypeMask)|magic.Data,
		count: 1,
		address: key,
	})

	buffer := make([]byte, size)
	copy(buffer, (*(*[dataSize]byte)(unsafe.Pointer(&data)))[0:dataSize])
	copy(buffer[dataSize:], value)

	return db.writeNode(buffer,	!acid.Volatile || acid.Durable)
}

func (db *DB) setForSnapshot(snapshot int64, key uint32, dataOffset int64) (int64, error) {

	for {
		newHead, err := db.setForNode(snapshot, snapshot, key, dataOffset)
		if err != nil {

			if _, ok := err.(rollback); !ok {
				return 0, err
			}

			continue
		}

		if atomic.CompareAndSwapInt64(&db.head.volatile, snapshot, newHead) {
			return newHead, nil
		}

		snapshot = db.head.volatile
		// TODO: merge code
	}
}

func (db *DB) markAsDurable(head int64) (error) {

	for {
		durableHead := db.head.durable

		if durableHead > head {
			return nil
		}

		if atomic.CompareAndSwapInt64(&db.head.durable, durableHead, head) {
			return unix.Msync(db.headerBuffer, 0)
		}
	}
}

func (db *DB) setForNode(snapshot, currentNode int64, key uint32, dataOffset int64) (int64, error) {

	if snapshot < db.head.vacuume {
		return 0, rollback{error: nil}
	}

	node, nodeType, err := db.readNode(currentNode)

	if err != nil {
		return 0, err
	}

	switch nodeType {
	case magic.Index:
		return db.setForIndex(snapshot, currentNode, node, key, dataOffset, false)
	case magic.Head:
		return db.setForIndex(snapshot, currentNode, node, key, dataOffset, true)
	case magic.Data, magic.Bigdata:
		return db.setForData(snapshot, currentNode, node, key, dataOffset)
	default:
		return 0, fmt.Errorf("internal bug type %d not found", nodeType)
	}
}

func (db *DB) setForIndex(snapshot int64, currentNode int64, node []byte, key uint32, dataOffset int64, ishead bool) (int64, error) {
	index := *((*index)(unsafe.Pointer(&node[0])))
	maskIndex := index.address & ^magic.AddressMask[27]

	// the key and the current index diffrent prefixes split this index
	if 0 != (key ^ index.address) & magic.AddressMask[maskIndex] {
		return db.splitIndex(snapshot, currentNode, index, node, key, dataOffset, ishead)
	}

	tri := (key >> (27 - maskIndex)) & ^magic.AddressMask[27]

	// there is no overlapping child node.  Insert a new one
	if 0 == index.bitmap & magic.PlaceBased[tri] {
		return db.insertIndex(snapshot, currentNode, index, node, key, dataOffset, ishead)
	}

	// there is an overlapping child node.  Replace it.
	return db.replaceIndex(snapshot, currentNode, index, node, key, dataOffset, ishead)
}

func (db *DB) setForData(snapshot int64, currentNode int64, node []byte, key uint32, dataOffset int64) (int64, error) {
	data := *((*data)(unsafe.Pointer(&node[0])))

	if data.address == key {
		return dataOffset, nil
	}

	maskIndex := bits.LeadingZeros32(data.address ^ key)

	buffer := make([]byte, indexSize + 8*2)
	index := *((*index)(unsafe.Pointer(&buffer[0])))

	index.size = indexSize + 8*2
	index.count = data.count + 2 // TODO: support big data
	index.address = (key & magic.AddressMask[maskIndex]) | (uint32(maskIndex) & ^magic.AddressMask[27])

	for _, k := range([2]uint32{data.address, key}) {
		index.bitmap |= (k >> uint(27 - maskIndex)) & ^magic.AddressMask[27]
	}

	type kv struct {
		key uint32
		value int64
	}

	for _, add := range([]kv{
		{
			key: key,
			value: dataOffset,
		},
		{
			key: data.address,
			value: currentNode,
		},
	}) {

		tri := (add.key >> uint(27 - maskIndex)) & ^magic.AddressMask[27]
		indexOffset := 8* (bits.OnesCount32(index.bitmap & ^magic.AddressMask[tri]) -1)
		copy(buffer[indexOffset:indexOffset+8], (*(*[8]byte)(unsafe.Pointer(&add.value)))[:])
	}

	addChecksumIndex(&index)

	newheadOffset, err := db.allocate(int64(len(buffer)), true)
	if err != nil {
		return 0, nil
	}

	_, err = db.file.WriteAt(buffer, newheadOffset)

	return newheadOffset, err
}

func (db *DB) insertIndex(snapshot int64, currentNode int64, i index, node []byte, key uint32, dataOffset int64, ishead bool) (int64, error) {

	newIndexSize := 8+ (i.size & ^magic.TypeMask)
	
	buffer := make([]byte, newIndexSize)
	index := (*index)(unsafe.Pointer(&buffer[0]))

	index.size = newIndexSize

	if ishead {
		index.size |= magic.Head
	} else {
		index.size |= magic.Index
	}
	
	index.address = i.address

	maskIndex := index.address & ^magic.AddressMask[27]
	tri := (key >> (27 - maskIndex)) & ^magic.AddressMask[27]
	index.bitmap = i.bitmap | magic.PlaceBased[tri]
	
	for k, mask := range(magic.PlaceBased) {
		if i.bitmap & mask != 0 {
			iOffset := bits.OnesCount32(i.bitmap & ^magic.AddressMask[k]) -1
			indexOffset := bits.OnesCount32(index.bitmap & ^magic.AddressMask[k]) -1
			
			*((*int64)(unsafe.Pointer(&buffer[indexSize + indexOffset*8]))) = *((*int64)(unsafe.Pointer(&node[indexSize + iOffset*8])))
		}
	}

	indexOffset := bits.OnesCount32(index.bitmap & ^magic.AddressMask[tri]) - 1
	*((*int64)(unsafe.Pointer(&buffer[indexSize + indexOffset*8]))) = dataOffset

	addChecksumIndex(index)

	newIndexOffset, err := db.allocate(int64(newIndexSize), true)
	if err != nil {
		return 0, err
	}

	fmt.Printf("insert index: %v\n", buffer)
	
	_, err = db.file.WriteAt(
		buffer,
		newIndexOffset,
	)

	return newIndexOffset, err
}

func (db *DB) splitIndex(snapshot int64, currentNode int64, index index, node []byte, key uint32, dataOffset int64, ishead bool) (int64, error) {
	return 0, errors.New("internal bug split")
}

func (db *DB) replaceIndex(snapshot int64, currentNode int64, index index, node []byte, key uint32, dataOffset int64, ishead bool) (int64, error) {
	return 0, errors.New("internal bug replace")
}
