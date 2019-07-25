//+linux
package bar

import (
	"math/bits"
	"unsafe"
	"sync/atomic"

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
		newHead, err := setForNode(snapshot, snapshot, key, dataOffset)
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

func setForNode(snapshot, currentNode int64, key uint32, dataOffset int64) (int64, error) {

	if snapshot < db.head.vacuume {
		return nil, rollback{error: nil}
	}

	node, nodeType, err := db.readNode(currentNode)

	if err != nil {
		return []byte{}, err
	}

	switch nodeType {
	case magic.Index:
		return db.setForIndex(snapshot, currentNode, node, key, dataOffset, false)
	case magic.Head:
		return db.setForIndex(snapshot, currentNode, node, key, dataOffset, true)
	case magic.Data, magic.Bigdata:
		return db.setForData(snapshot, currentNode, node, key, dataOffset)
	default:
		return 0, errors.New("internal bug")
	}
}

func setForIndex(snapshot int64, currentNode int64, node []byte, key uint32, dataOffset int64, ishead bool) (int64, error) {
	index := *((*index)(unsafe.Pointer(&node[0])))
	maskIndex := index.address & ^magic.AddressMask[27]

	// the key and the current index diffrent prefixes split this index
	if 0 != (key ^ index.address) & magic.AddressMask[maskIndex] {
		return splitIndex(snapshot, currentNode, index, key, dataOffset, ishead)
	}

	tri := (key >> (27 - maskIndex)) & ^magic.AddressMask[27]

	// there is no overlapping child node.  Insert a new one
	if 0 == index.bitmap & magic.PlaceBased[tri] {
		return insertIndex(snapshot, index, key, dataOffset, ishead)
	}

	// there is an overlapping child node.  Replace it.
	replace replaceIndex(snapshot, index, key, dataOffset, ishead)
}

func setForData(snapshot int64, currentNode int64, node []byte, key uint32, dataOffset int64) (int64, error) {
	data := *((*data)(unsafe.Pointer(&node[0])))

	if data.address == key {
		return dataOffset, nil
	}

	maskIndex := bit.LeadingZeros32(data.address ^ key)

	buffer := make([]byte, indexSize + 8*2)
	index := *((*index)(unsafe.Pointer(&buffer[0])))

	index.size = indexSize + 8*2
	index.count = data.count + 2 // TODO: support big data
	index.address = (key & magic.AddressMask[maskIndex]) | (maskIndex & ^magic.AddressMask[27])

	for _, k := range(data.address, key) {
		index.bitmap |= (k >> (27 - maskIndex)) & ^magic.AddressMask[27]
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

		tri := (add.key >> (27 - maskIndex)) & ^magic.AddressMask[27]
		indexOffset := 8* bits.OnesCount32(index.bitmap & ^magic.AddressMask[tri])
		copy(buffer[indexOffset, indexOffset+8], (*[8]byte)(unsafe.Pointer(&add.value)))
	}
	
		
}

	
