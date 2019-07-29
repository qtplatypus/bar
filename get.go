//+linux
package bar

import (
	"errors"
	"math/bits"
	"unsafe"

	"github.com/qtplatypus/bar/internal/magic"
)


// Get retrieve the value for this key.  Returns nil if the infomation isn't
// found
func (db *DB) Get(key uint32) ([]byte, error) {

	for {
		data, err := db.getForSnapshot(db.head.volatile, key)

		if err == nil {
			return data, err
		}

		if _, ok := err.(rollback); !ok {
			return data, err
		}
	}
}

func (db *DB) getForSnapshot(snapshot int64, key uint32) ([]byte, error) {

	currentNode := snapshot
	
	for {
		if currentNode == 0 {
			return nil, nil
		}

		if snapshot < db.head.vacuume {
			return nil, rollback{error: nil}
		}

		node, nodeType, err := db.readNode(currentNode)

		if err != nil {
			return []byte{}, err
		}

		switch nodeType {
		case magic.Index, magic.Head:
			currentNode, err = db.getForIndex(key, node)
		case magic.Data:
			return db.getForData(key, node)
		case magic.Bigdata:
			return db.getForBigdata(key, node)
		case magic.Bucket:
			return nil, nil
		default:
			return []byte{}, errors.New("internal bug")
		}

		if err != nil {
			return []byte{}, err
		}

	}
}

func (db *DB) getForIndex(key uint32, node []byte) (int64, error) {
	index := *((*index)(unsafe.Pointer(&node[0])))

	maskIndex := index.address & ^magic.AddressMask[27]

	if 0 != (key ^ index.address) & magic.AddressMask[maskIndex] {
		return 0, nil
	}

	tri := (key >> (27 - maskIndex)) & ^magic.AddressMask[27]
	
	if 0 == index.bitmap & magic.PlaceBased[tri] {
		return 0, nil
	}

	indexOffset := bits.OnesCount32(index.bitmap & ^magic.AddressMask[tri]) - 1

	return *((*int64)(unsafe.Pointer(&node[indexSize + indexOffset*8]))), nil
}

func (db *DB) getForData(key uint32, node []byte) ([]byte, error) {
	data := *((*data)(unsafe.Pointer(&node[0])))

	if data.address != key {
		return nil, nil
	}
	
	return node[dataSize:], nil
}

func (db *DB) getForBigdata(key uint32, node []byte) ([]byte, error) {
	data := *((*data)(unsafe.Pointer(&node[0])))

	if data.address != key {
		return nil, nil
	}

	buffer := []byte{}

	for offset := dataSize; offset < int(data.size | ^magic.TypeMask); offset += 8 {
		datanode, _, err := db.readNode(*((*int64)(unsafe.Pointer(&node[offset]))))
		if err != nil {
			return nil, err
		}

		temp, err := db.getForData(0, datanode)
		if err != nil {
			return nil, err
		}
		
		buffer = append(buffer, temp...)
	}
	
	return buffer, nil
}
