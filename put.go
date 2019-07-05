//+linux
package bar

import (
	"unsafe"

	"github.com/qtplatypus/bar/internal/magic"
)

// Set the value for that key
func (db *DB) Set(key uint32, value []byte, acid PH) (error) {
	oldHighTide := db.head.highTide
	snapshot := db.head.volatile

	_ = oldHighTide
	_ = snapshot

	dataOffset, err := db.writeData(key, value, acid)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) writeData(key uint32, value []byte, acid PH) (int64, error) {
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
