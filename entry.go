package myminidb

import (
	"encoding/binary"
)

const (
	entryHeaderLen = 10
)

type Entry struct {
	Key       []byte
	Value     []byte
	KeySize   uint32
	ValueSize uint32
	Mark      uint16
}

func (e *Entry) GetSize() int64 {
	return int64(entryHeaderLen + e.KeySize + e.ValueSize)
}

func (e *Entry) Encode() ([]byte, error) {
	buf := make([]byte, e.GetSize())

	// encode header
	binary.BigEndian.PutUint32(buf[0:4], e.KeySize)
	binary.BigEndian.PutUint32(buf[4:8], e.ValueSize)
	binary.BigEndian.PutUint16(buf[8:10], e.Mark)

	// encode key & value
	copy(buf[entryHeaderLen:entryHeaderLen+e.KeySize], e.Key)
	copy(buf[entryHeaderLen+e.KeySize:], e.Value)
	return buf, nil
}

func Decode(buf []byte) *Entry {
	keySize := binary.BigEndian.Uint32(buf[0:4])
	valueSize := binary.BigEndian.Uint32(buf[4:8])
	mark := binary.BigEndian.Uint16(buf[8:10])

	return &Entry{KeySize: keySize, ValueSize: valueSize, Mark: mark}
}
