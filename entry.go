package myminidb

import (
	"encoding/binary"
)

const (
	entryHeaderLen = 10
)

const (
	Mark_Put = iota
	Mark_Del
)

type Entry struct {
	Key       []byte
	Value     []byte
	KeySize   uint32
	ValueSize uint32
	Mark      uint16
}

func NewEntry(key, value []byte, mark uint16) *Entry {
	return &Entry{
		Key:       []byte(key),
		Value:     []byte(value),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		Mark:      mark,
	}
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

func DecodeHeader(buf []byte) *Entry {
	keySize := binary.BigEndian.Uint32(buf[0:4])
	valueSize := binary.BigEndian.Uint32(buf[4:8])
	mark := binary.BigEndian.Uint16(buf[8:10])

	return &Entry{KeySize: keySize, ValueSize: valueSize, Mark: mark}
}
