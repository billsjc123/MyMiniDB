package myminidb

import (
	"os"
)

const (
	FileName       = "myminidb.data"
	MergedFileName = "myminidb.data.merged"
)

type DBFile struct {
	File   *os.File
	Offset int64
}

func newInterval(fileName string) (*DBFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	return &DBFile{File: file, Offset: stat.Size()}, nil
}

func NewDBFile(path string) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + FileName
	return newInterval(fileName)
}

func NewMergedDBFile(path string) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + MergedFileName
	return newInterval(fileName)
}

func (df *DBFile) Read(offset int64) (*Entry, error) {
	header := make([]byte, entryHeaderLen)
	_, err := df.File.ReadAt(header, offset)
	if err != nil {
		return nil, err
	}
	entry := DecodeHeader(header)

	buf := make([]byte, entryHeaderLen+entry.KeySize+entry.ValueSize)
	_, err = df.File.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}

	if entry.KeySize > 0 {
		entry.Key = make([]byte, entry.KeySize)
		copy(entry.Key, buf[entryHeaderLen:entryHeaderLen+entry.KeySize])
	}
	if entry.ValueSize > 0 {
		entry.Value = make([]byte, entry.ValueSize)
		copy(entry.Value, buf[entryHeaderLen+entry.KeySize:])
	}

	return entry, nil
}

func (df *DBFile) Write(e *Entry) error {
	buf, err := e.Encode()
	if err != nil {
		return err
	}
	_, err = df.File.WriteAt(buf, df.Offset)
	if err != nil {
		return err
	}
	df.Offset += e.GetSize()
	return nil
}
