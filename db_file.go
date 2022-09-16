package myminidb

import "os"

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
