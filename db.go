package myminidb

import "sync"

type MiniDB struct {
	index   map[string]int64
	file    *DBFile
	dirpath string
	mu      *sync.RWMutex
}

func (db *MiniDB) Open(dirpath string) {

}

func (db *MiniDB) loadIndexFromDBFile() {

}

func (db *MiniDB) Put() error {
	return nil
}

func (db *MiniDB) Get() (string, error) {
	return "", nil
}

func (db *MiniDB) Delete() error {
	return nil
}

func (db *MiniDB) Merge() error {
	return nil
}
