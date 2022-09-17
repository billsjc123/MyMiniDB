package myminidb

import (
	"fmt"
	"os"
	"sync"
)

type MiniDB struct {
	index   map[string]int64
	file    *DBFile
	dirpath string
	mu      *sync.RWMutex
}

func (db *MiniDB) Open(dirpath string) {

}

func (db *MiniDB) Put(key, value []byte) error {
	if len(key) == 0 || len(value) == 0 {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	entry := NewEntry(key, value, Mark_Put)

	offset := db.file.Offset
	err := db.file.Write(entry)
	if err != nil {
		fmt.Printf("write db file error, dirpath: %v, err: %v", db.dirpath, err)
		return err
	}

	db.index[string(key)] = offset
	return nil
}

func (db *MiniDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return []byte(""), nil
	}

	db.mu.RLocker().Lock()
	defer db.mu.RLocker().Unlock()

	offset, ok := db.index[string(key)]
	if !ok {
		return []byte(""), nil
	}

	entry, err := db.file.Read(offset)
	if err != nil {
		fmt.Printf("Read DBFile at offset:%d error: %v", offset, err)
		return nil, err
	}

	return entry.Value, nil
}

func (db *MiniDB) Delete(key []byte) error {
	if len(key) == 0 {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	entry := NewEntry(key, nil, Mark_Del)
	if err := db.file.Write(entry); err != nil {
		fmt.Printf("delete key: %v error: %v", key, err)
		return err
	}

	delete(db.index, string(key))
	return nil

}

func (db *MiniDB) Merge() error {
	if db.file.Offset == 0 {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	var offset int64 = 0
	validEntry := make([]*Entry, 0)

	for offset < db.file.Offset {
		entry, err := db.file.Read(offset)
		if err != nil {
			return err
		}

		// 通过offset判断最新的entry
		if off, ok := db.index[string(entry.Key)]; ok && off == offset && entry.Mark == Mark_Put {
			validEntry = append(validEntry, entry)
		}
		offset += entry.GetSize()
	}

	if len(validEntry) > 0 {
		mergedFile, err := NewMergedDBFile(db.dirpath)
		if err != nil {
			return err
		}
		defer os.Remove(mergedFile.File.Name())
		// 重写有效Entry
		for _, e := range validEntry {
			writeOffset := mergedFile.Offset
			err = db.file.Write(e)
			if err != nil {
				fmt.Printf("write entry to merged file error. entry: %v, err: %v", e, err)
				return err
			}
			db.index[string(e.Key)] = int64(writeOffset)
		}
	}

	return nil
}

func (db *MiniDB) loadIndexFromDBFile() {

}
