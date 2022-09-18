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

func Open(dirpath string) (*MiniDB, error) {
	// 如果目录不存在则创建目录
	if _, err := os.Stat(dirpath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirpath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	dbFile, err := NewDBFile(dirpath)
	if err != nil {
		return nil, err
	}

	db := &MiniDB{
		file:    dbFile,
		dirpath: dirpath,
		index:   make(map[string]int64),
	}

	err = db.loadIndexFromDBFile()
	if err != nil {
		return nil, err
	}
	return db, nil
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
			err = mergedFile.Write(e)
			if err != nil {
				fmt.Printf("write entry to merged file error. entry: %v, err: %v", e, err)
				return err
			}
			db.index[string(e.Key)] = int64(writeOffset)
		}

		// 删除原文件
		dbFileName := db.file.File.Name()
		db.file.File.Close()
		os.Remove(dbFileName)

		mergeDBFileName := mergedFile.File.Name()
		mergedFile.File.Close()
		os.Rename(mergeDBFileName, db.dirpath+string(os.PathSeparator)+FileName)
		db.file = mergedFile
	}
	return nil
}

func (db *MiniDB) loadIndexFromDBFile() error {
	var offset int64 = 0
	for offset < db.file.Offset {
		entry, err := db.file.Read(offset)
		if err != nil {
			return err
		}
		db.index[string(entry.Key)] = offset
		offset += entry.GetSize()
	}
	return nil
}
