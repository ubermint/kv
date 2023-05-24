package storage

import (
	"fmt"
	"github.com/ubermint/kv/format"
	"hash/fnv"
	_ "log"
)

type ValStat struct {
	FileID    uint32
	Timestamp uint32
	ValSize   uint32
	ValPos    int64
}

func (db *Storage) MemTableUpdate(kv_rec format.KVRecord, fileID uint32, valPos int64) {
	hash := fnv.New128a()
	hash.Write(kv_rec.Key)
	hashKey := hash.Sum(nil)

	stat := ValStat{fileID, kv_rec.Timestamp, kv_rec.ValueSize, valPos}
	db.MemLock.Lock()
	db.MemTable[string(hashKey)] = stat
	db.MemLock.Unlock()
}

func (db *Storage) MemTableSeek(key []byte) (ValStat, error) {
	hash := fnv.New128a()
	hash.Write(key)
	hashKey := hash.Sum(nil)

	db.MemLock.RLock()
	stat, exist := db.MemTable[string(hashKey)]
	db.MemLock.RUnlock()

	if exist {
		return stat, nil
	}

	return ValStat{}, fmt.Errorf("Error: No such key in MemTable.")
}

func (db *Storage) MemTableDelete(key []byte) error {
	hash := fnv.New128a()
	hash.Write(key)
	hashKey := hash.Sum(nil)

	db.MemLock.RLock()
	_, exist := db.MemTable[string(hashKey)]
	db.MemLock.RUnlock()

	if exist {
		db.MemLock.Lock()
		delete(db.MemTable, string(hashKey))
		db.MemLock.Unlock()
		return nil
	}

	return fmt.Errorf("Error: No such key in MemTable.")
}
