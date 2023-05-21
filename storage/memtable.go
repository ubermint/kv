package storage

import (
	"fmt"
	"github.com/ubermint/kv/format"
	"hash/fnv"
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
	db.MemTable[string(hashKey)] = stat
}

func (db *Storage) MemTableSeek(key []byte) (ValStat, error) {
	hash := fnv.New128a()
	hash.Write(key)
	hashKey := hash.Sum(nil)

	stat, exist := db.MemTable[string(hashKey)]

	if exist {
		return stat, nil
	}

	return ValStat{}, fmt.Errorf("Error: No such key in MemTable.")
}

func (db *Storage) MemTableDelete(key []byte) error {
	hash := fnv.New128a()
	hash.Write(key)
	hashKey := hash.Sum(nil)

	_, exist := db.MemTable[string(hashKey)]

	if exist {
		delete(db.MemTable, string(hashKey))
		return nil
	}

	return fmt.Errorf("Error: No such key in MemTable.")
}
