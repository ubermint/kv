package storage

import (
    "fmt"
    "hash/fnv"
    "github.com/ubermint/kv/format"
)


type ValStat struct {
	FileID uint32
	Timestamp uint32
	ValSize uint32
    ValPos  int64
}

func (disk *Storage) MemTableUpdate(kv_rec format.KVRecord, fileID uint32, valPos int64) {
	hash := fnv.New128a()
	hash.Write(kv_rec.Key)
	hashKey := hash.Sum(nil)

	stat := ValStat{fileID, kv_rec.Timestamp, kv_rec.ValueSize, valPos}
	disk.MemTable[string(hashKey)] = stat
}

func (disk *Storage) MemTableSeek(key []byte) (ValStat, error) {
	hash := fnv.New128a()
	hash.Write(key)
	hashKey := hash.Sum(nil)

	stat, exist := disk.MemTable[string(hashKey)]

	if exist {
		return stat, nil
	}

	return ValStat{}, fmt.Errorf("Error: No such key in MemTable.")
}

func (disk *Storage) MemTableDelete(key []byte) error {
	hash := fnv.New128a()
	hash.Write(key)
	hashKey := hash.Sum(nil)

	_, exist := disk.MemTable[string(hashKey)]

	if exist {
		delete(disk.MemTable, string(hashKey))
		return nil
	}

	return fmt.Errorf("Error: no key in map.")
}