package storage

import (
	"fmt"
	"github.com/ubermint/kv/format"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Storage struct {
	BaseDir  string
	ID       uint32
	File     *os.File
	MemTable map[string]ValStat
	MemLock  sync.RWMutex
	FileLock sync.RWMutex
}

func (db *Storage) Build(files []string) error {
	sort.Strings(files)
	for _, datafile := range files {
		file, err := os.Open(datafile)
		if err != nil {
			return err
		}

		fileID, err := strconv.ParseUint(filepath.Base(datafile)[4:], 10, 32)
		if err != nil {
			return err
		}

		var position int64

		for {
			var kv_rec format.KVRecord
			pos, err := kv_rec.Decode(file)
			if err != nil {
				if err == io.EOF {
					break
				} else {
					return err
				}
			}

			if kv_rec.ValueSize == 0 {
				db.MemTableDelete(kv_rec.Key)
			} else {
				db.MemTableUpdate(kv_rec, uint32(fileID), position)
			}

			position += int64(pos)
		}

		file.Close()
	}

	log.Println("Restored: ", len(db.MemTable), " key-value pairs.")

	return nil
}

func (db *Storage) New(dirPath string) error {
	if !filepath.IsAbs(dirPath) {
		wd, err := os.Getwd()
		if err != nil {
			return err
		}

		dirPath = filepath.Join(wd, dirPath) + string(filepath.Separator)
	}

	err := os.MkdirAll(filepath.Dir(dirPath), 0755)
	if err != nil {
		return err
	}

	datafiles, err := filepath.Glob(filepath.Join(dirPath, "Data*"))
	if err != nil {
		return fmt.Errorf("Error: Can't parse datafiles: %s", err)
	}

	fileID := uint32(time.Now().Unix())
	fileName := fmt.Sprintf("Data%d", fileID)
	filePath := filepath.Join(dirPath, fileName)

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	db.BaseDir = dirPath
	db.File = file
	db.ID = fileID
	db.MemTable = make(map[string]ValStat)

	if len(datafiles) > 0 {
		err = db.Build(datafiles)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Storage) Close() error {
	if db.File == nil {
		return fmt.Errorf("Error: File is not open.")
	}

	err := db.File.Close()
	db.File = nil
	return err
}

func (db *Storage) Destroy() error {
	if db.File != nil {
		db.Close()
	}

	files, err := filepath.Glob(filepath.Join(db.BaseDir, "Data*"))
	if err != nil {
		fmt.Println("Error:", err)
	}

	for _, datafile := range files {

		err := os.Remove(datafile)
		if err != nil {
			return err
		}
	}

	return err
}

func (db *Storage) Set(key []byte, value []byte) error {
	var kv_rec format.KVRecord
	err := kv_rec.New(key, value)
	if err != nil {
		return err
	}

	data, err := kv_rec.Encode()
	if err != nil {
		return err
	}

	db.FileLock.Lock()
	position, err := db.File.Seek(0, os.SEEK_END)
	if err != nil {
		log.Println("Failed to get current position:", err)
		return err
	}

	_, err = db.File.Write(data.Bytes())
	if err != nil {
		return err
	}
	db.FileLock.Unlock()

	db.MemTableUpdate(kv_rec, db.ID, position)

	return nil
}

func (db Storage) OpenOnRead(fileID uint32) (*os.File, error) {
	if fileID == db.ID {
		return db.File, nil
	}

	fileName := fmt.Sprintf("Data%d", fileID)
	filePath := filepath.Join(db.BaseDir, fileName)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (db *Storage) Get(key []byte) ([]byte, error) {
	stat, err := db.MemTableSeek(key)
	if err != nil {
		return []byte{}, err
	}

	file, err := db.OpenOnRead(stat.FileID)
	if err != nil {
		log.Fatal(err)
	}

	db.FileLock.Lock()
	_, err = file.Seek(stat.ValPos, 0)
	if err != nil {
		log.Fatal(err)
	}

	var kv_rec format.KVRecord
	_, err = kv_rec.Decode(file)
	if err != nil {
		log.Fatal(err)
		return []byte{}, err
	}

	if file != db.File {
		file.Close()
	}
	db.FileLock.Unlock()

	return kv_rec.Value, nil
}

func (db *Storage) Update(key []byte, value []byte) error {
	_, err := db.MemTableSeek(key)
	if err != nil {
		return err
	}

	db.Set(key, value)
	return nil
}

func (db *Storage) Delete(key []byte) error {
	_, err := db.MemTableSeek(key)
	if err != nil {
		return err
	}

	db.Set(key, []byte{})
	err = db.MemTableDelete(key)
	return nil
}
