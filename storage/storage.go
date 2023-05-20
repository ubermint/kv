package storage

import (
    "fmt"
    "log"
    "bytes"
    "os"
    "io"
    "time"
    "path/filepath"
    "strconv"
    _ "encoding/binary"
    "github.com/ubermint/kv/format"
)

type Storage struct {
	BaseDir string
	ID uint32
    File *os.File
    MemTable map[string]ValStat
}

func (disk *Storage) Build(files []string) error {
	for _, datafile := range files {
		fmt.Println(datafile)
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
			pos, err := kv_rec.Parse(file)
			if err != nil {
				if err == io.EOF {
	            	break
		        } else {
		        	return err
		        }
			}

			if kv_rec.ValueSize == 0 {
				disk.MemTableDelete(kv_rec.Key)
			} else {
				disk.MemTableUpdate(kv_rec, uint32(fileID), position)
			}

			position += int64(pos)
		}

		file.Close()
	}

	log.Println("Length of the map: ",  len(disk.MemTable))

	return nil
}

func (disk *Storage) New(dirPath string) error {
	if !filepath.IsAbs(dirPath) {
		wd, err := os.Getwd()
		if err != nil {
			fmt.Println("Error getting current working directory:", err)
			return err
		}

		dirPath = filepath.Join(wd, dirPath) + string(filepath.Separator)
	}


	err := os.MkdirAll(filepath.Dir(dirPath), 0755)
	if err != nil {
		fmt.Println("Error creating directory: ", err)
		return err
	}


	datafiles, err := filepath.Glob(filepath.Join(dirPath, "Data*"))
	if err != nil {
		fmt.Println("Error:", err)
	}

	fileID := uint32(time.Now().Unix())
	fileName := fmt.Sprintf("Data%d", fileID)
	filePath := filepath.Join(dirPath, fileName)
	
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	disk.BaseDir = dirPath
	disk.File = file
	disk.ID = fileID
	disk.MemTable = make(map[string]ValStat)

	fmt.Println(len(datafiles))
	if len(datafiles) > 0 {
		err = disk.Build(datafiles)
		if err != nil {
			return err
		}
	}
	return nil
}

func (disk *Storage) Close() error {
	if disk.File == nil {
		return fmt.Errorf("Error: File is not open.")
	}

	err := disk.File.Close()
	disk.File = nil
	return err
}


func (disk *Storage) Destroy() error {
	if disk.File != nil {
		disk.Close()
	}
	
	files, err := filepath.Glob(filepath.Join(disk.BaseDir, "Data*"))
	if err != nil {
		fmt.Println("Error:", err)
	}

	for _, datafile := range files {
		
		err := os.Remove(datafile)
		if err != nil {
			return err
		}
	}

	log.Println("Datafiles removed")
	return err
}

func (disk *Storage) Set(key []byte, value []byte) error {
	var kv_rec format.KVRecord
    err := kv_rec.New(key, value)
    if err != nil {
        return err
    }

    data, err := kv_rec.Encode()
    if err != nil {
        return err
    }

    position, err := disk.File.Seek(0, os.SEEK_END)
	if err != nil {
		log.Println("Failed to get current position:", err)
		return err
	}

    _, err = disk.File.Write(data.Bytes())
    if err != nil {
		log.Println("Failed to get current position:", err)
		return err
	}
    
    disk.MemTableUpdate(kv_rec, disk.ID, position)

    return nil
}

func (disk Storage) OpenOnRead(fileID uint32) (*os.File, error) {
	if fileID == disk.ID { 
		return disk.File, nil
	}

	fileName := fmt.Sprintf("Data%d", fileID)
	filePath := filepath.Join(disk.BaseDir, fileName)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (disk *Storage) Get(key []byte) ([]byte, error) {
	stat, err := disk.MemTableSeek(key)
	if err != nil {
		return []byte{}, err
	}

	//log.Println("Stat:", stat.FileID, stat.ValPos)

	content := make([]byte, uint32(len(key)) + stat.ValSize +
		format.HEADER_SIZE + format.CRC_SIZE)

	file, err := disk.OpenOnRead(stat.FileID)
	if err != nil {
		log.Fatal(err)
	}
	_, err = file.Seek(stat.ValPos, 0)
	if err != nil {
		log.Fatal(err)
	}

	_, err = file.Read(content)
	if err != nil {
		if err.Error() != "EOF" {
			log.Fatal(err)
		}
	}

	if file != disk.File {
		file.Close()
	}


	buf := new(bytes.Buffer)
    buf.Write(content)

	var kv_rec format.KVRecord
	err = kv_rec.Decode(buf)
    if err != nil {
    	log.Println(string(key))
        log.Fatal(err)
    }

    return kv_rec.Value, nil
}


func (disk *Storage) Update(key []byte, value []byte) (error) {
	_, err := disk.MemTableSeek(key)

	if err != nil {
		log.Println("Error: No such key in MemTable.")
		return err
	}

	disk.Set(key, value)
	return nil
}

func (disk *Storage) Delete(key []byte) (error) {
	_, err := disk.MemTableSeek(key)

	if err != nil {
		log.Println("Error: No such key in MemTable.")
		return err
	}

	disk.Set(key, []byte{})
	err = disk.MemTableDelete(key)
	return nil
}





