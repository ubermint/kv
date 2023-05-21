package storage

import (
	"fmt"
	"log"
	"strconv"
	"testing"
)

func TestSingleKV(t *testing.T) {
	var db Storage
	err := db.New("test")
	if err != nil {
		// handle error
	}
	defer db.Destroy()

	db.Set([]byte("idiot"), []byte("dostoevsky"))

	val, err := db.Get([]byte("idiot"))
	if err != nil {
		fmt.Println(err)
	}

	log.Println(string(val))

	if string(val) != string([]byte("dostoevsky")) {
		t.Errorf("Failed.")
	}
}

func TestKVLoad(t *testing.T) {
	hashTable := make(map[string]string)

	var storage Storage
	err := storage.New("test")
	if err != nil {
		// handle error
	}
	defer storage.Destroy()

	for i := 1; i <= 10000000; i++ {
		key := "Key" + strconv.Itoa(i)
		value := "Value" + strconv.Itoa(i)
		hashTable[key] = value
		err = storage.Set([]byte(key), []byte(value))
	}

	log.Println("Length of the map: ", len(storage.MemTable))

	x := 0

	for key, value := range hashTable {
		diskval, err := storage.Get([]byte(key))
		if err != nil {
			// handle error
		}

		if string(diskval) == value {
			x++
		}
	}

	for key, _ := range hashTable {
		err = storage.Delete([]byte(key))
		if err != nil {
			// handle error
		}

	}

	log.Println("Length of the map: ", len(storage.MemTable))

	if x != 10000000 {
		t.Errorf("Failed.")
	}
}

func TestKVSetup(t *testing.T) {
	hashTable := make(map[string]string)

	var storage Storage
	err := storage.New("test-build")
	if err != nil {
		// handle error
	}

	for i := 1; i <= 10000; i++ {
		key := "Key" + strconv.Itoa(i)
		value := "Value" + strconv.Itoa(i)
		hashTable[key] = value
		err = storage.Set([]byte(key), []byte(value))
	}

	log.Println("Test: Length of the map: ", len(storage.MemTable))

	for i := 1; i <= 5000; i++ {
		key := "Key" + strconv.Itoa(i)
		value := "Pepe" + strconv.Itoa(i) + strconv.Itoa(i)
		hashTable[key] = value
		err = storage.Update([]byte(key), []byte(value))
	}

	log.Println("Test: Length of the map: ", len(storage.MemTable))

	storage.Close()
}

func TestKVBuild(t *testing.T) {
	hashTable := make(map[string]string)
	for i := 1; i <= 5000; i++ {
		key := "Key" + strconv.Itoa(i)
		value := "Pepe" + strconv.Itoa(i) + strconv.Itoa(i)
		hashTable[key] = value

	}

	for i := 5001; i <= 10000; i++ {
		key := "Key" + strconv.Itoa(i)
		value := "Value" + strconv.Itoa(i)
		hashTable[key] = value

	}

	var storage2 Storage
	err := storage2.New("test-build")
	if err != nil {
		// handle error
	}

	x := 0
	for key, value := range hashTable {
		diskval, err := storage2.Get([]byte(key))
		if err != nil {
			// handle error
		}

		if string(diskval) == value {
			x++
		}
	}

	log.Println("Test: Length of the map: ", len(storage2.MemTable))

	if x != 10000 {
		t.Errorf("Failed.")
	}
	storage2.Close()

}
