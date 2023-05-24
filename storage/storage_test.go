package storage

import (
	"log"
	"strconv"
	"testing"
	"time"
)

func TestKVMethods(t *testing.T) {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.Println("Test 1: TestKVMethods")
	var db Storage
	err := db.New("test")
	if err != nil {
		// handle error
	}
	defer db.Destroy()

	db.Set([]byte("test-key"), []byte("test-value"))

	val, err := db.Get([]byte("test-key"))
	if err != nil {
		t.Errorf("Failed: %s.", err)
	}

	if string(val) != string([]byte("test-value")) {
		t.Errorf("Failed: %s.", err)
	}

	err = db.Update([]byte("test-key"), []byte("new-value"))
	if err != nil {
		t.Errorf("Failed: %s.", err)
	}

	val, err = db.Get([]byte("test-key"))
	if err != nil {
		t.Errorf("Failed: %s.", err)
	}

	if string(val) != string([]byte("new-value")) {
		t.Errorf("Failed: %s.", err)
	}

	db.Delete([]byte("test-key"))
	if err != nil {
		t.Errorf("Failed: %s.", err)
	}

	val, err = db.Get([]byte("test-key"))
	if err == nil {
		t.Errorf("Failed: %s.", err)
	}
}

func TestKVLoad(t *testing.T) {
	log.Println("Test 2: TestKVLoad")
	hashTable := make(map[string]string)

	var storage Storage
	err := storage.New("test")
	defer storage.Destroy()

	for i := 1; i <= 1000000; i++ {
		key := "Key" + strconv.Itoa(i)
		value := "Value" + strconv.Itoa(i)
		hashTable[key] = value
		err = storage.Set([]byte(key), []byte(value))
	}

	log.Println("Test 2: Length of the map: ", len(storage.MemTable))

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

	log.Println("Test 2: Length of the map after delete: ", len(storage.MemTable))

	if x != 1000000 {
		t.Errorf("Failed.")
	}

	storage.Destroy()
}

func TestKVSetup(t *testing.T) {
	log.Println("Test 3.1: TestKVSetup")
	hashTable := make(map[string]string)

	var storage Storage
	err := storage.New("test-build")
	if err != nil {
		// handle error
	}

	for i := 1; i <= 10000; i++ {
		key := "Key" + strconv.Itoa(i)
		value := "firstValue" + strconv.Itoa(i)
		hashTable[key] = value
		err = storage.Set([]byte(key), []byte(value))
	}

	log.Println("Test 3.1: Length of the map: ", len(storage.MemTable))

	for i := 1; i <= 5000; i++ {
		key := "Key" + strconv.Itoa(i)
		value := "secondValue" + strconv.Itoa(i) + strconv.Itoa(i)
		hashTable[key] = value
		err = storage.Update([]byte(key), []byte(value))
	}

	log.Println("Test 3.1: Length of the map after update: ", len(storage.MemTable))

	storage.Close()
}

func TestKVBuild(t *testing.T) {
	log.Println("Test 3.2: TestKVBuild")
	hashTable := make(map[string]string)
	for i := 1; i <= 5000; i++ {
		key := "Key" + strconv.Itoa(i)
		value := "secondValue" + strconv.Itoa(i) + strconv.Itoa(i)
		hashTable[key] = value

	}

	for i := 5001; i <= 10000; i++ {
		key := "Key" + strconv.Itoa(i)
		value := "firstValue" + strconv.Itoa(i)
		hashTable[key] = value

	}

	time.Sleep(2 * time.Second)
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
		} else {
			log.Println(key, value)
		}
	}

	log.Println("Test 3.2: Length of the map after rebuild: ", len(storage2.MemTable))

	if x != 10000 {
		t.Errorf("Failed. %d", x)
	}

	storage2.Destroy()
}
