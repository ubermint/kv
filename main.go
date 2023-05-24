package main

import (
	"fmt"
	"github.com/ubermint/kv/storage"
	"log"
)

func main() {

	var db storage.Storage

	err := db.New("test")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Destroy()

	db.Set([]byte("test"), []byte("value"))

	val, err := db.Get([]byte("test"))
	if err != nil {
		fmt.Println(err)
	}
	log.Println("Length of the map: ", len(db.MemTable), string(val))

	db.Update([]byte("test"), []byte("value2"))

	val, err = db.Get([]byte("test"))
	if err != nil {
		fmt.Println(err)
	}

	log.Println("Length of the map: ", len(db.MemTable), string(val))

	db.Delete([]byte("test"))
	val, err = db.Get([]byte("test"))
	if err == nil {
		fmt.Println(err)
	}
	log.Println("Length of the map: ", len(db.MemTable), string(val))
}
