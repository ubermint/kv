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
	defer db.Close()

	db.Set([]byte("idiot"), []byte("dostoevsky"))

	val, err := db.Get([]byte("idiot"))
	if err != nil {
		fmt.Println(err)
	}
	log.Println("Length of the map: ", len(db.MemTable), string(val))

	db.Update([]byte("idiot"), []byte("dostoevsky2"))

	val, err = db.Get([]byte("idiot"))
	if err != nil {
		fmt.Println(err)
	}

	log.Println("Length of the map: ", len(db.MemTable), string(val))

	db.Delete([]byte("idiot"))
	val, err = db.Get([]byte("idiot"))
	if err != nil {
		fmt.Println(err)
	}
	log.Println("Length of the map: ", len(db.MemTable), string(val))
}
