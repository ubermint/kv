package format

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"
)

func TestKVRecordSize(t *testing.T) {
	key := []byte("test")
	value := []byte("value")

	var kv_rec KVRecord
	err := kv_rec.New(key, value)
	if err != nil {
		t.Errorf("Failed.")
	}

	myBuffer, err := kv_rec.Encode()
	if err != nil {
		t.Errorf("Failed.")
	}

	sizer := binary.Size(kv_rec.Timestamp) + binary.Size(kv_rec.KeySize) +
		binary.Size(kv_rec.ValueSize) + len(key) + len(value)

	fmt.Println(myBuffer.Len(), sizer)

	if myBuffer.Len()-CRC_SIZE != sizer {
		t.Errorf("Failed. Encoding is not equal in size.")
	}
}

func TestKVZeroRecord(t *testing.T) {
	key := []byte("test")
	value := []byte{}

	var kv_rec KVRecord
	err := kv_rec.New(key, value)
	if err != nil {
		t.Errorf("Failed.")
	}

	myBuffer, err := kv_rec.Encode()
	if err != nil {
		t.Errorf("Failed.")
	}

	sizer := binary.Size(kv_rec.Timestamp) + binary.Size(kv_rec.KeySize) +
		binary.Size(kv_rec.ValueSize) + len(key) + len(value)

	fmt.Println(myBuffer.Len(), sizer)

	if myBuffer.Len()-CRC_SIZE != sizer {
		t.Errorf("Failed. Encoding is not equal in size.")
	}
}

func TestKVRecordEncoding(t *testing.T) {
	key := []byte("key1")
	value := []byte("value1")

	var kv_rec KVRecord
	err := kv_rec.New(key, value)
	if err != nil {
		t.Errorf("Failed.")
	}

	myBuffer, err := kv_rec.Encode()
	if err != nil {
		t.Errorf("Failed.")
	}

	var kv_rec2 KVRecord
	kv_rec2.Decode(myBuffer)

	if !reflect.DeepEqual(kv_rec, kv_rec2) {
		t.Errorf("Failed. Encoding is not equal.")
	}
}
