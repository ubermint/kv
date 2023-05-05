package main

import (
    "fmt"
    "bytes"
    "encoding/binary"
)

const (
    headerSize = 20
)

type header struct {
    timestamp uint32
    keySize   uint32
    valueSize uint32
}

type kv_entry struct {
    header
    key   []byte
    value []byte
}

func (h header) encodeHeader() []byte {
    headerBuffer := make([]byte, 12)
    
    binary.LittleEndian.PutUint32(headerBuffer[:4], h.timestamp)
    binary.LittleEndian.PutUint32(headerBuffer[4:8], h.keySize)
    binary.LittleEndian.PutUint32(headerBuffer[8:], h.valueSize)
    
    return headerBuffer
}

func (h *header) decodeHeader(headerBytes []byte) error {
    if len(headerBytes) != 12 {
        return fmt.Errorf("invalid header length: %d", len(headerBytes))
    }
    
    h.timestamp = binary.LittleEndian.Uint32(headerBytes[:4])
    h.keySize = binary.LittleEndian.Uint32(headerBytes[4:8])
    h.valueSize = binary.LittleEndian.Uint32(headerBytes[8:])
    
    return nil
}


func (kv kv_entry) encode_kv(timestamp uint32, []byte key, []byte value) int, []byte {
	buf := new(bytes.Buffer)
	h := header{timestamp: timestamp, keySize: len(key), valueSize: len(value)}
	headerBytes := h.encodeHeader()

	buf.Write(headerBytes)


	return 0, 0
}

func main() {
    fmt.Println("Hello, world!")

	h := header{timestamp: 12345, keySize: 10, valueSize: 20}
	headerBytes := h.encodeHeader()

	var h2 header
	err := h2.decodeHeader(headerBytes)
	if err != nil {
	    // handle error
	}
	fmt.Println(h2.timestamp, h2.keySize, h2.valueSize)
}
