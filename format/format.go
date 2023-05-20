package format

import (
    "fmt"
    "bytes"
    "encoding/binary"
    "time"
    "math"
    "hash/crc32"
    "io"
    "log"
)

const (
    HEADER_SIZE = 12
    CRC_SIZE = 4
)

type Header struct {
    Timestamp uint32
    KeySize   uint32
    ValueSize uint32
}

func (h Header) encodeHeader() []byte {
    headerBuffer := make([]byte, HEADER_SIZE)
    
    binary.LittleEndian.PutUint32(headerBuffer[:4], h.Timestamp)
    binary.LittleEndian.PutUint32(headerBuffer[4:8], h.KeySize)
    binary.LittleEndian.PutUint32(headerBuffer[8:12], h.ValueSize)
    
    return headerBuffer
}

func (h *Header) decodeHeader(headerBytes []byte) error {
    if len(headerBytes) != HEADER_SIZE {
        return fmt.Errorf("Error: invalid header length: %d.", len(headerBytes))
    }
    
    h.Timestamp = binary.LittleEndian.Uint32(headerBytes[:4])
    h.KeySize = binary.LittleEndian.Uint32(headerBytes[4:8])
    h.ValueSize = binary.LittleEndian.Uint32(headerBytes[8:12])

    return nil
}

type KVRecord struct {
    Header
    Key   []byte
    Value []byte
}

func (rec *KVRecord) New(key []byte, value []byte) error {
    timestamp := uint32(time.Now().Unix())

    k_size := len(key)
    v_size := len(value)
    if v_size > math.MaxUint32 || k_size > math.MaxUint32  {
        return fmt.Errorf("Error: Byte array is too large to save as uint32.")
    }

    header := Header{timestamp, uint32(k_size), uint32(v_size)}

    rec.Header = header
    rec.Key = key
    rec.Value = value

    return nil
}


func (rec *KVRecord) Parse(reader io.Reader) (int, error) {
    var CRC uint32
    err := binary.Read(reader, binary.LittleEndian, &CRC)
    if err != nil {
        return 0, err
    }

    var header Header
    err = binary.Read(reader, binary.LittleEndian, &header)
    if err != nil {
        return 0, err
    }

    key := make([]byte, header.KeySize)
    err = binary.Read(reader, binary.LittleEndian, key)
    if err != nil {
        return 0, err
    }
    
    value := make([]byte, header.ValueSize)
    err = binary.Read(reader, binary.LittleEndian, value)
    if err != nil {
        return 0, err
    }
    
    rec.Header = header
    rec.Key = key
    rec.Value = value

    myBuffer, err := rec.Encode()
    if err != nil {
        //handle error
    }

    return myBuffer.Len(), nil
}


func (rec *KVRecord) Decode(buf *bytes.Buffer) (error) {
    // binary to format
    
    var rec_checksum uint32
    err := binary.Read(buf, binary.LittleEndian, &rec_checksum)
    if err != nil {
        return err
    }

    crc := crc32.NewIEEE()
    crc.Write(buf.Bytes())
    var checksum uint32 = crc.Sum32()

    if checksum != rec_checksum {
        var header Header
        err = binary.Read(buf, binary.LittleEndian, &header)
        if err != nil {
            return err
        }
        rec.Header = header
        fmt.Printf("%x\n",  buf.Bytes())
        log.Println("Decode: ", buf.Bytes())
        log.Println(rec.Header.Timestamp, rec.Header.KeySize, rec.Header.ValueSize)
        return fmt.Errorf("Error: checksum is not match %d != %d.", checksum, rec_checksum)
    }

	var header Header

    err = binary.Read(buf, binary.LittleEndian, &header)
    if err != nil {
        return err
    }
    rec.Header = header

    key := make([]byte, header.KeySize)
    _, err = buf.Read(key)
    if err != nil {
        return err
    }
    rec.Key = key

    value := make([]byte, header.ValueSize)
    _, err = buf.Read(value)
    if err != nil {
        return err
    }
    rec.Value = value

	return nil
}

func (rec *KVRecord) Encode() (*bytes.Buffer, error) {
    buf := new(bytes.Buffer)
    headerBytes := rec.Header.encodeHeader()

    buf.Write(headerBytes)
    buf.Write(rec.Key)
    buf.Write(rec.Value)

    crc := crc32.NewIEEE()
    crc.Write(buf.Bytes())
    checksum := crc.Sum32()

    data := new(bytes.Buffer)


    err := binary.Write(data, binary.LittleEndian, checksum)
    if err != nil {
        return new(bytes.Buffer), err
    }

    data.Write(buf.Bytes())

    return data, nil
}

func (rec *KVRecord) Sizer() int {
    return binary.Size(rec.Timestamp) + binary.Size(rec.KeySize) + 
        binary.Size(rec.ValueSize) + len(rec.Key) + len(rec.Value)
}