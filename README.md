Disk based Log Structured Hash Table Store

Features
    Low latency for reads and writes
    High throughput
    Easy to back up / restore
    Simple and easy to understand
    Store data much larger than the RAM


# Our key value pair, when stored on disk looks like this:
#   ┌───────────┬──────────┬────────────┬─────┬───────┐
#   │ timestamp │ key_size │ value_size │ key │ value │
#   └───────────┴──────────┴────────────┴─────┴───────┘

1st 4 bytes are a 32-bit integer representing CRC.
The following 4 bytes are a 32-bit integer representing epoch timestamp.
The following 8 bytes are two 32-bit integers representing keysize and valuesize.
The remaining bytes are our key and value.


`format` module provides encode/decode functions for serialisation and deserialisation operations

`disk_store` module implements DiskStorage class which implements the KV store on the disk

`node` module is a kv storage node server procces