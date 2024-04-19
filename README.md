# NeoKV

NeoKV is a non-heap, LSM tree-based distributed key-value storage.

The Dynamo architecture is used in neoKV for high availability, high reliability, high scalability, and decentralized storage.


## NeoKV Tree Structure
NeoKV utilizes a multicomponent LSM tree structure. This structure flushes the entire MemTable at once, and as flushes occur repeatedly, multiple tables are created on the disk.
The number of these tables increases over time, which can lead to degradation in read performance.

To address this issue, NeoKV periodically performs Compaction & Merge operations.

## Merge & Compaction
In NeoKV, the well-known Leveled Compaction technique is used. This method is also known to be used by RocksDB.
