![image](https://img.shields.io/badge/neoKV-0.1-green)

# NeoKV

NeoKV is a non-heap, LSM tree-based distributed key-value storage.

The Dynamo architecture is used in neoKV for high availability, high reliability, high scalability, and decentralized storage.


## NeoKV Tree Structure
NeoKV utilizes a multicomponent LSM tree structure. This structure flushes the entire MemTable at once, and as flushes occur repeatedly, multiple tables are created on the disk.
The number of these tables increases over time, which can lead to degradation in read performance.

To address this issue, NeoKV periodically performs Compaction & Merge operations.

## Merge & Compaction
In NeoKV, the well-known Leveled Compaction technique is used. This method is also known to be used by RocksDB.

![image](https://github.com/mayang3/neoKV/assets/14806803/2f843b5d-a960-439c-9a6b-c36df716f20c)

![image](https://github.com/mayang3/neoKV/assets/14806803/03b157b0-ef77-4575-a79b-27281b2981b4)
