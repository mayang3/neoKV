package com.neoKV.neoKVServer.storage;

import com.neoKV.neoKVServer.file.DirectBufferReader;
import com.neoKV.neoKVServer.file.DirectBufferWriter;
import com.neoKV.neoKVServer.filter.BloomFilter;
import com.neoKV.neoKVServer.filter.SparseIndex;
import com.neoKV.neoKVServer.merge_compaction.lock.CompactionReadWriteLock;
import com.neoKV.network.common.Constants;
import com.neoKV.network.utils.FilePathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author neo82
 */
public class SSTable {
    private static final Logger log = LoggerFactory.getLogger(SSTable.class);
    private final DirectBufferReader directBufferReader = DirectBufferReader.getInstance();
    private final BloomFilter bloomFilter = new BloomFilter(10000);
    private final SparseIndex sparseIndex;
    private final Path dataFilePath;
    private final Path indexFilePath;
    private AtomicInteger dataCount = new AtomicInteger(0);


    public SSTable(Set<String> keys, SparseIndex sparseIndex, Path dataFilePath, Path indexFilePath) {
        this.sparseIndex = sparseIndex;
        this.dataFilePath = dataFilePath;
        this.indexFilePath = indexFilePath;

        for (String k : keys) {
            this.bloomFilter.put(k, 5);
        }
    }

    public SSTable(Path dataFilePath) {
        this.sparseIndex = new SparseIndex();
        this.dataFilePath = dataFilePath;
        this.indexFilePath = FilePathUtils.getIndexFilePathBy(dataFilePath.toString());
    }

    public boolean mightContains(String key) {
        return this.bloomFilter.mightContains(key, 5);
    }


    public ByteBuffer get(String key) {
        try {
            CompactionReadWriteLock.readLock().lock();

            Long lower = sparseIndex.floorIndex(key);
            Long upper = sparseIndex.higherEntry(key);

            return directBufferReader.findPos(dataFilePath, key, lower, upper);
        } catch (Exception e) {
            log.error("[SSTableCore] get error! key is : {}", key, e);
        } finally {
            CompactionReadWriteLock.readLock().unlock();
        }

        return null;
    }

    public void commit(DataRecord dataRecord) {
        ByteBuffer buffer = dataRecord.toDirectByteBuffer();
        DirectBufferWriter.getInstance().saveToFileQuietly(dataFilePath, buffer);
    }
}
