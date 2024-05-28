package com.neoKV.neoKVServer.storage;

import com.neoKV.neoKVServer.file.DirectBufferReader;
import com.neoKV.neoKVServer.file.DirectBufferWriter;
import com.neoKV.neoKVServer.filter.BloomFilter;
import com.neoKV.neoKVServer.filter.SparseIndex;
import com.neoKV.neoKVServer.merge_compaction.iterator.MergeIterator;
import com.neoKV.neoKVServer.merge_compaction.lock.CompactionReadWriteLock;
import com.neoKV.network.common.Constants;
import com.neoKV.network.utils.FilePathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
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


    public SSTable(Path dataFilePath) {
        this.sparseIndex = new SparseIndex();
        this.dataFilePath = dataFilePath;
        this.indexFilePath = FilePathUtils.getIndexFilePathBy(dataFilePath.toString());

        if (Files.exists(this.dataFilePath)) {
            loadBloomFilter();
        }

        if (Files.exists(this.indexFilePath)) {
            loadIndex();
        }
    }

    public SSTable(Path dataFilePath, Path indexFilePath, Set<Map.Entry<String, byte[]>> entries) {
        this.dataFilePath = dataFilePath;
        this.indexFilePath = indexFilePath;

        this.sparseIndex = DirectBufferWriter.getInstance().writeData(dataFilePath, entries);
        DirectBufferWriter.getInstance().writeIndex(indexFilePath, sparseIndex);

        for (Map.Entry<String, byte[]> entry : entries) {
            this.bloomFilter.put(entry.getKey());
        }
    }

    public boolean mightContains(String key) {
        return this.bloomFilter.mightContains(key);
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


    private void loadIndex() {
        try {
            ByteBuffer buffer = DirectBufferReader.getInstance().read(this.indexFilePath);

            while (buffer.hasRemaining()) {
                int keyLength = buffer.getInt();
                byte[] keyBytes = new byte[keyLength];
                buffer.get(keyBytes, 0, keyLength);

                this.sparseIndex.put(new String(keyBytes), buffer.getLong());
            }
        } catch (Exception e) {
            log.error("[SSTable] loadIndex error! indexFilePath:{}", indexFilePath, e);
        }
    }

    private void loadBloomFilter() {
        try (MergeIterator iterator = new MergeIterator(this.dataFilePath)) {
            for (ByteBuffer buffer : iterator) {
                DataRecord dataRecord = DataRecord.of(buffer);
                if (!dataRecord.isEmpty()) {
                    bloomFilter.put(new String(dataRecord.getKey()));
                }
            }
        }
    }
}
