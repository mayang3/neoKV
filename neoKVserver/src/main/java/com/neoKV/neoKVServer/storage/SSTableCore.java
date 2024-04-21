package com.neoKV.neoKVServer.storage;

import com.neoKV.neoKVServer.file.DirectBufferReader;
import com.neoKV.neoKVServer.filter.BloomFilter;
import com.neoKV.neoKVServer.filter.SparseIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Set;

/**
 * @author neo82
 */
public class SSTableCore {
    private static final Logger log = LoggerFactory.getLogger(SSTableCore.class);
    private final DirectBufferReader directBufferReader = DirectBufferReader.getInstance();
    private final BloomFilter bloomFilter;
    private final SparseIndex sparseIndex;
    private final Path dataFilePath;
    private final Path indexFilePath;

    public SSTableCore(Set<String> keys, SparseIndex sparseIndex, Path dataFilePath, Path indexFilePath) {
        this.bloomFilter = new BloomFilter(10000);
        this.sparseIndex = sparseIndex;
        this.dataFilePath = dataFilePath;
        this.indexFilePath = indexFilePath;

        for (String k : keys) {
            this.bloomFilter.put(k, 5);
        }
    }
    public boolean mightContains(String key) {
        return this.bloomFilter.mightContains(key, 5);
    }


    public ByteBuffer get(String key) {
        try {
            Long lower = sparseIndex.floorIndex(key);
            Long upper = sparseIndex.higherEntry(key);

            return directBufferReader.findPos(dataFilePath, key, lower, upper);
        } catch (Exception e) {
            log.error("[SSTableCore] get error! key is : {}", key, e);
        }

        return null;
    }
}
