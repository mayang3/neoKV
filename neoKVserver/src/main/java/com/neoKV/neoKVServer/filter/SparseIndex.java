package com.neoKV.neoKVServer.filter;

import com.neoKV.network.common.Constants;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author neo82
 */
public class SparseIndex {
    private int totalSize = 0;
    private final TreeMap<String, Long> indices = new TreeMap<>();

    public Long put(String k, Long v) {
        totalSize += Constants.INDEX_KEY_SIZE_BYTE_LENGTH; // key size
        totalSize += k.getBytes().length; // key bytes
        totalSize += Constants.INDEX_POSITION_SIZE_BYTE_LENGTH; // pos in file

        return indices.put(k, v);
    }

    public int getTotalSize() {
        return totalSize;
    }

    public TreeMap<String, Long> getIndices() {
        return indices;
    }

    public int getDensity() {
        return Constants.SPARSE_INDEX_DENSITY;
    }

    public Long higherEntry(String key) {
        Map.Entry<String, Long> entry = this.indices.higherEntry(key);

        if (entry == null) {
            return null;
        }

        return entry.getValue();
    }

    public Long floorIndex(String key) {
        Map.Entry<String, Long> entry = this.indices.floorEntry(key);

        if (entry == null) {
            return 0L;
        }

        return entry.getValue();
    }
}
