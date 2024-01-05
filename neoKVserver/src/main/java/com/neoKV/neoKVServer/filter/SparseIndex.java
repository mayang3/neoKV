package com.neoKV.neoKVServer.filter;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author neo82
 */
public class SparseIndex {
    private int totalSize = 0;
    private final TreeMap<String, Integer> indices = new TreeMap<>();

    private static final int density = 100;

    public Integer put(String k, Integer v) {
        totalSize += 4; // key size
        totalSize += k.getBytes().length; // key bytes
        totalSize += 4; // pos in file

        return indices.put(k, v);
    }

    public int getTotalSize() {
        return totalSize;
    }

    public TreeMap<String, Integer> getIndices() {
        return indices;
    }

    public int getDensity() {
        return density;
    }

    public int ceilingIndex(String key) {
        Map.Entry<String, Integer> entry = this.indices.ceilingEntry(key);

        if (entry == null) {
            return 0;
        }

        return entry.getValue();
    }

    public int floorIndex(String key) {
        Map.Entry<String, Integer> entry = this.indices.floorEntry(key);

        if (entry == null) {
            return indices.lastEntry().getValue();
        }

        return entry.getValue();
    }
}
