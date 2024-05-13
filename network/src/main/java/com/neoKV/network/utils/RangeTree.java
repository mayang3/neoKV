package com.neoKV.network.utils;

import com.neoKV.network.exception.NeoKVException;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author neo82
 */
public class RangeTree<K extends Comparable<K>, V> {

    private final TreeMap<K, Map.Entry<K, V>> treeMap = new TreeMap<>();

    public V put(Range<K> range, V value) {
        if (treeMap.containsKey(range.getStart())) {
            throw new NeoKVException("The Ranges Overlap..");
        }

        Map.Entry<K, Map.Entry<K, V>> entry = treeMap.lowerEntry(range.getStart());

        if (entry != null && range.getStart().compareTo(entry.getValue().getKey()) <= 0) {
            throw new NeoKVException("The Ranges Overlap..");
        }

        treeMap.put(range.getStart(), Map.entry(range.getEnd(), value));

        return treeMap.get(range.getStart()).getValue();
    }


    public V get(K key) {
        Map.Entry<K, Map.Entry<K, V>> entry = treeMap.floorEntry(key);

        if (entry == null || entry.getValue().getKey().compareTo(key) < 0) {
            return null;
        }

        return entry.getValue().getValue();
    }
}
