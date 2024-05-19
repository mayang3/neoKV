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
        if (treeMap.containsKey(range.start())) {
            throw new NeoKVException("The Ranges Overlap..");
        }

        Map.Entry<K, Map.Entry<K, V>> entry = treeMap.lowerEntry(range.start());

        if (entry != null && range.start().compareTo(entry.getValue().getKey()) <= 0) {
            throw new NeoKVException("The Ranges Overlap..");
        }

        treeMap.put(range.start(), Map.entry(range.end(), value));

        return treeMap.get(range.start()).getValue();
    }


    public V get(K key) {
        Map.Entry<K, Map.Entry<K, V>> entry = treeMap.floorEntry(key);

        if (entry == null || entry.getValue().getKey().compareTo(key) < 0) {
            return null;
        }

        return entry.getValue().getValue();
    }
}
