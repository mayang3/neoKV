package com.neoKV.neoKVServer.storage;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author neo82
 */
public class MemtableSnapshot {

    private final ConcurrentSkipListMap<String, byte[]> concurrentSkipListMap;

    public MemtableSnapshot(final ConcurrentSkipListMap<String, byte[]> concurrentSkipListMap) {
        this.concurrentSkipListMap = concurrentSkipListMap;
    }

    public Set<String> keySet() {
        return this.concurrentSkipListMap.keySet();
    }

    public Collection<byte[]> values() {
        return this.concurrentSkipListMap.values();
    }

    public byte [] get(String key) {
        return this.concurrentSkipListMap.get(key);
    }

    public Set<Map.Entry<String, byte[]>> entrySet() {
        return this.concurrentSkipListMap.entrySet();
    }

    public boolean isEmpty() {
        return this.concurrentSkipListMap.isEmpty();
    }
}
