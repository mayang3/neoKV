package com.neoKV.neoKVServer.storage;

import com.neoKV.network.common.Constants;
import com.neoKV.network.DataType;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author neo82
 */
public class Memtable {
    private static final Memtable instance = new Memtable();
    private final AtomicReference<ConcurrentSkipListMap<String, byte[]>> mapRef = new AtomicReference<>(new ConcurrentSkipListMap<>());

    private final SSTableGroup ssTableGroup = SSTableGroup.getInstance();

    private final Lock lock = new ReentrantLock();

    private MemtableSnapshot memtableSnapshot = null;

    private static final int MEMTABLE_MAX_SIZE = 5;

    public static Memtable getInstance() {
        return instance;
    }

    private Memtable() {
    }

    public byte[] put(String key, DataType dataType, byte[] value) {
        ByteBuffer buffer = ByteBuffer.allocate(value.length + Constants.TOMBSTONE_BYTE_LENGTH + Constants.DATATYPE_BYTE_LENGTH);

        buffer.put(Constants.TOMBSTONE_ACTIVE);
        buffer.put(dataType.getCode());
        buffer.put(value);

        flushIfSizeOver();

        return this.mapRef.get().put(key, buffer.array());
    }

    public void forceFlush() {
        try {
            lock.lock();

            final ConcurrentSkipListMap<String, byte[]> tmp = this.mapRef.get();
            this.mapRef.set(new ConcurrentSkipListMap<>());
            this.memtableSnapshot = new MemtableSnapshot(tmp);
            ssTableGroup.saveToSSTable(this.memtableSnapshot);

        } finally {
            lock.unlock();
        }
    }

    private void flushIfSizeOver() {
        try {
            lock.lock();

            if (this.mapRef.get().size() >= MEMTABLE_MAX_SIZE) {
                final ConcurrentSkipListMap<String, byte[]> tmp = this.mapRef.get();
                this.mapRef.set(new ConcurrentSkipListMap<>());
                this.memtableSnapshot = new MemtableSnapshot(tmp);
                ssTableGroup.saveToSSTable(this.memtableSnapshot);
            }

        } finally {
            lock.unlock();
        }
    }

    public byte[] get(String key) {
        if (this.mapRef.get().containsKey(key)) {
            return this.mapRef.get().get(key);
        }

        return  this.memtableSnapshot != null ? this.memtableSnapshot.get(key) : null;
    }

    public boolean containsKey(String key) {
        return this.mapRef.get().containsKey(key);
    }
}
