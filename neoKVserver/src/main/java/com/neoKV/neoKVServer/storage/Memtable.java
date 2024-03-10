package com.neoKV.neoKVServer.storage;

import com.neoKV.network.common.Constants;
import com.neoKV.network.DataType;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author neo82
 */
public class Memtable {
    private static final Memtable instance = new Memtable();
    private final AtomicReference<ConcurrentSkipListMap<String, byte[]>> mapRef = new AtomicReference<>(new ConcurrentSkipListMap<>());

    private final Semaphore semaphore = new Semaphore(1);

    private final SSTableGroup ssTableGroup = SSTableGroup.getInstance();

    private final AtomicInteger putCounter = new AtomicInteger(0);


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

        return this.mapRef.get().put(key, buffer.array());
    }

    public byte[] get(String key) {
        return this.mapRef.get().get(key);
    }

    public boolean containsKey(String key) {
        return this.mapRef.get().containsKey(key);
    }


    public MemtableSnapshot snapshot() {
        try {
            semaphore.acquire();
            final ConcurrentSkipListMap<String, byte[]> tmp = this.mapRef.getAndSet(new ConcurrentSkipListMap<>());
            return new MemtableSnapshot(tmp);

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } finally {
            semaphore.release();
        }

        return null;
    }
}
