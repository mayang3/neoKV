package com.neoKV.neoKVServer.storage;

import com.neoKV.network.common.Constants;
import com.neoKV.network.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author neo82
 */
public class Memtable {
    private static final Logger log = LoggerFactory.getLogger(Memtable.class);
    private static final Memtable instance = new Memtable();
    private final AtomicReference<ConcurrentSkipListMap<String, byte[]>> mapRef = new AtomicReference<>(new ConcurrentSkipListMap<>());

    private final Semaphore semaphore = new Semaphore(1);

    private volatile MemtableSnapshot memtableSnapshot = null;


    public static Memtable getInstance() {
        return instance;
    }

    private Memtable() {
    }

    public byte[] put(String key, DataType dataType, byte[] value) {
        ByteBuffer buffer = ByteBuffer.allocate(    Constants.TOMBSTONE_BYTE_LENGTH + Constants.TIMESTAMP_BYTE_LENGTH + Constants.DATATYPE_BYTE_LENGTH + value.length);

        buffer.put(Constants.TOMBSTONE_ALIVE);
        buffer.putLong(System.currentTimeMillis());
        buffer.put(dataType.getCode());
        buffer.put(value);

        return this.mapRef.get().put(key, buffer.array());
    }

    public byte[] get(String key) {
        if (this.mapRef.get().containsKey(key)) {
            return this.mapRef.get().get(key);
        }

        return memtableSnapshot != null ? memtableSnapshot.get(key) : null;
    }

    public boolean containsKey(String key) {
        return this.mapRef.get().containsKey(key);
    }


    /**
     * This method will run in the background.
     *
     * @return
     */
    public MemtableSnapshot snapshot() {
        try {
            semaphore.acquire();
            final ConcurrentSkipListMap<String, byte[]> tmp = this.mapRef.getAndSet(new ConcurrentSkipListMap<>());
            return this.memtableSnapshot = new MemtableSnapshot(tmp);

        } catch (InterruptedException ie) {
            log.error("[Memtable] snapshot interrupted.. ", ie);
        } finally {
            semaphore.release();
        }

        return null;
    }
}
