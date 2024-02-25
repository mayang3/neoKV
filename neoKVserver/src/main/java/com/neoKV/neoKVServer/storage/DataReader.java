package com.neoKV.neoKVServer.storage;

import com.neoKV.neoKVServer.common.Constants;

import java.nio.ByteBuffer;

/**
 * @author neo82
 */
public class DataReader {

    private static final DataReader instance = new DataReader();
    private final Memtable memtable = Memtable.getInstance();
    private final SSTableGroup ssTableGroup = SSTableGroup.getInstance();

    private DataReader() {}

    public static DataReader getInstance() {
        return instance;
    }

    public ByteBuffer get(String key) {
        ByteBuffer byteBuffer;

        if (memtable.containsKey(key)) {
            byteBuffer =  ByteBuffer.wrap(memtable.get(key));
        } else {
            byteBuffer = ssTableGroup.get(key);
        }

        if (byteBuffer.get() == Constants.TOMBSTONE_DELETED) {
            return null;
        }

        // skip tombstone
        byteBuffer.position(1);

        return byteBuffer;
    }
}
