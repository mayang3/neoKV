package com.neoKV.neoKVServer.storage;

import com.neoKV.network.DataType;

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

        if (memtable.containsKey(key)) {
            return ByteBuffer.wrap(memtable.get(key));
        }

        return ssTableGroup.get(key);
    }
}
