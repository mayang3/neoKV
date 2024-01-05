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

    public ByteBuffer get(String key, DataType dataType) {
        String generatedKey = key + "_" + dataType.getCommand();

        if (memtable.containsKey(generatedKey)) {
            return ByteBuffer.wrap(memtable.get(generatedKey));
        }

        return ssTableGroup.get(generatedKey);
    }
}
