package com.neoKV.neoKVServer.storage;

import com.neoKV.network.common.Constants;

import java.nio.ByteBuffer;

/**
 * @author neo82
 */
public class DataRecord {
    private static final DataRecord EMPTY = new DataRecord();
    private final byte[] key;
    private final byte tombstone;
    private final long timestamp;
    private final byte dataType;
    private final byte[] data;

    private DataRecord(ByteBuffer buffer) {
        int pos = buffer.position();

        // key
        int keyLength = buffer.getInt();
        this.key = new byte[keyLength];
        buffer.get(this.key);

        this.tombstone = buffer.get();
        this.timestamp = buffer.getLong();
        this.dataType = buffer.get();

        // value
        this.data = new byte[buffer.remaining()];
        buffer.get(this.data);

        buffer.position(pos);
    }

    private DataRecord() {
        this.key = null;
        this.tombstone = 0;
        this.timestamp = 0;
        this.dataType = 0;
        this.data = null;
    }

    public static DataRecord of(ByteBuffer buffer) {
        return new DataRecord(buffer);
    }

    public static DataRecord empty() {
        return EMPTY;
    }

    public boolean isEmpty() {
        return this == EMPTY;
    }

    public byte[] getKey() {
        return key;
    }

    public byte getTombstone() {
        return tombstone;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte getDataType() {
        return dataType;
    }

    public byte[] getData() {
        return data;
    }

    public ByteBuffer toDirectByteBuffer() {
        int totalSize = Constants.KEY_SIZE_BYTE_LENGTH + key.length + Constants.TOMBSTONE_BYTE_LENGTH + Constants.TIMESTAMP_BYTE_LENGTH + Constants.DATATYPE_BYTE_LENGTH + data.length;

        ByteBuffer buffer = ByteBuffer.allocateDirect(Constants.TOTAL_SIZE_BYTE_LENGTH + totalSize);

        buffer.putInt(totalSize);
        buffer.putInt(key.length);
        buffer.put(key);
        buffer.put(tombstone);
        buffer.putLong(timestamp);
        buffer.put(dataType);
        buffer.put(data);

        buffer.flip();

        return buffer;
    }
}
