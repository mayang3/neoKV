package com.neoKV.neoKVServer.protocol;

import com.neoKV.neoKVServer.common.Constants;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

/**
 * @author neo82
 */
public class DataProtocol implements Protocol {
    private static final DataProtocol instance = new DataProtocol();

    private DataProtocol() {
    }

    public static DataProtocol getInstance() {
        return instance;
    }

    // format : [TOTAL_SIZE(4 byte)][TOMBSTONE(1 byte)][KEY_SIZE(4 byte)][KEY_BYTES_LENGTH(key bytes length)][VALUE_WITH_DATA_TYPE()]
    public ByteBuffer allocateDirect(Set<Map.Entry<String, byte[]>> data) {
        int totalLength = 0;

        for (Map.Entry<String, byte[]> entry : data) {
            totalLength += Constants.TOTAL_SIZE_BYTE_LENGTH;
            totalLength += Constants.TOMBSTONE_BYTE_LENGTH;
            totalLength += Constants.KEY_SIZE_BYTE_LENGTH;
            totalLength += entry.getKey().getBytes().length;
            totalLength += entry.getValue().length; // with dataType flag
        }

        return ByteBuffer.allocateDirect(totalLength);
    }

    /**
     *
     * @param byteBuffer
     * @param keyBytes
     * @param values : tombstone + data type + data bytes
     */
    public void putData(ByteBuffer byteBuffer, byte[] keyBytes, byte[] values) {
        byteBuffer.putInt( Constants.KEY_SIZE_BYTE_LENGTH + keyBytes.length + values.length); // set total length
        byteBuffer.putInt(keyBytes.length); // key length
        byteBuffer.put(keyBytes); // key data
        byteBuffer.put(values);
    }

}
