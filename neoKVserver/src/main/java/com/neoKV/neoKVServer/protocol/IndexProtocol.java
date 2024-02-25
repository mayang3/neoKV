package com.neoKV.neoKVServer.protocol;

import com.neoKV.neoKVServer.filter.SparseIndex;

import java.nio.ByteBuffer;

/**
 * @author neo82
 */
public class IndexProtocol implements Protocol {

    private static final IndexProtocol instance = new IndexProtocol();

    public static IndexProtocol getInstance() {
        return instance;
    }

    public ByteBuffer allocateDirect(SparseIndex sparseIndex) {
        return ByteBuffer.allocateDirect(sparseIndex.getTotalSize());
    }

    public void putIndexes(ByteBuffer byteBuffer, byte [] keyBytes, int pos) {
        byteBuffer.putInt(keyBytes.length);
        byteBuffer.put(keyBytes);
        byteBuffer.putInt(pos);
    }
}
