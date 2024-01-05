package com.neoKV.neoKVServer.storage;

import com.neoKV.network.DataType;
import org.junit.Test;

import java.nio.ByteBuffer;

public class MemtableTest {

    private Memtable memtable = Memtable.getInstance();

    @Test
    public void testGet() {
//        String value = "VALUE1";
//
//        ByteBuffer byteBuffer = ByteBuffer.allocate(value.getBytes().length + 6);
//        byteBuffer.put(DataType.STRING.getCode());
//        byteBuffer.put((byte)0);
//        byteBuffer.putInt(value.getBytes().length);
//        byteBuffer.put(value.getBytes());
//
//        memtable.put("KEY1", DataType.STRING, byteBuffer.array());
//        System.out.println(memtable.get("KEY1", DataType.STRING));
    }
}