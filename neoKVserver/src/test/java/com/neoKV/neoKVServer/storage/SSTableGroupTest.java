package com.neoKV.neoKVServer.storage;

import org.junit.Test;

public class SSTableGroupTest {

    @Test
    public void testKK() {
        Memtable memtable = Memtable.getInstance();

//        for (int i = 0; i < 10000; i++) {
//            memtable.put("key" + i, ("value" + i).getBytes());
//        }

        SSTableGroup ssTableGroup = SSTableGroup.getInstance();
    }
}