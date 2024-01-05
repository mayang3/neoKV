package com.neoKV.neoKVServer.serialize;

import org.junit.Test;

public class DataDeserializerTest {

    @Test
    public void test33() {
        String value = new DataDeserializer().read("abc");

        System.out.println(value);
    }
}