package com.neoKV.network.utils;

import com.neoKV.network.DataType;

import java.nio.ByteBuffer;

/**
 * @author junbeom.bae
 */
public final class ByteBufferUtils {

    public static byte [] getByteArrayBy(DataType dataType, String val) {
        switch (dataType) {
            case INTEGER:
                return getIntByteArray(val);
            case LONG:
                return getLongByteArray(val);
            case FLOAT:
                return getFloatByteArray(val);
            case DOUBLE:
                return getDoubleByteArray(val);
            case STRING:
                return val.getBytes();
        }

        throw new IllegalArgumentException(String.format("[ByteBufferUtils] dataType is %s", dataType));
    }

    public static byte [] getIntByteArray(String val) {
        return ByteBuffer.allocate(4).putInt(Integer.parseInt(val)).array();
    }

    public static byte [] getLongByteArray(String val) {
        return ByteBuffer.allocate(8).putLong(Long.parseLong(val)).array();
    }

    public static byte [] getFloatByteArray(String val) {
        return ByteBuffer.allocate(4).putFloat(Float.parseFloat(val)).array();
    }

    public static byte [] getDoubleByteArray(String val) {
        return ByteBuffer.allocate(8).putDouble(Double.parseDouble(val)).array();
    }
}
