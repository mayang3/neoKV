package com.neoKV.network.utils;

import com.neoKV.network.DataType;
import com.neoKV.network.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static io.netty.util.internal.shaded.org.jctools.util.UnsafeAccess.UNSAFE;

/**
 * @author neo82
 */
public final class ByteBufferUtils {
    private static final Logger log = LoggerFactory.getLogger(ByteBufferUtils.class);

    public static boolean notExists(ByteBuffer byteBuffer) {
        return byteBuffer == null || !byteBuffer.hasRemaining() || byteBuffer.get() == Constants.TOMBSTONE_DELETED;
    }

    public static byte [] getByteArrayBy(DataType dataType, String val) {
        return switch (dataType) {
            case INTEGER -> getIntByteArray(val);
            case LONG -> getLongByteArray(val);
            case FLOAT -> getFloatByteArray(val);
            case DOUBLE -> getDoubleByteArray(val);
            case STRING -> val.getBytes();
            default -> throw new IllegalArgumentException(String.format("[ByteBufferUtils] dataType is %s", dataType));
        };

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

    public static void clean(ByteBuffer buffer) {
        if (buffer != null && buffer.isDirect()) {
            try {
                UNSAFE.invokeCleaner(buffer);
            } catch (Exception e) {
                log.error("[ByteBufferUtils] clean error", e);
            }
        }
    }
}
