package com.neoKV.neoKVServer.serialize;

import com.neoKV.network.DataType;
import com.neoKV.neoKVServer.storage.DataReader;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * in case of primitive types...
 * [DATA_TYPE(1 byte)][TOMBSTONE_FLAG(1 byte)][DATA]
 *
 * in case of String
 * [DATA_TYPE(1 byte)][TOMBSTONE_FLAG(1 byte)][DATA_LENGTH(4 bytes)][DATA]
 *
 * in case of List,Set
 * [DATA_TYPE(1 byte)][TOMBSTONE_FLAG(1 byte)][LIST_LENGTH(4 bytes)][DATA_LENGTH(4 bytes)][DATA]...
 *
 * in case of Map
 * [DATA_TYPE(1 byte)][TOMBSTONE_FLAG(1 byte)][MAP_LENGTH(4 bytes)][KEY_LENGTH(4 bytes)][KEY][DATA_LENGTH(4 bytes)][DATA]...
 *
 * @author neo82
 */
public class DataDeserializer {

    private final DataReader dataReader = DataReader.getInstance();

    @SuppressWarnings("unchecked")
    public <T> T read(String key) {
//        ByteBuffer byteBuffer = dataReader.get(key);
//
//        if (byteBuffer == null) {
//            return null;
//        }
//
//        byte code = byteBuffer.get();
//        byte tombstone = byteBuffer.get();
//
//        if (tombstone == 1) {
//            throw new NoSuchElementException(key);
//        }
//
//        Object res = null;
//
//        switch (DataType.of(code)) {
//            case INTEGER:
//                res = byteBuffer.getInt();
//                break;
//            case LONG:
//                res = byteBuffer.getLong();
//                break;
//            case FLOAT:
//                res = byteBuffer.getFloat();
//                break;
//            case DOUBLE:
//                res = byteBuffer.getDouble();
//                break;
//            case STRING:
//                res = getString(byteBuffer);
//                break;
//            case LIST:
//                res = getList(byteBuffer);
//                break;
//            case SET:
//                res = getSet(byteBuffer);
//                break;
//            case MAP:
//                res = getMap(byteBuffer);
//                break;
//        }
//
//        byteBuffer.clear();
//
//        return (T)res;
//    }
//
//    private Map<String, String> getMap(ByteBuffer byteBuffer) {
//        Map<String, String> map = new HashMap<>();
//        int length = byteBuffer.getInt();
//
//        for (int i = 0; i < length; i++) {
//            byte [] keyBytes = new byte[byteBuffer.getInt()];
//            byteBuffer.get(keyBytes);
//
//            byte [] valueBytes = new byte[byteBuffer.getInt()];
//            byteBuffer.get(valueBytes);
//
//            map.put(new String(keyBytes), new String(valueBytes));
//        }

        return null;
    }

    private Set<String> getSet(ByteBuffer byteBuffer) {
        Set<String> set = new HashSet<>();
        int length = byteBuffer.getInt();

        for (int i = 0; i < length; i++) {
            byte [] bytes = new byte[byteBuffer.getInt()];
            byteBuffer.get(bytes);
            set.add(new String(bytes));
        }

        return set;
    }

    private List<String> getList(ByteBuffer byteBuffer) {
        List<String> list = new ArrayList<>();
        int length = byteBuffer.getInt();

        for (int i = 0; i < length; i++) {
            byte [] bytes = new byte[byteBuffer.getInt()];
            byteBuffer.get(bytes);
            list.add(new String(bytes));
        }

        return list;
    }

    private String getString(ByteBuffer byteBuffer) {
        int length = byteBuffer.getInt();
        byte [] bytes = new byte[length];
        byteBuffer.get(bytes);
        return new String(bytes);
    }
}
