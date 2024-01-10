package com.neoKV.network;

import java.util.HashMap;
import java.util.Map;

public enum DataType {
    NULL((byte)0, "null"),
    INTEGER((byte) 1, "int"),
    LONG((byte) 2, "long"),
    FLOAT((byte) 3, "float"),
    DOUBLE((byte) 4, "double"),
    STRING((byte) 5, "string"),
    LIST((byte) 6, "list"),
    MAP((byte) 7, "map"),
    SET((byte) 8, "set");

    private final byte code;
    private final String command;

    private static final Map<Byte, DataType> BYTE_CODE_MAP = new HashMap<>();
    private static final Map<String, DataType> STRING_CODE_MAP = new HashMap<>();


    static {
        for (DataType dataType : DataType.values()) {
            BYTE_CODE_MAP.put(dataType.getCode(), dataType);
            STRING_CODE_MAP.put(dataType.getCommand(), dataType);
        }
    }

    DataType(byte code, String command) {
        this.code = code;
        this.command = command;
    }

    public static DataType of(byte code) {
        return BYTE_CODE_MAP.get(code);
    }

    public static DataType of(String command) {
        return STRING_CODE_MAP.get(command);
    }

    public byte getCode() {
        return code;
    }

    public String getCommand() {
        return command;
    }
}
