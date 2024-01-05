package com.neoKV.network;

import java.util.HashMap;
import java.util.Map;

public enum DataType {
    INTEGER((byte) 0, "int"),
    LONG((byte) 1, "long"),
    FLOAT((byte) 2, "float"),
    DOUBLE((byte) 3, "double"),
    STRING((byte) 4, "string"),
    LIST((byte) 5, "list"),
    MAP((byte) 6, "map"),
    SET((byte) 7, "set");

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
