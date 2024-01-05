package com.neoKV.network;

import java.util.HashMap;
import java.util.Map;

/**
 * @author neo82
 */
public enum OperationCode {
    PUT((byte)0),
    GET((byte)1),
    DELETE((byte)2),
    RESPONSE_SUCCESS((byte)3),
    RESPONSE_ERROR((byte)4);

    private final byte code;

    private static final Map<Byte, OperationCode> CODE_MAP = new HashMap<>();

    static {
        for (OperationCode code : OperationCode.values()) {
            CODE_MAP.put(code.getCode(), code);
        }
    }

    OperationCode(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static OperationCode of(byte code) {
        return CODE_MAP.get(code);
    }
}
