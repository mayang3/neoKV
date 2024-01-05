package com.neoKV.neoKVServer.protocol;

/**
 * @author neo82
 */
public enum OperationType {
    PUT(0),
    GET(1),
    DELETE(2),
    RESPONSE_SUCCESS(3),
    RESPONSE_ERROR(4);

    private final int code;

    OperationType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
