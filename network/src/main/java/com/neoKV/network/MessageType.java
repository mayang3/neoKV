package com.neoKV.network;

import java.util.HashMap;
import java.util.Map;

public enum MessageType {

    PUT((byte) 0),
    GET((byte) 1),
    DELETE((byte) 2),
    RESPONSE_SUCCESS((byte) 3),
    RESPONSE_ERROR((byte) 4),

    ADMIN_COMMAND((byte)5);

    private final byte code;

    private static final Map<Byte, MessageType> CODE_MAP = new HashMap<>();

    static {
        for (MessageType messageType : MessageType.values()) {
            CODE_MAP.put(messageType.getCode(), messageType);
        }
    }

    MessageType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static MessageType of(byte code) {
        return CODE_MAP.get(code);
    }


}
