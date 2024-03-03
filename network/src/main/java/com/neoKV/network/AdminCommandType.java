package com.neoKV.network;

import java.util.HashMap;
import java.util.Map;

public enum AdminCommandType {
    FLUSH("flush");

    private final String code;

    AdminCommandType(String code) {
        this.code = code;
    }

    private static final Map<String, AdminCommandType> CODE_MAP = new HashMap<>();

    static {
        for (AdminCommandType adminCommandType : AdminCommandType.values()) {
            CODE_MAP.put(adminCommandType.code, adminCommandType);
        }
    }

    public static AdminCommandType findEnum(String subCommand) {
        return CODE_MAP.get(subCommand);
    }

    public String getCode() {
        return code;
    }
}
