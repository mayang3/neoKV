package com.neoKV.network.payload;

import com.neoKV.network.AdminCommandType;
import com.neoKV.network.MessageType;

/**
 * Admin Command
 *
 * @author neo82
 */
public class AdminCommandMessage implements Message {
    private final AdminCommandType adminCommandType;

    private AdminCommandMessage(AdminCommandType adminCommandType) {
        this.adminCommandType = adminCommandType;
    }

    public static AdminCommandMessage of(AdminCommandType adminCommandType) {
        return new AdminCommandMessage(adminCommandType);
    }

    public AdminCommandType getAdminCommandType() {
        return adminCommandType;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.ADMIN_COMMAND;
    }

    @Override
    public String getKey() {
        return null;
    }
}
