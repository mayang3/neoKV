package com.neoKV.network.payload;

import com.neoKV.network.MessageType;

/**
 * @author neo82
 */
public class ResponseFailMessage implements Message {

    private final String reason;

    public ResponseFailMessage(String reason) {
        this.reason = reason;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.RESPONSE_ERROR;
    }

    @Override
    public String getKey() {
        return null;
    }

    public String getReason() {
        return reason;
    }
}
