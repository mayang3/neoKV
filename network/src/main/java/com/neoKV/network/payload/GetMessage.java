package com.neoKV.network.payload;

import com.neoKV.network.DataType;
import com.neoKV.network.MessageType;

/**
 * @author neo82
 */
public class GetMessage implements Message {

    private final String key;

    public GetMessage(String key) {
        this.key = key;
    }

    public static GetMessage of(String key) {
        return new GetMessage(key);
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.GET;
    }

    @Override
    public String getKey() {
        return key;
    }

}
