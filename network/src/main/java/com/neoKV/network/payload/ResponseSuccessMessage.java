package com.neoKV.network.payload;

import com.neoKV.network.DataType;
import com.neoKV.network.MessageType;

/**
 * @author neo82
 */
public class ResponseSuccessMessage implements Message {
    private final MessageType messageType;
    private final DataType dataType;
    private final String key;
    private final byte [] value;

    private ResponseSuccessMessage(MessageType messageType, DataType dataType, String key, byte[] value) {
        this.messageType = messageType;
        this.dataType = dataType;
        this.key = key;
        this.value = value;
    }

    public static ResponseSuccessMessage of(MessageType messageType, DataType dataType, String key, byte[] value) {
        return new ResponseSuccessMessage(messageType, dataType, key, value);
    }

    @Override
    public MessageType getMessageType() {
        return messageType;
    }

    public DataType getDataType() {
        return dataType;
    }

    @Override
    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }
}
