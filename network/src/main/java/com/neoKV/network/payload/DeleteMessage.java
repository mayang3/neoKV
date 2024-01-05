package com.neoKV.network.payload;

import com.neoKV.network.DataType;
import com.neoKV.network.MessageType;

/**
 * @author neo82
 */
public class DeleteMessage implements Message {

    private final MessageType messageType;
    private final DataType dataType;
    private final String key;

    public DeleteMessage(MessageType messageType, DataType dataType, String key) {
        this.messageType = messageType;
        this.dataType = dataType;
        this.key = key;
    }

    public static DeleteMessage of(MessageType messageType, DataType dataType, String key) {
        return new DeleteMessage(messageType, dataType, key);
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public DataType getDataType() {
        return dataType;
    }

    public String getKey() {
        return key;
    }
}
