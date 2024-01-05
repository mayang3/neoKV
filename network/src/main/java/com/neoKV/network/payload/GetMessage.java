package com.neoKV.network.payload;

import com.neoKV.network.DataType;
import com.neoKV.network.MessageType;

/**
 * @author neo82
 */
public class GetMessage implements Message {

    private final DataType dataType;
    private final String key;

    public GetMessage(DataType dataType, String key) {
        this.dataType = dataType;
        this.key = key;
    }

    public static GetMessage of(DataType dataType, String key) {
        return new GetMessage(dataType, key);
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.GET;
    }

    @Override
    public String getKey() {
        return key;
    }

    public DataType getDataType() {
        return dataType;
    }
}
