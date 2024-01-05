package com.neoKV.network.payload;

import com.neoKV.network.DataType;
import com.neoKV.network.MessageType;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @author neo82
 */
public class PutMessage implements Message {

    private final DataType dataType;
    private final String key;
    private final byte[] value;

    private PutMessage(DataType dataType, String key, byte[] value) {
        this.dataType = dataType;
        this.key = key;
        this.value = value;
    }

    public static PutMessage of(DataType dataType, String key, byte[] value) {
        return new PutMessage(dataType, key, value);
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.PUT;
    }

    public DataType getDataType() {
        return dataType;
    }

    public String getKey() {
        return this.key;
    }

    public byte[] getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
