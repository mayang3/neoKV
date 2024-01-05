package com.neoKV.network.payload;

import com.neoKV.network.MessageType;

/**
 *
 */
public interface Message {

    MessageType getMessageType();

    String getKey();
}
