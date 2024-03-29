package com.neoKV.network.encoder;

import com.neoKV.network.AdminCommandType;
import com.neoKV.network.MessageType;
import com.neoKV.network.Packet;
import com.neoKV.network.common.Constants;
import com.neoKV.network.payload.*;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;
import java.util.UUID;

/**
 * @author neo82
 */
public class MessageToPacketEncoder extends MessageToMessageEncoder<Message> {

    static int ERROR_REASON_BYTE_LENGTH = 4;
    static int MESSAGE_CODE_BYTE_LENGTH = 1;
    static int DATA_CODE_BYTE_LENGTH = 1;
    static int KEY_BYTES_LENGTH = 4;

    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, List<Object> out) throws Exception {
        Packet packet = new Packet(ByteBufAllocator.DEFAULT.buffer(64));

        MessageType messageType = msg.getMessageType();
        String key = msg.getKey();
        String uuid = UUID.randomUUID().toString();

        if (msg instanceof PutMessage) { // client side
            PutMessage putMessage = (PutMessage) msg;
            byte[] value = putMessage.getValue();
            int totalLength = uuid.getBytes().length + MESSAGE_CODE_BYTE_LENGTH + DATA_CODE_BYTE_LENGTH + KEY_BYTES_LENGTH + key.getBytes().length + value.length;

            packet.getBuf().writeInt(totalLength); // Total Length
            packet.getBuf().writeBytes(uuid.getBytes()); // UUID
            packet.getBuf().writeByte(messageType.getCode()); // Message Type, PUT, GET, DELETE
            packet.getBuf().writeByte(putMessage.getDataType().getCode()); // Data Type code
            packet.getBuf().writeInt(key.length()); // bytes length of key
            packet.getBuf().writeBytes(key.getBytes()); // key bytes
            packet.getBuf().writeBytes(value); // value bytes
        } else if (msg instanceof GetMessage) { // client side
            GetMessage getMessage = (GetMessage) msg;

            int totalLength = uuid.getBytes().length + MESSAGE_CODE_BYTE_LENGTH + KEY_BYTES_LENGTH + key.getBytes().length;

            packet.getBuf().writeInt(totalLength);
            packet.getBuf().writeBytes(uuid.getBytes());
            packet.getBuf().writeByte(messageType.getCode());
            packet.getBuf().writeInt(key.length());
            packet.getBuf().writeBytes(key.getBytes());

        } else if (msg instanceof ResponseSuccessMessage) { // server side
            ResponseSuccessMessage responseSuccessMessage = (ResponseSuccessMessage) msg;

            byte[] value = responseSuccessMessage.getValue();
            int totalLength = uuid.getBytes().length + MESSAGE_CODE_BYTE_LENGTH + MESSAGE_CODE_BYTE_LENGTH + KEY_BYTES_LENGTH + key.getBytes().length + DATA_CODE_BYTE_LENGTH + value.length;

            packet.getBuf().writeInt(totalLength); // Total Length
            packet.getBuf().writeBytes(uuid.getBytes()); // UUID
            packet.getBuf().writeByte(MessageType.RESPONSE_SUCCESS.getCode()); // only if response success
            packet.getBuf().writeByte(messageType.getCode()); // Message Type, PUT, GET, DELETE
            packet.getBuf().writeInt(key.length()); // bytes length of key
            packet.getBuf().writeBytes(key.getBytes()); // key bytes
            packet.getBuf().writeByte(responseSuccessMessage.getDataType().getCode());
            packet.getBuf().writeBytes(value); // value bytes
        } else if (msg instanceof ResponseFailMessage) {
            ResponseFailMessage responseFailMessage = (ResponseFailMessage) msg;

            String reason = responseFailMessage.getReason();
            int totalLength = uuid.getBytes().length + MESSAGE_CODE_BYTE_LENGTH + ERROR_REASON_BYTE_LENGTH + reason.length();

            packet.getBuf().writeInt(totalLength); // Total Length
            packet.getBuf().writeBytes(uuid.getBytes()); // UUID
            packet.getBuf().writeByte(messageType.getCode());
            packet.getBuf().writeInt(reason.length());
            packet.getBuf().writeBytes(reason.getBytes());
        } else if (msg instanceof AdminCommandMessage) {
            AdminCommandMessage adminCommandMessage = (AdminCommandMessage) msg;

            AdminCommandType adminCommandType = adminCommandMessage.getAdminCommandType();

            int totalLength = uuid.getBytes().length + MESSAGE_CODE_BYTE_LENGTH + Constants.ADMIN_COMMAND_BYTE_LENGTH + adminCommandType.getCode().length();

            packet.getBuf().writeInt(totalLength); // Total Length
            packet.getBuf().writeBytes(uuid.getBytes()); // UUID
            packet.getBuf().writeByte(messageType.getCode());
            packet.getBuf().writeInt(adminCommandType.getCode().length());
            packet.getBuf().writeBytes(adminCommandType.getCode().getBytes());
        }

        out.add(packet);
    }
}
