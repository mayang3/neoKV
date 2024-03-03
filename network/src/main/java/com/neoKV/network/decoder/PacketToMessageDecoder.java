package com.neoKV.network.decoder;

import com.neoKV.network.AdminCommandType;
import com.neoKV.network.DataType;
import com.neoKV.network.MessageType;
import com.neoKV.network.Packet;
import com.neoKV.network.payload.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * @author neo82
 */
public class PacketToMessageDecoder extends MessageToMessageDecoder<Packet> {
    private static final Logger log = LoggerFactory.getLogger(PacketToMessageDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, Packet packet, List<Object> out) throws Exception {
        String transactionId = packet.readTransactionId(); // UUID
        MessageType messageType = MessageType.of(packet.readByte()); // MessageType

        Message message = null;

        if (MessageType.PUT == messageType) { // server side
            DataType dataType = DataType.of(packet.readByte());
            String key = packet.readString(); //
            byte[] value = packet.getAllBytes();
            message = PutMessage.of(dataType, key, value);
        } else if (MessageType.GET == messageType) { // server side
            String key = packet.readString(); //
            message = GetMessage.of(key);
        } else if (MessageType.DELETE == messageType) { // server side
            DataType dataType = DataType.of(packet.readByte());
            String key = packet.readString(); //
            message = DeleteMessage.of(messageType, dataType, key);
        } else if (MessageType.RESPONSE_SUCCESS == messageType) { // client side
            MessageType subMessageType = MessageType.of(packet.readByte());
            String key = packet.readString(); //
            DataType dataType = DataType.of(packet.readByte());
            byte[] value = packet.getAllBytes();
            message = ResponseSuccessMessage.of(subMessageType, dataType, key, value);

            if (subMessageType == MessageType.GET) {
                if (DataType.INTEGER == dataType) {
                    System.out.println("OUTPUT ===> " + ByteBuffer.wrap(value).getInt());
                } else if (DataType.LONG == dataType) {
                    System.out.println("OUTPUT ===> " + ByteBuffer.wrap(value).getLong());
                } else if (DataType.FLOAT == dataType) {
                    System.out.println("OUTPUT ===> " + ByteBuffer.wrap(value).getFloat());
                } else if (DataType.DOUBLE == dataType) {
                    System.out.println("OUTPUT ===> " + ByteBuffer.wrap(value).getDouble());
                } else if (DataType.STRING == dataType) {
                    System.out.println("OUTPUT ===> " + new String(ByteBuffer.wrap(value).array()));
                }
            }
        } else if (MessageType.RESPONSE_ERROR == messageType) {
            String reason = packet.readString();

            System.out.println("******************************");
            System.out.println("[ERROR MESSAGE] : " + reason);
            System.out.println("******************************");

            message = new ResponseFailMessage(reason);
        } else if (MessageType.ADMIN_COMMAND == messageType) {
            message = AdminCommandMessage.of(AdminCommandType.findEnum(packet.readString()));
        }

        out.add(message);
    }
}
