package com.neoKV.neoKVServer.handler;

import com.neoKV.neoKVServer.storage.DataReader;
import com.neoKV.neoKVServer.storage.Memtable;
import com.neoKV.neoKVServer.storage.SSTableGroup;
import com.neoKV.network.AdminCommandType;
import com.neoKV.network.DataType;
import com.neoKV.network.MessageType;
import com.neoKV.network.common.Constants;
import com.neoKV.network.payload.*;
import com.neoKV.network.utils.ByteBufferUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * @author neo82
 */
public class ServerChannelInboundHandler extends SimpleChannelInboundHandler<Message> {

    private static final Logger log = LoggerFactory.getLogger(ServerChannelInboundHandler.class);
    private final Memtable memtable = Memtable.getInstance();
    private final DataReader dataReader = DataReader.getInstance();

    private final SSTableGroup ssTableGroup = SSTableGroup.getInstance();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
        MessageType messageType = msg.getMessageType();

        if (msg instanceof PutMessage putMessage) {
            memtable.put(putMessage.getKey(), putMessage.getDataType(), putMessage.getValue());
            ctx.writeAndFlush(ResponseSuccessMessage.of(messageType, putMessage.getDataType(), putMessage.getKey(), new byte[]{1}));
        } else if (msg instanceof GetMessage getMessage) {
            writeGetMessage(ctx, msg, getMessage, messageType);
        } else if (msg instanceof DeleteMessage deleteMessage) {
            ctx.writeAndFlush(ResponseSuccessMessage.of(messageType, deleteMessage.getDataType(), msg.getKey(), new byte[]{1}));
        } else if (msg instanceof AdminCommandMessage adminCommandMessage) {

            log.warn("[ServerChannelInboundHandler] executes a forced command!! {}", adminCommandMessage.getAdminCommandType());

            if (AdminCommandType.FLUSH == adminCommandMessage.getAdminCommandType()) {
                ssTableGroup.saveToSSTable();
            }
        }
    }

    private void writeGetMessage(ChannelHandlerContext ctx, Message msg, GetMessage getMessage, MessageType messageType) {
        ByteBuffer buffer = null;

        try {
            buffer = dataReader.get(getMessage.getKey());

            if (buffer == null) {
                ctx.writeAndFlush(new ResponseFailMessage("not found value"));
                return;
            }

            DataType dataType = DataType.of(buffer.get());

            int dataBytesLen = buffer.capacity() - Constants.TOMBSTONE_BYTE_LENGTH - Constants.DATATYPE_BYTE_LENGTH - Constants.TIMESTAMP_BYTE_LENGTH;
            byte[] value = new byte[dataBytesLen];
            buffer.get(value, 0, dataBytesLen);
            ctx.writeAndFlush(ResponseSuccessMessage.of(messageType, dataType, msg.getKey(), value));
        } finally {
            ByteBufferUtils.clean(buffer);
        }
    }
}
