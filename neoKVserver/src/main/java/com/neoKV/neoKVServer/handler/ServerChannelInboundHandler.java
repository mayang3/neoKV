package com.neoKV.neoKVServer.handler;

import com.neoKV.neoKVServer.storage.DataReader;
import com.neoKV.neoKVServer.storage.Memtable;
import com.neoKV.neoKVServer.storage.SSTableGroup;
import com.neoKV.network.AdminCommandType;
import com.neoKV.network.DataType;
import com.neoKV.network.MessageType;
import com.neoKV.network.common.Constants;
import com.neoKV.network.payload.*;
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

        // memtable 에는 value 에 dataType 1byte, 나머지는 value 로 쓰인다.
        if (msg instanceof PutMessage) {
            PutMessage putMessage = (PutMessage) msg;
            memtable.put(putMessage.getKey(), putMessage.getDataType(), putMessage.getValue());
            ctx.writeAndFlush(ResponseSuccessMessage.of(messageType, putMessage.getDataType(), putMessage.getKey(), new byte[]{1}));
        } else if (msg instanceof GetMessage) {
            GetMessage getMessage = (GetMessage) msg;
            ByteBuffer buf = dataReader.get(getMessage.getKey());

            if (buf == null) {
                ctx.writeAndFlush(new ResponseFailMessage("not found value"));
                return;
            }

            DataType dataType = DataType.of(buf.get());

            int dataBytesLen = buf.capacity() - Constants.TOMBSTONE_BYTE_LENGTH - Constants.DATATYPE_BYTE_LENGTH - Constants.TIMESTAMP_BYTE_LENGTH;
            byte [] value = new byte[dataBytesLen];
            buf.get(value, 0, dataBytesLen);
            ctx.writeAndFlush(ResponseSuccessMessage.of(messageType, dataType, msg.getKey(), value));
        } else if (msg instanceof DeleteMessage) {
            DeleteMessage deleteMessage = (DeleteMessage) msg;
            ctx.writeAndFlush(ResponseSuccessMessage.of(messageType, deleteMessage.getDataType(), msg.getKey(), new byte[]{1}));
        } else if (msg instanceof AdminCommandMessage) {
            AdminCommandMessage adminCommandMessage = (AdminCommandMessage) msg;

            log.warn("[ServerChannelInboundHandler] executes a forced command!! {}", adminCommandMessage.getAdminCommandType());

            if (AdminCommandType.FLUSH == adminCommandMessage.getAdminCommandType()) {
                ssTableGroup.saveToSSTable();
            }
        }
    }
}
