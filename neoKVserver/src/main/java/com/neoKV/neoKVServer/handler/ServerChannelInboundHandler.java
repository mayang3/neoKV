package com.neoKV.neoKVServer.handler;

import com.neoKV.neoKVServer.storage.DataReader;
import com.neoKV.neoKVServer.storage.Memtable;
import com.neoKV.network.MessageType;
import com.neoKV.network.payload.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * @author neo82
 */
public class ServerChannelInboundHandler extends SimpleChannelInboundHandler<Message> {
    private final Memtable memtable = Memtable.getInstance();
    private final DataReader dataReader = DataReader.getInstance();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        MessageType messageType = msg.getMessageType();

        if (msg instanceof PutMessage) {
            PutMessage putMessage = (PutMessage) msg;
            memtable.put(putMessage.getKey(), putMessage.getDataType(), putMessage.getValue());
            ctx.writeAndFlush(ResponseSuccessMessage.of(messageType, putMessage.getDataType(), putMessage.getKey(), new byte[]{1}));
        } else if (msg instanceof GetMessage) {
            GetMessage getMessage = (GetMessage) msg;
            byte[] value = dataReader.get(getMessage.getKey(), getMessage.getDataType()).array();
            ctx.writeAndFlush(ResponseSuccessMessage.of(messageType, getMessage.getDataType(), msg.getKey(), value));
        } else if (msg instanceof DeleteMessage) {
            DeleteMessage deleteMessage = (DeleteMessage) msg;
            ctx.writeAndFlush(ResponseSuccessMessage.of(messageType, deleteMessage.getDataType(), msg.getKey(), new byte[]{1}));
        }
    }
}
