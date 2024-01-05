package com.neoKV.neoKVclient.handler;

import com.neoKV.network.payload.Message;
import com.neoKV.network.payload.ResponseSuccessMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * @author neo82
 */
public class ClientChannelInBoundHandler extends SimpleChannelInboundHandler<Message> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        if (msg instanceof ResponseSuccessMessage) {
            ResponseSuccessMessage responseSuccessMessage = (ResponseSuccessMessage) msg;
        }
    }
}
