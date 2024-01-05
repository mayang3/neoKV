package com.neoKV.network.decoder;

import com.neoKV.network.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * @author neo82
 */
public class ByteToPacketDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
        if (buf.readableBytes() < 4) {
            return;
        }

        buf.markReaderIndex();
        final int totalLength = buf.readInt();
        if (buf.readableBytes() < totalLength) {
            buf.resetReaderIndex();
            return;
        }

        out.add(new Packet(buf.readBytes(totalLength)));
    }
}
