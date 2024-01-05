package com.neoKV.neoKVclient;

import com.neoKV.network.decoder.ByteToPacketDecoder;
import com.neoKV.network.decoder.PacketToMessageDecoder;
import com.neoKV.network.encoder.MessageToPacketEncoder;
import com.neoKV.network.encoder.PacketToByteEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author neo82
 */
public class NeoKVClient {
    private static final Logger log = LoggerFactory.getLogger(NeoKVClient.class);

    private Channel channel;
    public void start() {
        String host = "127.0.0.1";
        int port = 8007;
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workerGroup);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                    socketChannel.pipeline().addLast(new ByteToPacketDecoder());
                    socketChannel.pipeline().addLast(new PacketToByteEncoder());

                    socketChannel.pipeline().addLast(new PacketToMessageDecoder());
                    socketChannel.pipeline().addLast(new MessageToPacketEncoder());

//                    socketChannel.pipeline().addLast(new ClientChannelInBoundHandler());

                }
            });
            ChannelFuture channelFuture = bootstrap.connect(host, port).sync();
            channelFuture.addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    channel = future.channel();
                    log.info("****************************************");
                    log.info("**                                    **");
                    log.info("**             [NeoKV]                **");
                    log.info("**      Connection successful         **");
                    log.info("**                                    **");
                    log.info("****************************************");
                } else {
                    log.error("Failed to connect to server", future.cause());
                    workerGroup.shutdownGracefully();
                }
            });
        } catch (Exception e) {
            log.error("[NeoKVClient] error!", e);
        }
    }

    public void sendData(Object data) {
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(data);
        } else {
            log.error("Channel is not active. Cannot send data.");
        }
    }
}
