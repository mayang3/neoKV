import com.neoKV.neoKVServer.config.NeoKVServerConfig;
import com.neoKV.neoKVServer.handler.ServerChannelInboundHandler;
import com.neoKV.neoKVServer.merge_compaction.compaction.LeveledCompactor;
import com.neoKV.neoKVServer.storage.SSTableGroup;
import com.neoKV.network.decoder.ByteToPacketDecoder;
import com.neoKV.network.decoder.PacketToMessageDecoder;
import com.neoKV.network.encoder.MessageToPacketEncoder;
import com.neoKV.network.encoder.PacketToByteEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * @author neo82
 */
public class NeoKVServer {
    private static final Logger log = LoggerFactory.getLogger(NeoKVServer.class);

    private final int port;
    private final ServerBootstrap bootstrap;

    public NeoKVServer() {
        this.port = NeoKVServerConfig.getConfig().getPort();
        this.bootstrap = new ServerBootstrap();
    }

    public static void main(String[] args) {
        new NeoKVServer().start();
    }

    private void start() {
        log.info("********** NeoKVServer Starting... **********");

        SSTableGroup ssTableGroup = SSTableGroup.getInstance();
        ssTableGroup.loadSSTableGroup();

        LeveledCompactor instance = LeveledCompactor.getInstance();

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ChannelFuture f = bootstrap.localAddress(new InetSocketAddress(port))
                                       .group(bossGroup, workerGroup)
                                       .channel(NioServerSocketChannel.class)
                                       .childHandler(new ChannelInitializer<SocketChannel>() {
                                           protected void initChannel(SocketChannel socketChannel) throws Exception {
                                               socketChannel.pipeline().addLast(new PacketToByteEncoder());
                                               socketChannel.pipeline().addLast(new ByteToPacketDecoder());

                                               socketChannel.pipeline().addLast(new MessageToPacketEncoder());
                                               socketChannel.pipeline().addLast(new PacketToMessageDecoder());

                                               socketChannel.pipeline().addLast(new ServerChannelInboundHandler());
                                           }
                                       })
                                       .option(ChannelOption.SO_BACKLOG, 128)
                                       .childOption(ChannelOption.SO_KEEPALIVE, true)
                                       .bind()
                                       .sync();

            f.channel().closeFuture().sync();

        } catch (Exception e) {
            log.error("[NeoKVServer] error!", e);
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }


        log.info("********** NeoKVServer Started **********");
    }
}
