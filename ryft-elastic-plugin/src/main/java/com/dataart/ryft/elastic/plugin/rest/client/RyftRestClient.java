package com.dataart.ryft.elastic.plugin.rest.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;

import java.net.InetSocketAddress;

import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.dataart.ryft.disruptor.PostConstruct;

@Singleton
public class RyftRestClient implements Provider<Channel>, PostConstruct {
    private static final ESLogger logger = Loggers.getLogger(RestClientHandler.class);
    private static final int WROKER_THREAD_COUNT = 2;
    // TODO: [imasternoy] In fact we are running on the same machine and can
    // access it via local address
    private static final String HOST = "172.16.13.3";
    private static final int PORT = 8765;

    private Bootstrap b;

    @Override
    public void onPostConstruct() {
        EventLoopGroup workerGroup = new NioEventLoopGroup(WROKER_THREAD_COUNT);
        b = new Bootstrap();
        b = b.group(workerGroup)//
                .channel(NioSocketChannel.class)//
                .option(ChannelOption.SO_KEEPALIVE, true)//
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        // DefaultChannelPipeline pipe =
                        // (DefaultChannelPipeline)
                        // ch.pipeline();
                        ch.pipeline().addLast("encoder", new HttpRequestEncoder());
                        ch.pipeline().addLast("decoder", new HttpResponseDecoder());
                    }
                });
    }

    public Channel get() {
        try {
            return b.connect(new InetSocketAddress(HOST, PORT)).sync().channel();
        } catch (InterruptedException e) {
            // Should not happen
            logger.error("Rest client dead", e);
        }
        return null;
    }
}
