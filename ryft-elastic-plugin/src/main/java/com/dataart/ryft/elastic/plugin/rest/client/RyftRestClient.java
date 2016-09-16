package com.dataart.ryft.elastic.plugin.rest.client;

import java.net.InetSocketAddress;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;

public class RyftRestClient {
    private static final ESLogger logger = Loggers.getLogger(RestClientHandler.class);
    private static final String HOST = "172.16.13.3";
    private static final int PORT = 8765;

    private Channel inboundChannel;
    private ActionListener<SearchResponse> listener;

    public RyftRestClient(ActionListener<SearchResponse> listener) {
        this.listener = listener;
    }

    public void init() {
        EventLoopGroup workerGroup = new NioEventLoopGroup(2);
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    // DefaultChannelPipeline pipe = (DefaultChannelPipeline)
                    // ch.pipeline();
                    ch.pipeline().addLast("encoder", new HttpRequestEncoder());
                    ch.pipeline().addLast("decoder", new HttpResponseDecoder());
                    ch.pipeline().addLast("client", new RestClientHandler(listener));
                }
            });
            // Start the client.
            inboundChannel = b.connect(new InetSocketAddress(HOST, PORT)).sync().channel();
        } catch (Exception e) {
            logger.error("Rest client died", e);
        }
    }

    public Channel getChannel() {
        return this.inboundChannel;
    }

}
