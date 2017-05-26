package com.ryft.elasticsearch.rest.client;

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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.ryft.elasticsearch.utils.PostConstruct;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.RyftProperties;
import java.util.Optional;

@Singleton
public class RyftRestClient implements PostConstruct {

    private static final ESLogger LOGGER = Loggers.getLogger(RyftRestClient.class);
    private Bootstrap b;
    RyftProperties props;

    @Inject
    public RyftRestClient(RyftProperties props) {
        this.props = props;
    }

    @Override
    public void onPostConstruct() {
        EventLoopGroup workerGroup = new NioEventLoopGroup(props.getInt(PropertiesProvider.WORKER_THREAD_COUNT));
        b = new Bootstrap();
        b = b.group(workerGroup)//
                .channel(NioSocketChannel.class)//
                .option(ChannelOption.SO_KEEPALIVE, true)//
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 40000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("encoder", new HttpRequestEncoder());
                        ch.pipeline().addLast("decoder", new HttpResponseDecoder());
                    }
                });
    }

    public Optional<Channel> get() {
        String host = props.getStr(PropertiesProvider.HOST);
        return get(host);
    }

    public Optional<Channel> get(String host) {
        Integer port = props.getInt(PropertiesProvider.PORT);
        return get(new InetSocketAddress(host, port));
    }

    private Optional<Channel> get(InetSocketAddress address) {
        try {
            return Optional.of(b.connect(address).sync().channel());
        } catch (Exception ex) {
            LOGGER.error("Can not open channel to {}.", ex, address);
            return Optional.empty();
        }
    }
}
