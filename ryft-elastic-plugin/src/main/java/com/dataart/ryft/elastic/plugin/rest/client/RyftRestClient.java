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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.dataart.ryft.disruptor.PostConstruct;
import com.dataart.ryft.elastic.plugin.PropertiesProvider;
import com.dataart.ryft.elastic.plugin.RyftProperties;

@Singleton
public class RyftRestClient implements PostConstruct {
    private static final ESLogger logger = Loggers.getLogger(RestClientHandler.class);
    private Bootstrap b;
    RyftProperties props;

    @Inject
    public RyftRestClient(RyftProperties props) {
        this.props = props;
    }

    @Override
    public void onPostConstruct() {
        EventLoopGroup workerGroup = new NioEventLoopGroup( props.getInt(PropertiesProvider.WROKER_THREAD_COUNT));
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
            return b.connect(new InetSocketAddress(//
                    props.getStr(PropertiesProvider.HOST),//
                    props.getInt(PropertiesProvider.PORT)))//
                    .sync().channel();
        } catch (InterruptedException e) {
            // Should not happen
            logger.error("Rest client dead", e);
        }
        return null;
    }
}
