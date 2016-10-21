package com.dataart.ryft.processors;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.dataart.ryft.disruptor.messages.InternalEvent;
import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.elastic.plugin.PropertiesProvider;
import com.dataart.ryft.elastic.plugin.RyftProperties;
import com.dataart.ryft.elastic.plugin.rest.client.RestClientHandler;
import com.dataart.ryft.elastic.plugin.rest.client.RyftRestClient;

@Singleton
public class RyftRequestProcessor extends RyftProcessor {
    private final ESLogger logger = Loggers.getLogger(getClass());
    RyftRestClient channelProvider;
    RyftProperties props;

    @Inject
    public RyftRequestProcessor(RyftProperties properties, RyftRestClient channelProvider) {
        this.props = properties;
        this.channelProvider = channelProvider;
    }

    @Override
    public void process(InternalEvent event) {
        RyftRequestEvent requestEvent = (RyftRequestEvent) event;
        executor.submit(() -> sendToRyft(requestEvent));
    }

    protected void sendToRyft(RyftRequestEvent requestEvent) {
        Channel ryftChannel = channelProvider.get();
        ryftChannel.pipeline().addLast("client", new RestClientHandler(requestEvent));
        String searchUri = props.getStr(PropertiesProvider.RYFT_SEARCH_URL) + requestEvent.getRyftSearchUrl();
        ryftChannel.writeAndFlush(new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, searchUri))
                .addListener(new ChannelFutureListener() {
                    public void operationComplete(ChannelFuture future) throws Exception {
                        logger.debug("Operation complete");
                    }
                });

    }

    @Override
    public int getPoolSize() {
        return props.getInt(PropertiesProvider.REQ_THREAD_NUM);
    }

    @Override
    public String getName() {
        return "ryft-request-pool-%d";
    }

}
