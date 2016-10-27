package com.dataart.ryft.processors;

import java.util.Arrays;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.internal.InternalSearchResponse;

import com.dataart.ryft.disruptor.messages.InternalEvent;
import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.elastic.plugin.PropertiesProvider;
import com.dataart.ryft.elastic.plugin.RyftProperties;
import com.dataart.ryft.elastic.plugin.rest.client.RestClientHandler;
import com.dataart.ryft.elastic.plugin.rest.client.RyftRestClient;
import com.google.common.collect.Lists;

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
        try {
            Channel ryftChannel = channelProvider.get();
            ryftChannel.pipeline().addLast("client", new RestClientHandler(requestEvent));
            String searchUri = props.getStr(PropertiesProvider.RYFT_SEARCH_URL) + requestEvent.getRyftSearchUrl();
            ryftChannel.writeAndFlush(new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, searchUri))
                    .addListener(new ChannelFutureListener() {
                        public void operationComplete(ChannelFuture future) throws Exception {
                            logger.debug("Operation complete");
                        }
                    });
        } catch (Exception e) {
            logger.error("Failed to connect to ryft server", e);
            SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), null, 1, 1, 0,
                    (ShardSearchFailure[]) Arrays.asList(new ShardSearchFailure(e)).toArray());
            requestEvent.getCallback().onResponse(response);
        }
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
