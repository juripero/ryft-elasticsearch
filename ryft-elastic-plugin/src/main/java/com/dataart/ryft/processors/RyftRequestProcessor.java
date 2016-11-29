package com.dataart.ryft.processors;

import java.util.Arrays;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.internal.InternalSearchResponse;

import com.dataart.ryft.disruptor.messages.InternalEvent;
import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.elastic.plugin.PropertiesProvider;
import com.dataart.ryft.elastic.plugin.rest.client.NettyUtils;
import com.dataart.ryft.elastic.plugin.rest.client.RestClientHandler;
import com.dataart.ryft.elastic.plugin.rest.client.RyftRestClient;

@Singleton
public class RyftRequestProcessor extends RyftProcessor {
    private final ESLogger logger = Loggers.getLogger(getClass());
    RyftRestClient channelProvider;
    PropertiesProvider props;

    @Inject
    public RyftRequestProcessor(PropertiesProvider properties, RyftRestClient channelProvider) {
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
            NettyUtils.setAttribute(RestClientHandler.START_TIME_ATTR, System.currentTimeMillis(), ryftChannel);
            NettyUtils.setAttribute(RestClientHandler.REQUEST_EVENT_ATTR, requestEvent, ryftChannel);
            NettyUtils.setAttribute(RestClientHandler.ACCUMULATOR_ATTR, Unpooled.buffer(), ryftChannel);
            ryftChannel.pipeline().addLast("client", new RestClientHandler());
            String searchUri = requestEvent.getRyftSearchUrl();
            logger.info("Ryft rest query has been generated: \n{}", searchUri);
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, searchUri);
            request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + props.get().getStr(PropertiesProvider.RYFT_REST_AUTH));
            ryftChannel.writeAndFlush(request)
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
        return props.get().getInt(PropertiesProvider.REQ_THREAD_NUM);
    }

    @Override
    public String getName() {
        return "ryft-request-pool-%d";
    }

}
