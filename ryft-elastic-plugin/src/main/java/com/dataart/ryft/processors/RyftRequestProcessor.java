package com.dataart.ryft.processors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import com.dataart.ryft.disruptor.messages.InternalEvent;
import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.elastic.plugin.rest.client.RyftRestClient;

@Singleton
public class RyftRequestProcessor extends RyftProcessor {
    private final ESLogger logger = Loggers.getLogger(getClass());
    private static final String RYFT_SEARCH_URL = "http://172.16.13.3:8765";

    @Override
    public void process(InternalEvent event) {
        RyftRequestEvent requestEvent = (RyftRequestEvent) event;
        executor.submit(new Runnable() {
            @Override
            public void run() {
                sendToRyft(requestEvent);
            }
        });
    }

    private void sendToRyft(RyftRequestEvent requestEvent) {
        RyftRestClient handler = new RyftRestClient(requestEvent.getCallback());
        handler.init();
        Channel channel = handler.getChannel();
        String searchUri  =RYFT_SEARCH_URL
                + requestEvent.getRyftSearchUrl();
        channel.writeAndFlush(
                new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, searchUri)).addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) throws Exception {
                logger.debug("Operation complete");
            }
        });

    }

    @Override
    public String getName() {
        return "ryft-request-pool-%d";
    }

}
