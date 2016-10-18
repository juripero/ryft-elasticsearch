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
import com.dataart.ryft.elastic.plugin.rest.client.RestClientHandler;

@Singleton
public class RyftRequestProcessor extends RyftProcessor {
    private final ESLogger logger = Loggers.getLogger(getClass());
    private static final String RYFT_SEARCH_URL = "http://172.16.13.3:8765";

    Provider<Channel> channelProvider;

    @Inject
    public RyftRequestProcessor(Provider<Channel> channelProvider) {
        this.channelProvider = channelProvider;
    }

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
        Channel ryftChannel = channelProvider.get();
        ryftChannel.pipeline().addLast("client", new RestClientHandler(requestEvent.getCallback()));
        String searchUri = RYFT_SEARCH_URL + requestEvent.getRyftSearchUrl();
        ryftChannel.writeAndFlush(new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, searchUri))
                .addListener(new ChannelFutureListener() {
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
