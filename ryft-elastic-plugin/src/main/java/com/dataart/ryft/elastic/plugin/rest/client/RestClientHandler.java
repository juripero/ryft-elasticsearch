package com.dataart.ryft.elastic.plugin.rest.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AttributeKey;


import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;


import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.elastic.plugin.mappings.RyftResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class RestClientHandler extends SimpleChannelInboundHandler<Object> {
    private static final ESLogger logger = Loggers.getLogger(RestClientHandler.class);

    private static final String START_TIME = "START_TIME";
    private static final String ACCUMULATOR = "ACCUMULATOR";
    private static final String REQUEST_EVENT = "REQUEST_EVENT";
    public static final AttributeKey<RyftRequestEvent> REQUEST_EVENT_ATTR = AttributeKey.valueOf(REQUEST_EVENT);
    public static final AttributeKey<Long> START_TIME_ATTR = AttributeKey.valueOf(START_TIME);
    public static final AttributeKey<ByteBuf> ACCUMULATOR_ATTR = AttributeKey.valueOf(ACCUMULATOR);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpResponse) {
            logger.trace("Message received {}", msg);
        } else if (msg instanceof HttpContent) {
            HttpContent m = (HttpContent) msg;
            NettyUtils.getAttribute(ctx, ACCUMULATOR_ATTR).writeBytes(m.content());
        } else if (msg instanceof LastHttpContent) {
            logger.trace("Received lastHttpContent {}", msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        try {
            super.exceptionCaught(ctx, cause);
            SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), null, 1, 1, 0,
                    (ShardSearchFailure[]) Lists.newArrayList(new ShardSearchFailure(cause)).toArray());
            NettyUtils.getAttribute(ctx, REQUEST_EVENT_ATTR).getCallback().onResponse(response);
        } finally {
            if (NettyUtils.getAttribute(ctx, ACCUMULATOR_ATTR) != null) {
                NettyUtils.getAttribute(ctx, ACCUMULATOR_ATTR).release();
            }
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        try {
            // Ugly construction because of SecurityManager used by ES
            RyftResponse results = (RyftResponse) AccessController.doPrivileged(//
                    (PrivilegedAction) () -> {
                        ByteBuf accumulator = NettyUtils.getAttribute(ctx, ACCUMULATOR_ATTR);
                        RyftResponse res = null;
                        try {
                            ObjectMapper mapper = new ObjectMapper();
                            if (accumulator.isReadable()) {
                                res = mapper.readValue(accumulator.array(), RyftResponse.class);
                            }
                        } catch (Exception e) {
                            logger.error("Failed to parse RYFT response", e);
                            NettyUtils.getAttribute(ctx, REQUEST_EVENT_ATTR).getCallback().onFailure(e);
                        }
                        return res;
                    });

            if (results == null) {
                NettyUtils.getAttribute(ctx, REQUEST_EVENT_ATTR).getCallback()
                        .onFailure(new RyftRestExeption("EMPTY response"));
                return;
            }

            if (results.getErrors() != null) {
                String[] fails = results.getErrors();
                List<ShardSearchFailure> failures = new ArrayList<ShardSearchFailure>();

                for (String failure : fails) {
                    failures.add(new ShardSearchFailure(new RyftRestExeption(failure), null));
                }
                SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), null, 1, 0, 0,
                        failures.toArray(new ShardSearchFailure[fails.length]));
                NettyUtils.getAttribute(ctx, REQUEST_EVENT_ATTR).getCallback().onResponse(response);
                return;
            }
            logger.trace("Response has been parsed channel will be closed. Response: {}", results);
            List<InternalSearchHit> searchHits = new ArrayList<InternalSearchHit>();
            results.getResults().forEach(
                    hit -> {
                        InternalSearchHit searchHit = new InternalSearchHit(searchHits.size(), hit.getUid(), new Text(
                                hit.getType()), ImmutableMap.of());
                        // TODO: [imasternoy] change index name
                        searchHit.shard(new SearchShardTarget(results.getStats().getHost(),Arrays.toString(NettyUtils
                                .getAttribute(ctx, REQUEST_EVENT_ATTR).getIndex()), 0));

                        if (hit.getDoc() == null && hit.getError() != null) {
                            searchHit.sourceRef(((BytesReference) new BytesArray("{\"error\": \""
                                    + hit.getError().toString() + "\"}")));
                        } else {
                            searchHit.sourceRef(((BytesReference) new BytesArray(hit.getDoc().toString())));
                        }
                        searchHits.add(searchHit);
                    });
            InternalSearchHits hits = new InternalSearchHits(
                    searchHits.toArray(new InternalSearchHit[searchHits.size()]), searchHits.size(), 1.0f);

            InternalSearchResponse searchResponse = new InternalSearchResponse(hits, InternalAggregations.EMPTY, null,
                    null, false, false);

            long diff = System.currentTimeMillis() - NettyUtils.getAttribute(ctx, START_TIME_ATTR);
            SearchResponse response = new SearchResponse(searchResponse, null, 1, 1, diff,
                    ShardSearchFailure.EMPTY_ARRAY);
            // TODO: [imasternoy] Should be changed to use message bus
            // TODO: [imasternoy] Check should we use thread pool or leave it as
            NettyUtils.getAttribute(ctx, REQUEST_EVENT_ATTR).getCallback().onResponse(response);

        } finally {
            if (NettyUtils.getAttribute(ctx, ACCUMULATOR_ATTR) != null) {
                NettyUtils.getAttribute(ctx, ACCUMULATOR_ATTR).release();
            }
            super.channelUnregistered(ctx);
        }
    }
}
