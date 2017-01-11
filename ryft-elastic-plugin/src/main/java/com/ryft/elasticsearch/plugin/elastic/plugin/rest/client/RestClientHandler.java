package com.ryft.elasticsearch.plugin.elastic.plugin.rest.client;

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
import java.util.UUID;

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

import com.ryft.elasticsearch.plugin.disruptor.messages.RyftRequestEvent;
import com.ryft.elasticsearch.plugin.elastic.plugin.mappings.RyftResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
            } else if (results.getResults() == null) {
                NettyUtils.getAttribute(ctx, REQUEST_EVENT_ATTR).getCallback()
                        .onFailure(new RyftRestExeption("Server error, could not complete search"));
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
                        ObjectNode hitObj = (ObjectNode) hit;
                        String uid = hitObj.has("_uid") ? hitObj.get("_uid").asText() : UUID.randomUUID().toString();
                        String type = hitObj.has("type") ? hitObj.get("type").asText() : "";

                        InternalSearchHit searchHit = new InternalSearchHit(searchHits.size(), uid, new Text(type),
                                ImmutableMap.of());
                        // TODO: [imasternoy] change index name
                        String[] indexes = new String[]{"tempIndex"};
                        searchHit.shard(new SearchShardTarget(results.getStats().getHost(), Arrays.toString(NettyUtils
                                .getAttribute(ctx, REQUEST_EVENT_ATTR).getIndex() == null ? indexes : NettyUtils
                                        .getAttribute(ctx, REQUEST_EVENT_ATTR).getIndex()), 0));

                        String error = hitObj.has("error") ? hitObj.get("error").asText() : "";
                        if (!error.isEmpty()) {
                            searchHit.sourceRef(((BytesReference) new BytesArray("{\"error\": \"" + error + "\"}")));
                        } else {
                            hitObj.remove("_index");
                            searchHit.sourceRef(((BytesReference) new BytesArray(hitObj.toString())));
                        }
                        searchHits.add(searchHit);
                    });
            
            long totalHits = 0l;
            if(results.getStats() != null){
                if(results.getStats().getMatches() != null){
                    totalHits = results.getStats().getMatches();
                }
            }
            
            InternalSearchHits hits = new InternalSearchHits(
                    searchHits.toArray(new InternalSearchHit[searchHits.size()]), totalHits == 0 ? searchHits.size() : totalHits, 1.0f);

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
