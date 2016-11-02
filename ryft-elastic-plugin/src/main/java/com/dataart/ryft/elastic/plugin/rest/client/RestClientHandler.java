package com.dataart.ryft.elastic.plugin.rest.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;

import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Permission;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.bootstrap.Elasticsearch;
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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class RestClientHandler extends SimpleChannelInboundHandler<Object> {
    private static final ESLogger logger = Loggers.getLogger(RestClientHandler.class);

    RyftRequestEvent event;
    ByteBuf accumulator;

    public RestClientHandler(RyftRequestEvent event) {
        this.event = event;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpResponse) {
            accumulator = Unpooled.buffer(); // BEWARE TO RELEASE
            logger.trace("Message received {}", msg);
        } else if (msg instanceof HttpContent) {
            HttpContent m = (HttpContent) msg;
            byte[] bytes = new byte[m.content().readableBytes()];
            ((io.netty.buffer.ByteBuf) m.content()).copy().readBytes(bytes);
            logger.trace("Message received {}", new String(bytes));
            accumulator.writeBytes(m.content());
        } else if (msg instanceof LastHttpContent) {
            logger.trace("Received lastHttpContent {}", msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), null, 1, 1, 0,
                (ShardSearchFailure[]) Lists.newArrayList(new ShardSearchFailure(cause)).toArray());
        event.getCallback().onResponse(response);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        try {
            // Ugly construction because of SecurityManager used by ES
            RyftResponse results = (RyftResponse) AccessController.doPrivileged(//
                    (PrivilegedAction) () -> {
                        RyftResponse res = null;
                        try {
                            ObjectMapper mapper = new ObjectMapper();
                            res = mapper.readValue(accumulator.array(), RyftResponse.class);
                        } catch (Exception e) {
                            logger.error("Failed to parse RYFT response", e);
                            event.getCallback().onFailure(e);
                        }
                        return res;
                    });

            if (results.getErrors() != null) {
                String[] fails = results.getErrors();
                List<ShardSearchFailure> failures = new ArrayList<ShardSearchFailure>();

                for (String failure : fails) {
                    failures.add(new ShardSearchFailure(new RyftRestExeption(failure), null));
                }
                SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), null, 1, 0, 0,
                        failures.toArray(new ShardSearchFailure[fails.length]));
                event.getCallback().onResponse(response);
                return;
            }
            logger.trace("Response has been parsed channel will be closed. Response: {}", results);
            List<InternalSearchHit> searchHits = new ArrayList<InternalSearchHit>();
            results.getResults().forEach(
                    hit -> {
                        InternalSearchHit searchHit = new InternalSearchHit(searchHits.size(), hit.getUid(), new Text(
                                hit.getType()), ImmutableMap.of());
                        // TODO: [imasternoy] change index name
                        searchHit.shard(new SearchShardTarget(results.getStats().getHost(), event.getIndex()[0], 0));
                        
                        if (hit.getDoc() == null && hit.getError() != null) {
                            searchHit.sourceRef(((BytesReference) new BytesArray("{\"error\": \""+hit.getError().toString()+"\"}")));
                        } else {
                            searchHit.sourceRef(((BytesReference) new BytesArray(hit.getDoc().toString())));
                        }
                        searchHits.add(searchHit);
                    });
            InternalSearchHits hits = new InternalSearchHits(
                    searchHits.toArray(new InternalSearchHit[searchHits.size()]), searchHits.size(), 1.0f);

            InternalSearchResponse searchResponse = new InternalSearchResponse(hits, InternalAggregations.EMPTY, null,
                    null, false, false);

            SearchResponse response = new SearchResponse(searchResponse, null, 1, 1, results.getStats().getDuration(),
                    ShardSearchFailure.EMPTY_ARRAY);
            // TODO: [imasternoy] Should be changed to use message bus
            // TODO: [imasternoy] Check should we use thread pool or leave it as
            // is
            event.getCallback().onResponse(response);

        } finally {
            if (accumulator != null) {
                accumulator.release();
            }
            super.channelUnregistered(ctx);
        }
    }
}
