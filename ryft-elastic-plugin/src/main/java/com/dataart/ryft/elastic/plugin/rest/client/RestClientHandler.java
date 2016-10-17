package com.dataart.ryft.elastic.plugin.rest.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.action.ActionListener;
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

import com.dataart.ryft.elastic.plugin.mappings.RyftResponse;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class RestClientHandler extends SimpleChannelInboundHandler<Object> {
    private static final ESLogger logger = Loggers.getLogger(RestClientHandler.class);

    ActionListener<SearchResponse> listener;
    // TODO: [imasternoy] should be deleted and changed to special decoder
    JsonParser parser;
    ByteBuf accumulator;

    public RestClientHandler(ActionListener<SearchResponse> listener) {
        this.listener = listener;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpResponse) {
            accumulator = Unpooled.buffer(); // BEWARE TO RELEASE
            logger.info("Message received {}", msg);
        } else if (msg instanceof HttpContent) {
            HttpContent m = (HttpContent) msg;
            byte[] bytes = new byte[m.content().readableBytes()];
            ((io.netty.buffer.ByteBuf) m.content()).copy().readBytes(bytes);
            logger.info("Message received {}", new String(bytes));
            accumulator.writeBytes(m.content());
        } else if (msg instanceof LastHttpContent) {
            logger.info("Received lastHttpContent {}", msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        SearchResponse response = new SearchResponse(InternalSearchResponse.empty(), null, 1, 1, 0,
                (ShardSearchFailure[]) Lists.newArrayList(new ShardSearchFailure(cause)).toArray());
        listener.onResponse(response);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
            RyftResponse results = mapper.readValue(accumulator.array(), RyftResponse.class);
            logger.info("Response has been parsed channel will be closed. Response: {}", results);
            // TODO: [imasternoy] we should not block I/O thread. Investigate to
            // move processing somewhere
            List<InternalSearchHit> searchHits = new ArrayList<InternalSearchHit>();
            results.getResults().forEach(
                    hit -> {
                        InternalSearchHit searchHit = new InternalSearchHit(searchHits.size(), hit.getUid(), new Text(
                                hit.getType()), ImmutableMap.of());
                        searchHit.shard(new SearchShardTarget(results.getStats().getHost(), "shakespeare", 0));
                        searchHit.sourceRef(((BytesReference) new BytesArray(hit.getDoc().toString())));
                        searchHits.add(searchHit);
                    });
            InternalSearchHits hits = new InternalSearchHits(
                    searchHits.toArray(new InternalSearchHit[searchHits.size()]), searchHits.size(), 1.0f);

            InternalSearchResponse searchResponse = new InternalSearchResponse(hits, InternalAggregations.EMPTY, null,
                    null, false, false);

            SearchResponse response = new SearchResponse(searchResponse, null, 1, 1, results.getStats().getDuration(),
                    ShardSearchFailure.EMPTY_ARRAY);
            // TODO [imasternoy] Should be changed to use message bus
            listener.onResponse(response);
        } catch (IOException e) {
            logger.error("Failed to parse RYFT response", e);
            listener.onFailure(e);
        } finally {
            if (accumulator != null) {
                accumulator.release();
            }
            super.channelUnregistered(ctx);
        }
    }
}
