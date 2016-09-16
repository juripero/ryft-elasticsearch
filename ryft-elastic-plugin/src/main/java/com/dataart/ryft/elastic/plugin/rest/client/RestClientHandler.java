package com.dataart.ryft.elastic.plugin.rest.client;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.internal.InternalSearchResponse;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;

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
            accumulator = Unpooled.buffer(); //BEWARE TO RELEASE
            logger.error("Message received {}", msg);
        } else if (msg instanceof HttpContent) {
            HttpContent m = (HttpContent) msg;
            byte[] bytes = new byte[m.content().readableBytes()];
            ((io.netty.buffer.ByteBuf) m.content()).copy().readBytes(bytes);
            logger.error("Message received {}", new String(bytes));
            
            accumulator.writeBytes(m.content());
        } else if (msg instanceof LastHttpContent) {
         
        }
        // ctx.close();
    }
    
    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        try {
            JsonParser parser = new JsonFactory().createParser(accumulator.copy().array());
            logger.debug("Response has been parsed channel will be closed");
        } catch (IOException e) {
            logger.error("Failed to parse RYFT response", e);
        }
        listener.onResponse(new SearchResponse(InternalSearchResponse.empty(), null, 0, 0, 0, ShardSearchFailure.EMPTY_ARRAY));
        super.channelUnregistered(ctx);
        accumulator.release();
    }

}
