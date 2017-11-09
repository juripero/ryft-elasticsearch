package com.ryft.elasticsearch.rest.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryft.elasticsearch.rest.mappings.RyftRequestPayload;
import com.ryft.elasticsearch.rest.mappings.RyftStreamResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AttributeKey;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ClusterRestClientStreamHandler extends SimpleChannelInboundHandler<Object> {

    private static final ESLogger LOGGER = Loggers.getLogger(ClusterRestClientStreamHandler.class);

    private static final String RYFT_STREAM_RESPONSE = "RYFT_STREAM_RESPONSE";
    public static final AttributeKey<RyftStreamResponse> RYFT_STREAM_RESPONSE_ATTR = AttributeKey.valueOf(RYFT_STREAM_RESPONSE);

    private final CountDownLatch countDownLatch;
    private final Integer size;
    private final ByteBuf accumulator = Unpooled.buffer();
    private final ObjectMapper mapper;
    private Future<RyftStreamResponse> future;

    public ClusterRestClientStreamHandler(CountDownLatch countDownLatch, Integer size, ObjectMapper mapper) {
        super();
        this.countDownLatch = countDownLatch;
        this.size = size;
        this.mapper = mapper;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpResponse) {
            LOGGER.debug("Message received {}", msg);
            RyftStreamReadingProcess readingProcess = new RyftStreamReadingProcess(ctx, size, new RyftStreamDecoder(accumulator, mapper));
            future = Executors.newSingleThreadExecutor().submit(readingProcess);
        } else if (msg instanceof HttpContent) {
            LOGGER.debug("Content received {}", msg);
            HttpContent m = (HttpContent) msg;
            accumulator.writeBytes(m.content());
            if (msg instanceof LastHttpContent) {
                RyftStreamResponse result = future.get();
                NettyUtils.setAttribute(RYFT_STREAM_RESPONSE_ATTR, result, ctx);
                countDownLatch.countDown();
                accumulator.clear();
                ctx.channel().close();
            }
        }
    }
}
