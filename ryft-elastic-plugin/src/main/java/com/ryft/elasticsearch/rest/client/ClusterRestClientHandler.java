package com.ryft.elasticsearch.rest.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryft.elasticsearch.rest.mappings.RyftRequestPayload;
import com.ryft.elasticsearch.rest.mappings.RyftResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AttributeKey;
import java.io.IOException;

import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.CountDownLatch;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ClusterRestClientHandler extends SimpleChannelInboundHandler<Object> {

    private static final ESLogger LOGGER = Loggers.getLogger(ClusterRestClientHandler.class);

    private static final String RYFT_RESPONSE = "RYFT_RESPONSE";
    private static final String RYFT_PAYLOAD = "RYFT_PAYLOAD";
    public static final AttributeKey<RyftResponse> RYFT_RESPONSE_ATTR = AttributeKey.valueOf(RYFT_RESPONSE);
    public static final AttributeKey<RyftRequestPayload> RYFT_PAYLOAD_ATTR = AttributeKey.valueOf(RYFT_PAYLOAD);

    private final CountDownLatch countDownLatch;
    private final ByteBuf accumulator = Unpooled.buffer();

    public ClusterRestClientHandler(CountDownLatch countDownLatch) {
        super();
        this.countDownLatch = countDownLatch;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpResponse) {
            LOGGER.debug("Message received {}", msg);
        } else if (msg instanceof LastHttpContent) {
            LOGGER.debug("Last http content {}", msg);
            HttpContent m = (HttpContent) msg;
            accumulator.writeBytes(m.content());
            parseFullReply(ctx);
        } else if (msg instanceof HttpContent) {
            LOGGER.debug("Content received {}", msg);
            HttpContent m = (HttpContent) msg;
            accumulator.writeBytes(m.content());
        }
    }

    private void parseFullReply(ChannelHandlerContext ctx) throws Exception {
        // Ugly construction because of SecurityManager used by ES
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            RyftResponse ryftResponse;
            try {
                ObjectMapper mapper = new ObjectMapper();
                ryftResponse = mapper.readValue(accumulator.toString(StandardCharsets.UTF_8), RyftResponse.class);
            } catch (IOException | RuntimeException ex) {
                LOGGER.error("Failed to parse RYFT response", ex);
                ryftResponse = new RyftResponse();
                ryftResponse.setMessage(ex.getMessage());
            }
            NettyUtils.setAttribute(RYFT_RESPONSE_ATTR, ryftResponse, ctx);
            return null;
        });
        countDownLatch.countDown();
        accumulator.clear();
        ctx.channel().close();
    }
}
