package com.ryft.elasticsearch.plugin.elastic.plugin.rest.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryft.elasticsearch.plugin.elastic.plugin.mappings.RyftResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.AttributeKey;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.CountDownLatch;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

/**
 *
 * @author denis
 */
public class ClusterRestClientHandler extends SimpleChannelInboundHandler<Object> {

    private static final ESLogger LOGGER = Loggers.getLogger(ClusterRestClientHandler.class);

    private static final String RYFT_RESPONSE = "RYFT_RESPONSE";
    private static final String INDEX_SHARD = "INDEX_SHARD";
    public static final AttributeKey<RyftResponse> RYFT_RESPONSE_ATTR = AttributeKey.valueOf(RYFT_RESPONSE);
    public static final AttributeKey<ShardRouting> INDEX_SHARD_ATTR = AttributeKey.valueOf(INDEX_SHARD);

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
        } else if (msg instanceof HttpContent) {
            LOGGER.debug("Content received {}", msg);
            HttpContent m = (HttpContent) msg;
            accumulator.writeBytes(m.content());
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // Ugly construction because of SecurityManager used by ES
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            RyftResponse ryftResponse = null;
            try {
                ObjectMapper mapper = new ObjectMapper();
                if (accumulator.isReadable()) {
                    ryftResponse = mapper.readValue(accumulator.array(), RyftResponse.class);
                }
            } catch (Exception ex) {
                LOGGER.error("Failed to parse RYFT response", ex);
                ryftResponse = new RyftResponse();
                ryftResponse.setErrors(new String[]{new String(accumulator.array()).trim()});
            }
            NettyUtils.setAttribute(RYFT_RESPONSE_ATTR, ryftResponse, ctx);
            countDownLatch.countDown();
            return null;
        });
        countDownLatch.countDown();
        ctx.channel().close();
    }

}
