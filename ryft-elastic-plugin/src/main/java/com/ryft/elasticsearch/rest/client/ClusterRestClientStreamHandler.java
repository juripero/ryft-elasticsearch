package com.ryft.elasticsearch.rest.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryft.elasticsearch.rest.mappings.RyftRequestPayload;
import com.ryft.elasticsearch.rest.mappings.RyftResponse;
import com.ryft.elasticsearch.rest.mappings.StreamReadResult;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AttributeKey;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.InputStreamReader;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ClusterRestClientStreamHandler extends SimpleChannelInboundHandler<Object> {

    private static final ESLogger LOGGER = Loggers.getLogger(ClusterRestClientStreamHandler.class);

    private static final String RYFT_RESPONSE = "RYFT_RESPONSE";
    private static final String RYFT_STREAM_RESPONSE = "RYFT_STREAM_RESPONSE";
    private static final String RYFT_PAYLOAD = "RYFT_PAYLOAD";
    public static final AttributeKey<RyftResponse> RYFT_RESPONSE_ATTR = AttributeKey.valueOf(RYFT_RESPONSE);
    public static final AttributeKey<StreamReadResult> RYFT_STREAM_RESPONSE_ATTR = AttributeKey.valueOf(RYFT_STREAM_RESPONSE);

    public static final AttributeKey<RyftRequestPayload> RYFT_PAYLOAD_ATTR = AttributeKey.valueOf(RYFT_PAYLOAD);

    private final CountDownLatch countDownLatch;
    private final Integer size;
    private final ByteBuf accumulator = Unpooled.buffer();
    private final DataInputStream inputStream = new DataInputStream(new ByteBufInputStream(accumulator));
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    private StreamReadResult result = new StreamReadResult();
    private final ObjectMapper mapper;
    RyftStreamReadingProcess readingProcess;
    Future<StreamReadResult> future;

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
            readingProcess = new RyftStreamReadingProcess(ctx, size, new RyftStreamDecoder(accumulator, mapper));
            future = Executors.newSingleThreadExecutor().submit(readingProcess);
        } else if (msg instanceof HttpContent) {
            LOGGER.debug("Content received {}", msg);
            HttpContent m = (HttpContent) msg;
            accumulator.writeBytes(m.content());
        } else if (msg instanceof LastHttpContent) {
            LOGGER.debug("Last http content {}", msg);
            HttpContent m = (HttpContent) msg;
            accumulator.writeBytes(m.content());
            getResult(ctx);
        }
    }

//    private void parseStream(ChannelHandlerContext ctx) throws Exception {
////        String jsonLine = reader.readLine();
////        accumulator.discardReadBytes();
////        while (jsonLine != null) {
////            switch (mapper.readValue(jsonLine, String.class)) {
////                case "rec":
////                    RyftResult r = mapper.readValue(reader.readLine(), RyftResult.class);
////                    result.addHit(r.getInternalSearchHit());
////                    break;
////                case "stat":
////                    RyftStats stats = mapper.readValue(reader.readLine(), RyftStats.class);
////                    break;
////            }
////            jsonLine = reader.readLine();
////        }
//    }

//    private Boolean processStreamObject(BufferedReader reader) throws IOException {
//        switch (getJsonString(reader.readLine())) {
//            case "rec":
//                rawData.add(getJsonObject(reader.readLine()));
//                receivedResults++;
//                break;
//            case "stat":
//                stats = getJsonObject(reader.readLine());
//                break;
//            case "end":
//                return false;
//            case "err":
//                String error = getJsonString(reader.readLine())
//                        .replaceAll("\\\\n", "\n").trim();
//                LOG.error(error);
//                errors.add(error);
//            default:
//        }
//        return true;
//    }
//    private void parseFullReply(ChannelHandlerContext ctx) throws Exception {
//        // Ugly construction because of SecurityManager used by ES
//        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
//            RyftResponse ryftResponse;
//            try {
//                ObjectMapper mapper = new ObjectMapper();
//                ryftResponse = mapper.readValue(accumulator.toString(StandardCharsets.UTF_8), RyftResponse.class);
//            } catch (IOException | RuntimeException ex) {
//                LOGGER.error("Failed to parse RYFT response", ex);
//                ryftResponse = new RyftResponse();
//                ryftResponse.setMessage(ex.getMessage());
//            }
//            NettyUtils.setAttribute(RYFT_RESPONSE_ATTR, ryftResponse, ctx);
//            return null;
//        });
//        countDownLatch.countDown();
//        accumulator.clear();
//        ctx.channel().close();
//    }

    private void getResult(ChannelHandlerContext ctx) throws InterruptedException, ExecutionException {
        NettyUtils.setAttribute(RYFT_STREAM_RESPONSE_ATTR, future.get(), ctx);
        countDownLatch.countDown();
        accumulator.clear();
        ctx.channel().close();
    }
}
