package com.ryft.elasticsearch.rest.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LineBasedFrameDecoder;
import java.nio.charset.StandardCharsets;

public class RyftStreamDecoder extends LineBasedFrameDecoder {

    private final ByteBuf buffer;
    private final ObjectMapper mapper;
    private final static Integer MAX_FRAME_SIZE = 1024 * 1024;//1Mb frame

    public RyftStreamDecoder(ByteBuf buffer, ObjectMapper mapper) {
        super(MAX_FRAME_SIZE); 
        this.buffer = buffer;
        this.mapper = mapper;
    }

    public <T> T decode(ChannelHandlerContext ctx, Class<T> clazz) throws Exception {
        ByteBuf byteBuf = decode(ctx);
        if (byteBuf != null) {
            String line = byteBuf.toString(StandardCharsets.UTF_8);
            return mapper.readValue(line, clazz);
        } else {
            return null;
        }
    }

    public ByteBuf decode(ChannelHandlerContext ctx) throws Exception {
        ByteBuf result;
        do {
            result = (ByteBuf) this.decode(ctx, buffer);
        } while (result == null);
        return result;
    }
}
