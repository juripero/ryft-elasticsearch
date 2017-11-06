package com.ryft.elasticsearch.rest.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LineBasedFrameDecoder;
import java.nio.charset.StandardCharsets;

public class RyftStreamDecoder extends LineBasedFrameDecoder {

    private final ByteBuf buffer;
    private final ObjectMapper mapper;

    public RyftStreamDecoder(ByteBuf buffer, ObjectMapper mapper) {
        super(1024 * 1024);
        this.buffer = buffer;
        this.mapper = mapper;
    }

    public <T> T decode(ChannelHandlerContext ctx, Class<T> clazz) throws Exception {
        ByteBuf slicedByteBuf = (ByteBuf) this.decode(ctx, buffer);
        if (slicedByteBuf != null) {
            String line = slicedByteBuf.toString(StandardCharsets.UTF_8);
            return mapper.readValue(line, clazz);
        } else {
            return null;
        }
    }
}
