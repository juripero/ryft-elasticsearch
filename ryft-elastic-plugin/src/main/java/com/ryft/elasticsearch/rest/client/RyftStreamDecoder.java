package com.ryft.elasticsearch.rest.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;

public class RyftStreamDecoder {

    private final ByteBuf buffer;
    private final ObjectMapper mapper;

    public RyftStreamDecoder(ByteBuf buffer, ObjectMapper mapper) {
        this.buffer = buffer;
        this.mapper = mapper;
    }

    public <T> T decode(Class<T> clazz) throws Exception {
        Integer lineSize = getLineSize();
        ByteBuf lineByteBuf = buffer.readBytes(lineSize);
        if (lineByteBuf != null) {
            String line = lineByteBuf.toString(StandardCharsets.UTF_8).trim();
            return mapper.readValue(line, clazz);
        } else {
            return null;
        }
    }

    public void skipLine() throws Exception {
        Integer lineSize = getLineSize();
        buffer.skipBytes(lineSize);
        buffer.discardSomeReadBytes();
    }
    
    private Integer getLineSize() {
        Integer result = -1;
        do {
            result = buffer.bytesBefore((byte) '\n');
            if (result == 0) {
                buffer.skipBytes(1);
                result = -1;
            }
        } while (result == -1);
        return result + 1;
    }
}
