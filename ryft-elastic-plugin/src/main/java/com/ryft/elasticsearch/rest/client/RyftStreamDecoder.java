/**
 * ##License
 * Ryft-Customized BSD License
 * Copyright (c) 2018, Ryft Systems, Inc.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
 *   This product includes software developed by Ryft Systems, Inc.
 * 4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
 *   to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
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
