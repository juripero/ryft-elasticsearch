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

    private final Integer bufferWarnSize;
    private final CountDownLatch countDownLatch;
    private final Integer size;
    private final ByteBuf accumulator;
    private final ObjectMapper mapper;
    private Future<RyftStreamResponse> future;

    public ClusterRestClientStreamHandler(CountDownLatch countDownLatch, Integer size, ObjectMapper mapper, Integer bufferSize) {
        super();
        this.countDownLatch = countDownLatch;
        this.size = (size == -1) ? Integer.MAX_VALUE : size;
        this.mapper = mapper;

        Double warnSize = bufferSize * 0.05;
        this.bufferWarnSize = warnSize.intValue();
        this.accumulator = Unpooled.buffer(bufferSize, bufferSize);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpResponse) {
            LOGGER.debug("Message received {}", msg);
            RyftStreamReadingProcess readingProcess = new RyftStreamReadingProcess(size, new RyftStreamDecoder(accumulator, mapper));
            future = Executors.newSingleThreadExecutor().submit(readingProcess);
        } else if (msg instanceof HttpContent) {
            LOGGER.debug("Content received {}", msg);
            HttpContent m = (HttpContent) msg;
            Integer bufferDelta = accumulator.maxCapacity() - accumulator.writerIndex();
            if (bufferDelta < bufferWarnSize) {
                LOGGER.warn("Buffer overflow. Buffer capacity: {}. Writer index: {}, Reader index: {}",
                        accumulator.maxCapacity(), accumulator.writerIndex(), accumulator.readerIndex());
                while (bufferDelta < bufferWarnSize) {
                    bufferDelta = accumulator.maxCapacity() - accumulator.writerIndex();
                    accumulator.discardReadBytes();
                    Thread.sleep(1);
                }
            }
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
