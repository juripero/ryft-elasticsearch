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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.ryft.elasticsearch.rest.mappings.RyftResult;
import com.ryft.elasticsearch.rest.mappings.RyftStats;
import com.ryft.elasticsearch.rest.mappings.RyftStreamResponse;
import java.util.concurrent.Callable;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class RyftStreamReadingProcess implements Callable<RyftStreamResponse> {

    private static final ESLogger LOGGER = Loggers.getLogger(RyftStreamReadingProcess.class);

    private Integer count = 0;
    private final Integer size;
    private final RyftStreamDecoder ryftStreamDecoder;
    private Boolean end = false;

    public RyftStreamReadingProcess(
            Integer size, RyftStreamDecoder ryftStreamDecoder) {
        this.size = size;
        this.ryftStreamDecoder = ryftStreamDecoder;
    }

    @Override
    public RyftStreamResponse call() throws Exception {
        RyftStreamResponse result = new RyftStreamResponse();
        LOGGER.info("Start response stream reading");
        try {
            do {
                try {
                    String controlMessage = ryftStreamDecoder.decode(String.class);
                    switch (controlMessage) {
                        case "rec":
                            if (count < size) {
                                RyftResult ryftResult = ryftStreamDecoder.decode(RyftResult.class);
                                LOGGER.trace("ryftResult: {}", ryftResult);
                                result.addHit(ryftResult.getInternalSearchHit());
                                count++;
                            } else {
                                ryftStreamDecoder.skipLine();
                                LOGGER.trace("skip");
                            }
                            break;
                        case "stat":
                            RyftStats stats = ryftStreamDecoder.decode(RyftStats.class);
                            LOGGER.info("Stats: {}", stats);
                            result.setStats(stats);
                            break;
                        case "err":
                            String error = ryftStreamDecoder.decode(String.class)
                                    .replaceAll("\\\\n", "\n").trim();
                            LOGGER.error("Error: {}", error);
                            result.addFailure(new ShardSearchFailure(new Exception(error)));
                            break;
                        case "end":
                            end = true;
                            LOGGER.debug("End response stream reading");
                            break;
                        default:
                            LOGGER.error("Unknown control message: {}", controlMessage);
                    }
                } catch (JsonProcessingException ex) {
                    LOGGER.error("Json parsing error", ex);
                }
            } while (!end);
        } catch (Exception ex) {
            LOGGER.error("Ryft response stream reading exception", ex);
        }
        return result;
    }

}
