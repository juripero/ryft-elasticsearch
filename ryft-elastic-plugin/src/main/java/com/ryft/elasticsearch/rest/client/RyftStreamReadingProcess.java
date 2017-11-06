package com.ryft.elasticsearch.rest.client;

import com.ryft.elasticsearch.rest.mappings.RyftResult;
import com.ryft.elasticsearch.rest.mappings.RyftStats;
import com.ryft.elasticsearch.rest.mappings.StreamReadResult;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.Callable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.aggregations.InternalAggregations;

public class RyftStreamReadingProcess implements Callable<StreamReadResult> {

    private static final ESLogger LOGGER = Loggers.getLogger(RyftStreamReadingProcess.class);

    private final Integer size;
    private final RyftStreamDecoder ryftStreamDecoder;
    private Boolean end = false;
    private final ChannelHandlerContext ctx;

    public RyftStreamReadingProcess(ChannelHandlerContext ctx,
            Integer size, RyftStreamDecoder ryftStreamDecoder) {
        this.size = size;
        this.ryftStreamDecoder = ryftStreamDecoder;
        this.ctx = ctx;
    }

//    public void setWritingFinished() {
//        this.writingFinished = true;
//    }
    @Override
    public StreamReadResult call() throws Exception {
        StreamReadResult result = new StreamReadResult();
        try {
            String jsonLine = ryftStreamDecoder.decode(ctx, String.class);
            while (!end) {
                if (jsonLine != null) {
                    switch (jsonLine) {
                        case "rec":
                            RyftResult ryftResult = ryftStreamDecoder.decode(ctx, RyftResult.class);
                            if (ryftResult != null) {
                                LOGGER.info("ryftResult: {}", ryftResult);
                                result.addHit(ryftResult.getInternalSearchHit());
                                jsonLine = ryftStreamDecoder.decode(ctx, String.class);
                            }
                            break;
                        case "stat":
                            RyftStats stats = ryftStreamDecoder.decode(ctx, RyftStats.class);
                            if (stats != null) {
                                LOGGER.info("stats: {}", stats);
                                result.setAggregations(InternalAggregations.EMPTY);
                                jsonLine = ryftStreamDecoder.decode(ctx, String.class);
                            }
                            break;
                        case "end":
                            end = true;
                            break;
                    }
                } else {
                    jsonLine = ryftStreamDecoder.decode(ctx, String.class);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Ryft response stream reading exception", ex);
        }
        return result;
    }

}
