package com.ryft.elasticsearch.rest.client;

import com.ryft.elasticsearch.rest.mappings.RyftResult;
import com.ryft.elasticsearch.rest.mappings.RyftStats;
import com.ryft.elasticsearch.rest.mappings.StreamReadResult;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.Callable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class RyftStreamReadingProcess implements Callable<StreamReadResult> {

    private static final ESLogger LOGGER = Loggers.getLogger(RyftStreamReadingProcess.class);

    private Integer count = 0;
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

    @Override
    public StreamReadResult call() throws Exception {
        StreamReadResult result = new StreamReadResult();
        try {
            do {
                String jsonLine = ryftStreamDecoder.decode(ctx, String.class);
                switch (jsonLine) {
                    case "rec":
                        if (count < size) {
                            RyftResult ryftResult = ryftStreamDecoder.decode(ctx, RyftResult.class);
                            LOGGER.trace("ryftResult: {}", ryftResult);
                            result.addHit(ryftResult.getInternalSearchHit());
                            count++;
                        } else {
                            ryftStreamDecoder.decode(ctx);
                            LOGGER.trace("skip");
                        }
                        break;
                    case "stat":
                        RyftStats stats = ryftStreamDecoder.decode(ctx, RyftStats.class);
                        LOGGER.info("Stats: {}", stats);
                        result.setStats(stats);
                        break;
                    case "end":
                        end = true;
                        break;
                }
            } while (!end);
        } catch (Exception ex) {
            LOGGER.error("Ryft response stream reading exception", ex);
        }
        return result;
    }

}
