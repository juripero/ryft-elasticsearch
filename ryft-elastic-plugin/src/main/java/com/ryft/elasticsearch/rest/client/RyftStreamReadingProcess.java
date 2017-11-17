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
