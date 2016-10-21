package com.dataart.ryft.elastic.plugin.interceptors;

import java.util.Optional;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.tasks.Task;

import com.dataart.ryft.disruptor.EventProducer;
import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.elastic.parser.FuzzyQueryParser;

public class SearchInterceptor implements ActionInterceptor {
    private final ESLogger logger = Loggers.getLogger(getClass());
    EventProducer<RyftRequestEvent> producer;

    @Inject
    public SearchInterceptor(EventProducer<RyftRequestEvent> producer) {
        this.producer = producer;
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public boolean intercept(Task task, String action, ActionRequest request, ActionListener listener,
            ActionFilterChain chain) {
        boolean proceed = false;
        // TODO: [imasternoy] ugly, sorry.
        SearchRequest searchReq = ((SearchRequest) request);
        BytesReference searchContent = searchReq.source().copyBytesArray();
        try {
            Optional<RyftRequestEvent> ryftFuzzy = FuzzyQueryParser.parseQuery(searchContent);
            if (ryftFuzzy.isPresent()) {
                RyftRequestEvent rf = ryftFuzzy.get();
                rf.setIndex(searchReq.indices());
                rf.setType(searchReq.types());
                rf.setCallback(listener);
                logger.info("Ryft request has been generated {}", ryftFuzzy);
                producer.send(rf);
            }else{
                proceed = true;
            }
        } catch (Exception e) {
            logger.error("Failed to filter search action", e);
            proceed = true;
        }
        return proceed;
    }

}
