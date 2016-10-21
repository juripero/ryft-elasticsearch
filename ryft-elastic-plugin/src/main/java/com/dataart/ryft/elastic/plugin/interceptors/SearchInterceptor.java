package com.dataart.ryft.elastic.plugin.interceptors;

import java.util.Optional;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.tasks.Task;

import com.dataart.ryft.disruptor.EventProducer;
import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.elastic.converter.ElasticConverter;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;

public class SearchInterceptor implements ActionInterceptor {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final EventProducer<RyftRequestEvent> producer;
    private final ElasticConverter elasticConverter;

    @Inject
    public SearchInterceptor(EventProducer<RyftRequestEvent> producer,
            ElasticConverter elasticConverter) {
        this.producer = producer;
        this.elasticConverter = elasticConverter;
    }

    @Override
    public boolean intercept(Task task, String action, ActionRequest request, ActionListener listener,
            ActionFilterChain chain) {
        boolean proceed = false;
        try {
            Optional<RyftQuery> maybeRyftQuery = elasticConverter.parse(request);
            if (maybeRyftQuery.isPresent()) {
                RyftQuery ryftQuery = maybeRyftQuery.get();
                logger.info("Ryft query {}", ryftQuery.buildRyftString());
                RyftRequestEvent ryftRequest = new RyftRequestEvent(ryftQuery);
                ryftRequest.setIndex(((SearchRequest) request).indices());
                ryftRequest.setCallback(listener);
                producer.send(ryftRequest);
            } else {
                proceed = true;
            }
        } catch (Exception e) {
            logger.error("Failed to filter search action", e);
            proceed = true;
        }
        return proceed;
    }

}
