package com.dataart.ryft.elastic.plugin.interceptors;

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
import com.dataart.ryft.elastic.converter.ElasticConversionCriticalException;
import com.dataart.ryft.elastic.converter.ElasticConverter;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.utils.Try;

public class SearchInterceptor implements ActionInterceptor {

    private final ESLogger LOGGER = Loggers.getLogger(SearchInterceptor.class);
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
        Try<RyftQuery> tryRyftQuery = elasticConverter.convert(request);
        Boolean isIntercepted = !tryRyftQuery.hasError();
        if (isIntercepted) {
            RyftQuery ryftQuery = tryRyftQuery.getResult();
            LOGGER.info("Constructed {}", ryftQuery);
            RyftRequestEvent ryftRequest = new RyftRequestEvent(ryftQuery);
            ryftRequest.setIndex(((SearchRequest) request).indices());
            ryftRequest.setCallback(listener);
            producer.send(ryftRequest);
        } else {
            try {
                tryRyftQuery.throwException();
            } catch (Exception ex) {
                LOGGER.error("Converion exception.", ex);
                return ex instanceof ElasticConversionCriticalException;
            }
        }
        return isIntercepted;
    }

}
