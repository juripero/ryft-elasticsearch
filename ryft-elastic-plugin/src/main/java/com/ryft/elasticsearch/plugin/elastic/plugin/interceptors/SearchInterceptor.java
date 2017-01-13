package com.ryft.elasticsearch.plugin.elastic.plugin.interceptors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.tasks.Task;

import com.ryft.elasticsearch.plugin.disruptor.EventProducer;
import com.ryft.elasticsearch.plugin.disruptor.messages.RyftRequestEvent;
import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConversionCriticalException;
import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConversionException;
import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConverter;
import com.ryft.elasticsearch.plugin.elastic.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.elastic.plugin.RyftProperties;

public class SearchInterceptor implements ActionInterceptor {

    private static final ESLogger LOGGER = Loggers.getLogger(SearchInterceptor.class);
    private final EventProducer<RyftRequestEvent> producer;
    private final ElasticConverter elasticConverter;
    private final RyftProperties properties;

    @Inject
    public SearchInterceptor(RyftProperties ryftProperties,
            EventProducer<RyftRequestEvent> producer, ElasticConverter elasticConverter) {
        this.properties = ryftProperties;
        this.producer = producer;
        this.elasticConverter = elasticConverter;
    }

//    @Override
//    public boolean intercept(Task task, String action, ActionRequest request, ActionListener listener,
//            ActionFilterChain chain) {
//        Try<RyftRequestEvent> tryRyftRequest = elasticConverter.convert(request);
//        Boolean isIntercepted = !tryRyftRequest.hasError() && (tryRyftRequest.getResult() != null);
//        Boolean isRyftIntegrationElabled;
//        if (isIntercepted) {
//            RyftRequestEvent requestEvent = tryRyftRequest.getResult();
//            isRyftIntegrationElabled = requestEvent.getRyftProperties().getBool(PropertiesProvider.RYFT_INTEGRATION_ENABLED);
//        } else {
//            isRyftIntegrationElabled = properties.getBool(PropertiesProvider.RYFT_INTEGRATION_ENABLED);
//        }
//        if (isRyftIntegrationElabled) {
//            if (isIntercepted) {
//                RyftRequestEvent requestEvent = tryRyftRequest.getResult();
//                requestEvent.setCallback(listener);
//                producer.send(requestEvent);
//                return true;
//            } else {
//                Exception ex = tryRyftRequest.getError();
//                if (ex != null) {
//                    LOGGER.error("Convertion exception.", ex);
//                    return ex instanceof ElasticConversionCriticalException;
//                } else {
//                    return false;
//                }
//            }
//        }
//        return false;
//    }
    @Override
    public boolean intercept(Task task, String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {
        RyftRequestEvent requestEvent = null;
        Boolean isIntercepted = null;
        try {
            requestEvent = elasticConverter.convert(request);
            isIntercepted = requestEvent != null;
        } catch (ElasticConversionException ex) {
            isIntercepted = null;
            if (ex != null) {
                LOGGER.error("Convertion exception.", ex);
                return ex instanceof ElasticConversionCriticalException;
            } else {
                return false;
            }
        }
        Boolean isRyftIntegrationElabled;
        if (isIntercepted) {
            isRyftIntegrationElabled = requestEvent.getRyftProperties().getBool(PropertiesProvider.RYFT_INTEGRATION_ENABLED);
        } else {
            isRyftIntegrationElabled = properties.getBool(PropertiesProvider.RYFT_INTEGRATION_ENABLED);
        }
        if (isRyftIntegrationElabled && isIntercepted) {
            requestEvent.setCallback(listener);
            producer.send(requestEvent);
            return true;
        }
        return false;
    }

}
