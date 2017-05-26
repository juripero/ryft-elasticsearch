package com.ryft.elasticsearch.plugin.elastic.plugin.interceptors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.tasks.Task;

import com.ryft.elasticsearch.plugin.disruptor.EventProducer;
import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConversionCriticalException;
import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConversionException;
import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConverter;
import com.ryft.elasticsearch.plugin.elastic.converter.entities.RyftRequestParameters;
import com.ryft.elasticsearch.plugin.elastic.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.elastic.plugin.RyftProperties;
import com.ryft.elasticsearch.plugin.elastic.plugin.cluster.RyftSearchService;
import com.ryft.elasticsearch.plugin.disruptor.messages.RequestEvent;

public class SearchInterceptor implements ActionInterceptor {

    private static final ESLogger LOGGER = Loggers.getLogger(SearchInterceptor.class);
    private final EventProducer<RequestEvent> producer;
    private final ElasticConverter elasticConverter;
    private final RyftProperties properties;
    private final RyftSearchService ryftSearchService;

    @Inject
    public SearchInterceptor(RyftProperties ryftProperties,
            EventProducer<RequestEvent> producer, ElasticConverter elasticConverter,
            RyftSearchService ryftSearchService) {
        this.properties = ryftProperties;
        this.producer = producer;
        this.elasticConverter = elasticConverter;
        this.ryftSearchService = ryftSearchService;
    }

    @Override
    public boolean intercept(Task task, String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {
        try {
            RyftRequestParameters ryftRequestParameters = elasticConverter.convert(request);
            if (ryftRequestParameters == null) {
                return false;
            } else {
                RequestEvent requestEvent = ryftSearchService.getClusterRequestEvent(ryftRequestParameters);
                Boolean isRyftIntegrationElabled;
                if (requestEvent != null) {
                    isRyftIntegrationElabled = ryftRequestParameters.getRyftProperties().getBool(PropertiesProvider.RYFT_INTEGRATION_ENABLED);
                } else {
                    isRyftIntegrationElabled = properties.getBool(PropertiesProvider.RYFT_INTEGRATION_ENABLED);
                }
                if (isRyftIntegrationElabled && (requestEvent != null)) {
                    requestEvent.setCallback(listener);
                    producer.send(requestEvent);
                    return true;
                }
                return false;
            }
        } catch (ElasticConversionException ex) {
            if (ex != null) {
                LOGGER.error("Convertion exception.", ex);
                return ex instanceof ElasticConversionCriticalException;
            } else {
                return false;
            }
        }
    }

}
