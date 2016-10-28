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
import com.dataart.ryft.elastic.plugin.PropertiesProvider;
import com.dataart.ryft.elastic.plugin.RyftPluginGlobalSettingsProvider;

import com.dataart.ryft.utils.Try;
import java.util.Map;

public class SearchInterceptor implements ActionInterceptor {

    private final ESLogger LOGGER = Loggers.getLogger(SearchInterceptor.class);
    private final EventProducer<RyftRequestEvent> producer;
    private final ElasticConverter elasticConverter;
    private final PropertiesProvider provider;
    private final RyftPluginGlobalSettingsProvider globalSettings;

    @Inject
    public SearchInterceptor(PropertiesProvider provider, RyftPluginGlobalSettingsProvider globalSettings,
            EventProducer<RyftRequestEvent> producer, ElasticConverter elasticConverter) {
        this.provider = provider;
        this.globalSettings = globalSettings;
        this.producer = producer;
        this.elasticConverter = elasticConverter;
    }

    @Override
    public boolean intercept(Task task, String action, ActionRequest request, ActionListener listener,
            ActionFilterChain chain) {
        Try<RyftQuery> tryRyftQuery = elasticConverter.convert(request);
        Boolean isIntercepted = ryftIntegrationEnabled() && !tryRyftQuery.hasError();
        if (isIntercepted) {
            RyftQuery ryftQuery = tryRyftQuery.getResult();
            LOGGER.info("Constructed {}", ryftQuery);
            RyftRequestEvent ryftRequest = new RyftRequestEvent(ryftQuery);
            ryftRequest.setIndex(((SearchRequest) request).indices());
            ryftRequest.setCallback(listener);
            producer.send(ryftRequest);
        } else {
            Exception ex = tryRyftQuery.getError();
            LOGGER.error("Converion exception.", ex);
            return ex instanceof ElasticConversionCriticalException;
        }
        return isIntercepted;
    }

    private boolean ryftIntegrationEnabled() {
        Map<String, Object> settings = globalSettings.getGlobalSettings();
        if (settings == null) {
            return false;
        }
        return globalSettings.getBool(RyftPluginGlobalSettingsProvider.RYFT_INTEGRATION_ENABLED).orElse(false);
    }

}
