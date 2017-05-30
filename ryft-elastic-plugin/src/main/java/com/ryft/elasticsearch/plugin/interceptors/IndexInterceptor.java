package com.ryft.elasticsearch.plugin.interceptors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.tasks.Task;

import com.ryft.elasticsearch.plugin.PropertiesProvider;

public class IndexInterceptor implements ActionInterceptor {
    private final ESLogger logger = Loggers.getLogger(getClass());
    PropertiesProvider propertiesProvider;

    @Inject
    public IndexInterceptor(PropertiesProvider provider) {
        propertiesProvider = provider;
    }

    @Override
    public boolean intercept(Task task, String action, ActionRequest request, ActionListener listener,
            ActionFilterChain chain) {
        String settingsIndex = propertiesProvider.get().getStr(PropertiesProvider.PLUGIN_SETTINGS_INDEX);
        if (request instanceof IndexRequest) {
            IndexRequest req = (IndexRequest) request;
            String id = req.id(); // Document id
            String type = req.type();
            String index = req.index();
            if (settingsIndex.equals(index)) {
              return true;
            }
        } else {
            logger.info("Intercepted request: {}", request);
        }
        return false;
    }

}
