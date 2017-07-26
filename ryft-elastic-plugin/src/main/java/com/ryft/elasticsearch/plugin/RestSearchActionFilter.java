package com.ryft.elasticsearch.plugin;

import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.tasks.Task;

import com.ryft.elasticsearch.plugin.interceptors.ActionInterceptor;

@Singleton
public class RestSearchActionFilter implements ActionFilter {
    // private final ESLogger logger = Loggers.getLogger(getClass());
    private final PropertiesProvider provider;
    private final Map<String, ActionInterceptor> interceptors;
    private final RyftPluginGlobalSettingsProvider globalSettings;
    private boolean rereadProperties = false;

    @Inject
    public RestSearchActionFilter(PropertiesProvider provider, Map<String, ActionInterceptor> interceptors,
            RyftPluginGlobalSettingsProvider globalSettings) {
        this.interceptors = interceptors;
        this.provider = provider;
        this.globalSettings = globalSettings;
    }

    @Override
    public int order() {
        return 0; // We are the first here!
    }

    @Override
    public void apply(Task task, String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {
        ActionInterceptor interceptor = interceptors.get(task.getAction());
        if (request instanceof IndexRequest) {
            // We are updating global settings
            String settingsIndex = provider.get().getStr(PropertiesProvider.PLUGIN_SETTINGS_INDEX);
            IndexRequest req = (IndexRequest) request;
            String index = req.index();
            rereadProperties = settingsIndex.equals(index);
        } else if (interceptor != null && interceptor.intercept(task, action, request, listener, chain)) {
            return; // Our Search interceptor intercepted request
        }
        chain.proceed(task, action, request, listener);
    }

    @Override
    public void apply(String action, ActionResponse response, ActionListener listener, ActionFilterChain chain) {
        chain.proceed(action, response, listener);
        if (rereadProperties && response instanceof IndexResponse) {
            //ES saved new GlobalSettings we should reread them now
            String settingsIndex = provider.get().getStr(PropertiesProvider.PLUGIN_SETTINGS_INDEX);
            if (((IndexResponse) response).getIndex().equals(settingsIndex)) {
                globalSettings.rereadGlobalSettings();
                rereadProperties = false;
            }
        }
    }
}
