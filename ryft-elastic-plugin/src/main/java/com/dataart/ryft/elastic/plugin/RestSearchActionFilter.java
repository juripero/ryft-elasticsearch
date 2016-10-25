package com.dataart.ryft.elastic.plugin;

import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.tasks.Task;

import com.dataart.ryft.elastic.plugin.interceptors.ActionInterceptor;

@Singleton
@SuppressWarnings("rawtypes")
public class RestSearchActionFilter implements ActionFilter {
    // private final ESLogger logger = Loggers.getLogger(getClass());

    private Map<String, ActionInterceptor> interceptors;

    @Inject
    public RestSearchActionFilter(Map<String, ActionInterceptor> interceptors) {
        this.interceptors = interceptors;
    }

    @Override
    public int order() {
        return 0; // We are the first here!
    }

    @Override
    public void apply(Task task, String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {
        ActionInterceptor interceptor = interceptors.get(task.getAction());
        if (interceptor == null || !interceptor.intercept(task, action, request, listener, chain)) {
            chain.proceed(task, action, request, listener);
        }
    }

    @Override
    public void apply(String action, ActionResponse response, ActionListener listener, ActionFilterChain chain) {
        chain.proceed(action, response, listener);
    }

}
