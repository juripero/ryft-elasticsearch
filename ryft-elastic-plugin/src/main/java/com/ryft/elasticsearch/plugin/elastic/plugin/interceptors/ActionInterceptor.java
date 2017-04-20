package com.ryft.elasticsearch.plugin.elastic.plugin.interceptors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.tasks.Task;

public interface ActionInterceptor {

    /**
     * 
     * Intercepts action execution and returns true/false if future execution allowed.
     * 
     */
    public boolean intercept(Task task, String action, ActionRequest request, ActionListener listener,
            ActionFilterChain chain);

}
