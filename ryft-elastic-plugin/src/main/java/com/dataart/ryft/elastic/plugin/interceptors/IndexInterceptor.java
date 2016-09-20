package com.dataart.ryft.elastic.plugin.interceptors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.tasks.Task;

public class IndexInterceptor implements ActionInterceptor {
    private final ESLogger logger = Loggers.getLogger(getClass());

    @Override
    public boolean intercept(Task task, String action, ActionRequest request, ActionListener listener,
            ActionFilterChain chain) {
        logger.info("Intercepted task: {}", task);
        return true;
    }

}
