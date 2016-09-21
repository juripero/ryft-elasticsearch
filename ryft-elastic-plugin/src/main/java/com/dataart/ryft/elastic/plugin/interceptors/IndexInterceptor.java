package com.dataart.ryft.elastic.plugin.interceptors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.tasks.Task;

public class IndexInterceptor implements ActionInterceptor {
    private final ESLogger logger = Loggers.getLogger(getClass());

    @Override
    public boolean intercept(Task task, String action, ActionRequest request, ActionListener listener,
            ActionFilterChain chain) {
        if (request instanceof IndexRequest) {
            IndexRequest req = (IndexRequest) request;
            String id = req.id(); // Document id
            String type = req.type();
            String index = req.index();
            String source = new String(req.source().array());// Document source
            logger.info("Write request received: id: {}, index/type: {}/{}, source: {}", id, index,type,source);

        } else {
            logger.info("Intercepted request: {}", request);
        }
        return true;
    }

}
