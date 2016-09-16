package com.dataart.ryft.elastic.plugin;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.tasks.Task;

public class RestSearchActionFilter implements ActionFilter {
    private final ESLogger logger = Loggers.getLogger(getClass());
    private final static String QUERY_KEY = "query";
    private final static String RYFT_SEARCH_KEY = "ryftSearch";

    public int order() {
        return 0; // We are the first here!
    }

    public void apply(Task task, String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {
        if (action.equals(SearchAction.INSTANCE.name())) {
            // TODO: [imasternoy] ugly, sorry.
            long startTime = System.nanoTime();
            byte[] searchContent = ((SearchRequest) request).source().copyBytesArray().array();
            try {
                Map<String, Object> json = (Map<String, Object>) XContentFactory.xContent(searchContent)
                        .createParser(searchContent).map();
                if (!json.containsKey(QUERY_KEY)) {
                    chain.proceed(task, action, request, listener);
                }
                Map<String, Object> queryContent = (Map<String, Object>) json.get(QUERY_KEY);
                if (!queryContent.containsKey(RYFT_SEARCH_KEY)) {
                    chain.proceed(task, action, request, listener);
                }
                // This is our case
                // TODO: [imasternoy] Process the request
                listener.onResponse(new SearchResponse(InternalSearchResponse.empty(), null, 0, 0, startTime
                        - System.nanoTime(), ShardSearchFailure.EMPTY_ARRAY));
            } catch (IOException e) {
                logger.error("Failed to filter search action", e);
            }
        } else {
            chain.proceed(task, action, request, listener);
        }
    }

    public void apply(String action, ActionResponse response, ActionListener listener, ActionFilterChain chain) {
        System.out.println(action);
    }

}
