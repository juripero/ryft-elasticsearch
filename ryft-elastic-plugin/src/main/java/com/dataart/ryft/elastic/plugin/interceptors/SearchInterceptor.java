package com.dataart.ryft.elastic.plugin.interceptors;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.tasks.Task;

import com.dataart.ryft.elastic.plugin.rest.client.RyftRestClient;

public class SearchInterceptor implements ActionInterceptor {
    private final ESLogger logger = Loggers.getLogger(getClass());
    private final static String QUERY_KEY = "query";
    private final static String QUERY_STRING = "query_string";
    private static final String RYFT_SEARCH_URL = "http://172.16.13.3:8765";

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public boolean intercept(Task task, String action, ActionRequest request, ActionListener listener,
            ActionFilterChain chain) {
        // TODO: [imasternoy] ugly, sorry.
        byte[] searchContent = ((SearchRequest) request).source().copyBytesArray().array();
        try {
            Map<String, Object> json = (Map<String, Object>) XContentFactory.xContent(searchContent)
                    .createParser(searchContent).map();
            if (!json.containsKey(QUERY_KEY)) {
                return true;
            }

            Map<String, Object> queryContent = (Map<String, Object>) json.get(QUERY_KEY);
            if (!queryContent.containsKey(QUERY_STRING)) {
                return true;
            }
            Map<String, Object> queryString = (Map<String, Object>) queryContent.get(QUERY_STRING);
            // OMG
            String searchQuery = queryString.get(QUERY_KEY) instanceof String ? (String) queryString.get(QUERY_KEY)
                    : "";

            if (!searchQuery.startsWith("_ryftSearch")) {
                return true;
            }

            String uri = "/search?query=";

            String customRyftQuery = searchQuery.substring("_ryftSearch".length());
            if (!customRyftQuery.isEmpty() && customRyftQuery.length() > 1) {
                uri = uri += customRyftQuery.substring(1);
            } else {
                // Sample search URI
                uri = uri += "(RECORD.type%20CONTAINS%20%22act%22)&files=elasticsearch/elasticsearch/nodes/0/indices/shakespeare/0/index/_0.shakespearejsonfld&mode=es&format=json&local=true&stats=true";
            }

            RyftRestClient handler = new RyftRestClient(listener);
            handler.init();
            Channel channel = handler.getChannel();

            channel.writeAndFlush(
                    new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, RYFT_SEARCH_URL + uri))
                    .addListener(new ChannelFutureListener() {

                        public void operationComplete(ChannelFuture future) throws Exception {
                            logger.debug("Operation complete");
                        }
                    });
        } catch (IOException e) {
            logger.error("Failed to filter search action", e);
        }
        return false;
    }

}
