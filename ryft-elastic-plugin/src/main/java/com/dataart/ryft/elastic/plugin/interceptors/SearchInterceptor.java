package com.dataart.ryft.elastic.plugin.interceptors;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.tasks.Task;

import com.dataart.ryft.disruptor.EventProducer;
import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.elastic.parser.FuzzyQueryParser;
import com.dataart.ryft.elastic.plugin.rest.client.RyftRestClient;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class SearchInterceptor implements ActionInterceptor {
    private final ESLogger logger = Loggers.getLogger(getClass());
    private final static String QUERY_KEY = "query";
    private final static String QUERY_STRING = "query_string";
    private static final String RYFT_SEARCH_URL = "http://172.16.13.3:8765";

    EventProducer<RyftRequestEvent> producer;

    @Inject
    public SearchInterceptor( EventProducer<RyftRequestEvent> producer) {
        this.producer = producer;
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public boolean intercept(Task task, String action, ActionRequest request, ActionListener listener,
            ActionFilterChain chain) {
        // TODO: [imasternoy] ugly, sorry.
        SearchRequest searchReq = ((SearchRequest) request);
        BytesReference searchContent = searchReq.source().copyBytesArray();
        try {
            RyftRequestEvent ryftFuzzy = FuzzyQueryParser.parseQuery(searchContent);
            if (ryftFuzzy == null) {
                // Not fuzzy search request case
                return true;
            }
            ryftFuzzy.setIndex(searchReq.indices());
            ryftFuzzy.setType(searchReq.types());

            logger.info("Ryft request has been generated {}", ryftFuzzy);
            producer.send(ryftFuzzy);

            Map<String, Object> json = (Map<String, Object>) XContentFactory.xContent(searchContent)
                    .createParser(searchContent).map();

            Map<String, Object> queryContent = (Map<String, Object>) json.get(QUERY_KEY);
            if (!queryContent.containsKey(QUERY_STRING)) {
                return true;
            }
            Map<String, Object> queryString = (Map<String, Object>) queryContent.get(QUERY_STRING);
            // OMG
            String searchQuery = queryString.get(QUERY_KEY) instanceof String ? (String) queryString.get(QUERY_KEY)
                    : "";

            if (!searchQuery.startsWith("_ryftSearch")) {
                if (queryContent.containsKey("someQuery")) {
                    List<InternalSearchHit> searchHits = new ArrayList<InternalSearchHit>();
                    InternalSearchHit searchHit = new InternalSearchHit(1, "12345", new Text("igorTest"),
                            ImmutableMap.of());
                    searchHit.shard(new SearchShardTarget("0", "shakespeare", 0));
                    searchHit.sourceRef(((BytesReference) new BytesArray("{\"test\":\"testValue\"}")));
                    searchHits.add(searchHit);
                    InternalSearchHits hits = new InternalSearchHits(
                            searchHits.toArray(new InternalSearchHit[searchHits.size()]), searchHits.size(), 1.0f);
                    InternalSearchResponse searchResponse = new InternalSearchResponse(hits,
                            InternalAggregations.EMPTY, null, null, false, false);
                    SearchResponse response = new SearchResponse(searchResponse, null, 1, 1, 10L,
                            ShardSearchFailure.EMPTY_ARRAY);
                    listener.onResponse(response);
                }
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

            // TODO: [imasternoy] Create message add it to the queue
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
