package com.dataart.ryft.elastic.plugin.interceptors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.tasks.Task;

import com.dataart.ryft.disruptor.EventProducer;
import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.elastic.converter.ElasticConverter;
import com.dataart.ryft.elastic.converter.ElasticConversionException;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import java.util.Optional;

public class SearchInterceptor implements ActionInterceptor {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final static String QUERY_KEY = "query";
    private final static String QUERY_STRING = "query_string";
    private static final String RYFT_SEARCH_URL = "http://172.16.13.3:8765";

    private final EventProducer<RyftRequestEvent> producer;
    private final ElasticConverter elasticParser;

    @Inject
    public SearchInterceptor(EventProducer<RyftRequestEvent> producer,
            ElasticConverter elasticParser) {
        this.producer = producer;
        this.elasticParser = elasticParser;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean intercept(Task task, String action, ActionRequest request, ActionListener listener,
            ActionFilterChain chain) {
        // TODO: [imasternoy] ugly, sorry.
//        SearchRequest searchReq = ((SearchRequest) request);
//        BytesReference searchContent = searchReq.source().copyBytesArray();
        try {
            Optional<RyftQuery> maybeRyftQuery = elasticParser.parse(request);
            if (maybeRyftQuery.isPresent()) {
                logger.info("Ryft query {}", maybeRyftQuery.get().buildRyftString());
            }
//            XContentParser parser = XContentFactory.xContent(searchContent).createParser(searchContent);
//            ElasticParser ryftParser = new ElasticParserQuery();
//            ryftParser.parse(parser);
//            RyftRequestEvent ryftFuzzy = null;//FuzzyQueryParser.parseQuery(searchContent);
//            if (ryftFuzzy == null) {
//                // Not fuzzy search request case
//                return true;
//            }
//            ryftFuzzy.setIndex(searchReq.indices());
//            ryftFuzzy.setType(searchReq.types());
//            ryftFuzzy.setCallback(listener);
//
//            logger.info("Ryft request has been generated {}", ryftFuzzy);
//            producer.send(ryftFuzzy);
//
//            Map<String, Object> json = (Map<String, Object>) XContentFactory.xContent(searchContent)
//                    .createParser(searchContent).map();
//
//            Map<String, Object> queryContent = (Map<String, Object>) json.get(QUERY_KEY);
//            if (!queryContent.containsKey(QUERY_STRING)) {
//                return true;
//            }
//            Map<String, Object> queryString = (Map<String, Object>) queryContent.get(QUERY_STRING);
//            // OMG
//            String searchQuery = queryString.get(QUERY_KEY) instanceof String ? (String) queryString.get(QUERY_KEY)
//                    : "";
//
//            if (!searchQuery.startsWith("_ryftSearch")) {
//                if (queryContent.containsKey("someQuery")) {
//                    List<InternalSearchHit> searchHits = new ArrayList<InternalSearchHit>();
//                    InternalSearchHit searchHit = new InternalSearchHit(1, "12345", new Text("igorTest"),
//                            ImmutableMap.of());
//                    searchHit.shard(new SearchShardTarget("0", "shakespeare", 0));
//                    searchHit.sourceRef(((BytesReference) new BytesArray("{\"test\":\"testValue\"}")));
//                    searchHits.add(searchHit);
//                    InternalSearchHits hits = new InternalSearchHits(
//                            searchHits.toArray(new InternalSearchHit[searchHits.size()]), searchHits.size(), 1.0f);
//                    InternalSearchResponse searchResponse = new InternalSearchResponse(hits,
//                            InternalAggregations.EMPTY, null, null, false, false);
//                    SearchResponse response = new SearchResponse(searchResponse, null, 1, 1, 10L,
//                            ShardSearchFailure.EMPTY_ARRAY);
//                    listener.onResponse(response);
//                }
//                return true;
//            }
//
//            String uri = "/search?query=";
//
//            String customRyftQuery = searchQuery.substring("_ryftSearch".length());
//            if (!customRyftQuery.isEmpty() && customRyftQuery.length() > 1) {
//                uri = uri += customRyftQuery.substring(1);
//            } else {
//                // Sample search URI
//                uri = uri += "(RECORD.type%20CONTAINS%20%22act%22)&files=elasticsearch/elasticsearch/nodes/0/indices/shakespeare/0/index/_0.shakespearejsonfld&mode=es&format=json&local=true&stats=true";
//            }
//
//            // TODO: [imasternoy] Create message add it to the queue
//            RyftRestClient handler = new RyftRestClient(listener);
//            handler.init();
//            Channel channel = handler.getChannel();
//
//            channel.writeAndFlush(
//                    new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, RYFT_SEARCH_URL + uri))
//                    .addListener(new ChannelFutureListener() {
//
//                        public void operationComplete(ChannelFuture future) throws Exception {
//                            logger.debug("Operation complete");
//                        }
//                    });
        } catch (ElasticConversionException e) {
            logger.error("Failed to filter search action", e);
        }
        return true;
    }

}
