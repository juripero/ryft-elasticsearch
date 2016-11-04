package com.dataart.ryft.integration.test;

import static org.hamcrest.Matchers.greaterThan;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RyftElasticPluginSmokeTest extends ESSmokeClientTestCase {
    protected static final ESLogger logger = ESLoggerFactory.getLogger(ESSmokeClientTestCase.class.getName());
    // index field from super will be deleted after test
    private static final String INDEX_NAME = "shakespeare";

    private Client client;

    @Before
    public void before() {
        client = getClient();
        // START SNIPPET: java-doc-admin-cluster-health
        ClusterHealthResponse health = client.admin().cluster().prepareHealth().setWaitForYellowStatus().get();
        String clusterName = health.getClusterName();
        int numberOfNodes = health.getNumberOfNodes();
        // END SNIPPET: java-doc-admin-cluster-health
        assertThat("cluster [" + clusterName + "] should have at least 1 node", numberOfNodes, greaterThan(0));
    }

    /**
     * Check that we are connected to a cluster named "elasticsearch".
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Ignore
    @Test
    public void testSimpleFuzzyMatch() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builder = QueryBuilders.matchPhraseQuery("text_entry", "To be or not to be")//
                .fuzziness(Fuzziness.ONE)//
                .operator(Operator.AND);
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();//

        String elasticQuery = "{" + "\"query\": {" + "\"match_phrase\": {" + "\"text_entry\":{"
                + "\"query\":\"To be or not to be\"," + "\"fuzziness\":\"1\"" + "}" + "}" + "},"
                + "\"ryft_enabled\":true" + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[] { INDEX_NAME }, elasticQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Ignore
    @Test
    public void testSimpleFuzzyMatch2() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builder = QueryBuilders.matchPhraseQuery("text_entry", "All the worlds a")//
                .fuzziness(Fuzziness.AUTO)//
                .operator(Operator.AND);
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();//

        String elasticQuery = "{" + "\"query\": {" + "\"match_phrase\": {" + "\"text_entry\":{"
                + "\"query\":\"All the worlds a\"," + "\"fuzziness\":\"1\"" + "}" + "}" + "},"
                + "\"ryft_enabled\":true" + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[] { INDEX_NAME }, elasticQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * fuzzy-query: Fuzziness 1 looking for speaker: marcelus
     */
    @Ignore
    @Test
    public void testSimpleFuzzyQuery() throws InterruptedException, ExecutionException {
        FuzzyQueryBuilder builder = QueryBuilders.fuzzyQuery("speaker", "marcelus")//
                .fuzziness(Fuzziness.ONE);//
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();//

        String elasticQuery = "{\"query\": {\"fuzzy\": {\"speaker\": "
                + "{\"value\": \"marcelus\", \"fuzziness\": 1}}}, \"ryft_enabled\":true }";

        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[] { INDEX_NAME }, elasticQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * fuzzy-query: Fuzziness 2 looking for speaker: macelus
     */
    @Ignore
    @Test
    public void testSimpleFuzzyQuery2() throws InterruptedException, ExecutionException {
        FuzzyQueryBuilder builder = QueryBuilders.fuzzyQuery("speaker", "macelus")//
                .fuzziness(Fuzziness.TWO);//
        logger.info("Testing query: {}", builder.toString());

        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setSize(total).setFrom(0)
                .get();//

        String elasticQuery = "{\"query\": {\"fuzzy\": {\"speaker\": "
                + "{\"value\": \"macelus\", \"fuzziness\": 2}}}, \"ryft_enabled\":true }";

        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[] { INDEX_NAME }, elasticQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Match-query: Fuzziness 1 Looking for: Lord Hmlet
     * 
     */
    @Ignore
    @Test
    public void testMatchMust() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builder = QueryBuilders//
                .matchQuery("text_entry", "Lord Hmlet")//
                .fuzziness(Fuzziness.ONE).operator(Operator.AND).fuzziness(Fuzziness.ONE);//

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();//

        String ryftQuery = "{\r\n\"query\": {\r\n\"match\" : {\r\n \"text_entry\": {\r\n\"query\":\"Lord Hmlet\",\r\n\"fuzziness\": \"1\",\r\n\"operator\":\"and\"\r\n}\r\n}\r\n},\r\n \"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[] { INDEX_NAME }, ryftQuery.getBytes())).get();

        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Match-query: Fuzziness 2 Looking for: Lord mlet
     * 
     */
    @Ignore
    @Test
    public void testMatchMust2() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builder = QueryBuilders//
                .matchQuery("text_entry", "Lord mlet")//
                .fuzziness(Fuzziness.ONE).operator(Operator.AND).fuzziness(Fuzziness.TWO);//

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();//

        String ryftQuery = "{\r\n\"query\": {\r\n\"match\" : {\r\n \"text_entry\": {\r\n\"query\":\"Lord mlet\",\r\n\"fuzziness\": \"2\",\r\n\"operator\":\"and\"\r\n}\r\n}\r\n},\r\n \"ryft_enabled\": true,\r\n \"size\":-1}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[] { INDEX_NAME }, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Ignore
    @Test
    public void testRyftQuerySample() throws IOException, InterruptedException, ExecutionException {
        String elasticQuery = "{\"query\":{" + "\"match_phrase\": { " + "\"doc.text_entry\": {"
                + "\"query\":\"To be, or not to be\"," + "\"metric\": \"Fhs\"," + "\"fuzziness\": 5" + "}" + "}" + "}}";
        logger.info("Testing query: {}", elasticQuery);
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[] { INDEX_NAME }, elasticQuery.getBytes())).get();
        assertNotNull(ryftResponse);
    }

    //

    private int getSize(QueryBuilder builder) {
        SearchResponse countResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setSize(0).get();
        assertNotNull(countResponse);
        assertNotNull(countResponse.getHits());
        assertTrue(countResponse.getHits().getTotalHits() > 0);
        Long total = countResponse.getHits().getTotalHits();
        return total.intValue();
    }

    private void elasticSubsetRyft(SearchResponse searchResponse, SearchResponse ryftResponse) {
        assertResponse(searchResponse);
        assertResponse(ryftResponse);
        assertTrue(ryftResponse.getHits().getHits().length >= searchResponse.getHits().getHits().length);

        SearchHit[] elasticHits = searchResponse.getHits().getHits();
        Map<String, SearchHit> hitMap = new HashMap<String, SearchHit>();
        for (int i = 0; i < elasticHits.length; i++) {
            hitMap.put(elasticHits[i].getId(), elasticHits[i]);
        }

        SearchHit[] ryftHits = ryftResponse.getHits().getHits();
        for (int i = 0; i < ryftHits.length; i++) {
            hitMap.remove(ryftHits[i].getId());
        }

        if (!hitMap.isEmpty()) {
            hitMap.forEach((k, v) -> {
                logger.info("Not in Ryft response {}", v.getSourceAsString());
            });
            assertTrue("ES contains results that not included in RYFT", false);
        }
        // Everything is OK
        assertTrue(true);
    }

    private void assertResponse(SearchResponse searchResponse) {
        assertNotNull(searchResponse);
        assertNotNull(searchResponse.getHits());
        assertNotNull(searchResponse.getHits().getHits());
    }
}
