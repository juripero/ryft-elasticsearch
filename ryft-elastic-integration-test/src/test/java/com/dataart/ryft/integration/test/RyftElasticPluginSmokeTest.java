package com.dataart.ryft.integration.test;

import static org.hamcrest.Matchers.greaterThan;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
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
import org.elasticsearch.index.query.QueryBuilders;
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
     */
//    @Ignore
    @Test
    public void testSimpleFuzzyMatch() {
        MatchQueryBuilder builder = QueryBuilders.matchPhraseQuery("text_entry", "To be or not to be")//
                .fuzziness(Fuzziness.TWO)//
                .operator(Operator.AND);
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();//
        assertResponse(searchResponse);
        assertEquals(1l, searchResponse.getHits().getTotalHits());
    }

//    @Ignore
    @Test
    public void testSimpleFuzzyQuery() {
        FuzzyQueryBuilder builder = QueryBuilders.fuzzyQuery("speaker", "macelus")//
                .fuzziness(Fuzziness.TWO);//
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();//
        assertResponse(searchResponse);
        assertEquals(69l, searchResponse.getHits().getTotalHits());
        assertTrue(searchResponse.getHits().getHits()[0].getSourceAsString().contains("\"line_id\":32453"));
    }

//    @Ignore
    @Test
    public void testBoolMatchMust() {
        BoolQueryBuilder builder = QueryBuilders.boolQuery().must(QueryBuilders//
                .matchQuery("speaker", "MARCELLIUS")//
                .fuzziness(Fuzziness.TWO))//
                .must(QueryBuilders//
                        .matchQuery("text_entry", "Hmlet")//
                        .fuzziness(Fuzziness.ONE));

        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();//
        assertResponse(searchResponse);
        assertEquals(3l, searchResponse.getHits().getTotalHits());
    }

    @Test
    public void testRyftQuerySample() throws IOException, InterruptedException, ExecutionException {
        String elasticQuery = "{\"query\":{"+
                                    "\"match_phrase\": { "+
                                    "\"doc.text_entry\": {"+
                                           "\"query\":\"To be, or not to be\","+
                                           "\"metric\": \"Fhs\","+
                                           "\"fuzziness\": 5"+
                                           "}"+
                                        "}"+
                                     "}}";
        logger.info("Testing query: {}", elasticQuery);
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[] { INDEX_NAME }, elasticQuery.getBytes())).get();
        assertNotNull(ryftResponse);
    }

    //

    private void assertResponse(SearchResponse searchResponse) {
        assertNotNull(searchResponse);
        assertNotNull(searchResponse.getHits());
    }
}
