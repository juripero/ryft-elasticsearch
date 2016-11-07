package com.dataart.ryft.integration.test;

import static org.hamcrest.Matchers.greaterThan;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.MatchQueryBuilder.Type;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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
     * match-phrase-query: Fuzziness 1 looking for speaker: All the worlds a. 
     * original phrase: To be, or not to be
     * 
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

    /**
     * match-phrase-query: Fuzziness 1 looking for speaker: All the worlds a. 
     * original phrase: All the world's a stage
     */
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
     * original speaker: Marcellus
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
     * original speaker: Marcellus
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
     * original : Lord Hamlet
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
     * original : Lord Hamlet
     */
    @Ignore
    @Test
    public void testMatchMust2() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builder = QueryBuilders//
                .matchQuery("text_entry", "Lord mlet")//
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);//

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();//

        String ryftQuery = "{\r\n\"query\": {\r\n\"match\" : {\r\n \"text_entry\": {\r\n\"query\":\"Lord mlet\",\r\n\"fuzziness\": \"2\",\r\n\"operator\":\"and\"\r\n}\r\n}\r\n},\r\n\"size\":30000,\r\n \"ryft_enabled\": true}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[] { INDEX_NAME }, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }
    
    
    /**
     * Bool-match-must-query: Fuzziness 1 Looking for: Lrd amlet and speaker: LRD PLONIU
     * original : Lord Hamlet, Lord POLONIU
     */
    @Ignore
    @Test
    public void testBoolMatchMust() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders//
                .matchQuery("speaker", "LRD POLONIU")//
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);//
        
        MatchQueryBuilder builderText = QueryBuilders//
                .matchQuery("text_entry", "Lrd amlet")//
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);//
        
        BoolQueryBuilder builder = QueryBuilders//
                .boolQuery().must(builderSpeaker).must(builderText);

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();//

        String ryftQuery = "{\"query\": {\"bool\": { \"must\": [{\"match\": {\"text_entry\": {\"query\": \"Lrd amlet\",\"fuzziness\": 1,\"operator\": \"and\"}}},{\"match\": {\"speaker\": {\"query\": \"LRD POLONIU\",\"fuzziness\": 1,\"operator\": \"and\"}}}]}},\r\n\"size\":10000, \"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[] { INDEX_NAME }, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }
    
    /**
     * Bool-match-must-query: Fuzziness 2 Looking for: Lrd mlet and speaker: LRD PLONIU
     * original : Lord Hamlet, Lord POLONIUS
     */
    @Ignore
    @Test
    public void testBoolMatchMust2() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders//
                .matchQuery("speaker", "LRD PLONIU")//
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);//
        
        MatchQueryBuilder builderText = QueryBuilders//
                .matchQuery("text_entry", "Lrd mlet")//
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);//
        
        BoolQueryBuilder builder = QueryBuilders//
                .boolQuery().must(builderSpeaker).must(builderText);

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();//

        String ryftQuery = "{\"query\": {\"bool\": { \"must\": [{\"match\": {\"text_entry\": {\"query\": \"Lrd mlet\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"speaker\": {\"query\": \"LRD PLONIU\",\"fuzziness\": 2,\"operator\": \"and\"}}}]}},\r\n  \"size\":20000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[] { INDEX_NAME }, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }
    
    /**
     * Bool-match-must-query: Fuzziness 1 Looking for: 'tht is qetion' and speaker: mlet
     * original : that is the question: Hamlet
     */
    @Ignore
    @Test
    public void testBoolMatchMust3() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders//
                .matchQuery("text_entry", "tht is qetion")//
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);//
        
        MatchQueryBuilder builderText = QueryBuilders//
                .matchQuery("speaker", "mlet")//
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);//
        
        BoolQueryBuilder builder = QueryBuilders//
                .boolQuery().must(builderSpeaker).must(builderText);

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();//

        String ryftQuery = "{\"query\": {\"bool\": { \"must\": [{\"match\": {\"speaker\": {\"query\": \"mlet\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"text_entry\": {\"query\": \"tht is qetion\",\"fuzziness\": 2,\"operator\": \"and\"}}}]}},\r\n  \"size\":20000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[] { INDEX_NAME }, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-must-query: Fuzziness 2 Looking for: 'tht is qetion' and speaker: mlet
     * original : that is the question: Hamlet
     */
    @Ignore
    @Test
    public void testBoolMatchMust4() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders//
                .matchQuery("text_entry", "th is te qution")//
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);//
        
        MatchQueryBuilder builderText = QueryBuilders//
                .matchQuery("speaker", "mlet")//
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);//
        
        BoolQueryBuilder builder = QueryBuilders//
                .boolQuery().must(builderSpeaker).must(builderText);

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();//

        String ryftQuery = "{\"query\": {\"bool\": { \"must\": [{\"match\": {\"speaker\": {\"query\": \"mlet\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"text_entry\": {\"query\": \"th is te qution\",\"fuzziness\": 2,\"operator\": \"and\"}}}]}},\r\n  \"size\":20000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[] { INDEX_NAME }, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }
    
    /**
     * Bool-match-should-query: Fuzziness 1 Looking for: 'that is the quetion' OR 'all the world a stage'
     * original : that is the question, all the worlds a stage
     */
   @Ignore
    @Test
    public void testBoolMatchShould() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders//
                .matchQuery("text_entry", "that is the quetion")//
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);//
        
        MatchQueryBuilder builderText = QueryBuilders//
                .matchQuery("text_entry", "all the world a stage")//
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);//
        
        BoolQueryBuilder builder = QueryBuilders//
                .boolQuery().should(builderSpeaker).should(builderText).minimumShouldMatch("1");

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();//

        String ryftQuery = "{\"query\": {\"bool\": { \"should\": [{\"match\": {\"text_entry\": {\"query\": \"that is the quetion\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"text_entry\": {\"query\": \"all the world a stage\",\"fuzziness\": 1,\"operator\": \"and\"}}}]}},\r\n  \"size\":20000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[] { INDEX_NAME }, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }
    
   /**
    * Bool-match-should-query: Fuzziness 2 Looking for: 'tht is th quetion' OR 'all the wld a stage'
    * original : that is the question, all the worlds a stage
    */
   @Ignore
    @Test
    public void testBoolMatchShould2() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders//
                .matchQuery("text_entry", "tht is th quetion")//
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);//
        
        MatchQueryBuilder builderText = QueryBuilders//
                .matchQuery("text_entry", "all the wld a stage")//
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);//
        
        BoolQueryBuilder builder = QueryBuilders//
                .boolQuery().should(builderSpeaker).should(builderText).minimumShouldMatch("1");

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();//

        String ryftQuery = "{\"query\": {\"bool\": { \"should\": [{\"match\": {\"text_entry\": {\"query\": \"tht is th quetion\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"text_entry\": {\"query\": \"all the wld a stage\",\"fuzziness\": 2,\"operator\": \"and\"}}}]}},\r\n  \"size\":30000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[] { INDEX_NAME }, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }
    
    /**
     * Bool-match-should-query: Fuzziness 2 Looking for: 'Lrd Halet' and speaker 'PONIUS' or for 'Lrd Halet' and speaker 'Hlet'
     * original : that is the question, all the worlds a stage, A horse! a horse! my kingdom for a horse!
     */
     @Test
     public void testBoolMatchShould3() throws InterruptedException, ExecutionException {
         MatchQueryBuilder builderSpeaker = QueryBuilders//
                 .matchQuery("text_entry", "Lrd Halet")//
                 .operator(Operator.AND).fuzziness(Fuzziness.TWO).type(Type.PHRASE);//
         
         MatchQueryBuilder builderText = QueryBuilders//
                 .matchQuery("speaker", "PONIUS")//
                 .operator(Operator.AND).fuzziness(Fuzziness.TWO);//
         
         MatchQueryBuilder builder3 = QueryBuilders//
                 .matchQuery("speaker", "HLET")//
                 .operator(Operator.AND).fuzziness(Fuzziness.TWO);//
         
         //"A horse! a horse! my kingdom for a horse!"
         
         BoolQueryBuilder builder = QueryBuilders//
                 .boolQuery().should(builderSpeaker).should(builderText).should(builder3).minimumShouldMatch("2");

         logger.info("Testing query: {}", builder.toString());
         int total = getSize(builder);
         SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                 .get();//

         String ryftQuery = "{\"query\": {\"bool\": { \"should\": [{\"match\": {\"text_entry\": {\"query\": \"Lrd Halet\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"speaker\": {\"query\": \"PONIUS\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"speaker\": {\"query\": \"Hlet\",\"fuzziness\": 2,\"operator\": \"and\"}}} ], \"minimum_should_match\":2 }},\r\n  \"size\":30000,\"ryft_enabled\": true\r\n}";
         SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                 new SearchRequest(new String[] { INDEX_NAME }, ryftQuery.getBytes())).get();
         elasticSubsetRyft(searchResponse, ryftResponse);
     }


    public void ryftQuerySample() throws IOException, InterruptedException, ExecutionException {
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
