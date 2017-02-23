package com.ryft.elasticsearch.integration.test;

import static org.hamcrest.Matchers.greaterThan;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.MatchQueryBuilder.Type;
import org.elasticsearch.search.SearchHit;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class RyftElasticPluginSmokeTest extends ESSmokeClientTestCase {
    protected static final ESLogger logger = ESLoggerFactory.getLogger(ESSmokeClientTestCase.class.getName());
    // index field from super will be deleted after test
    private static final String INDEX_NAME = "shakespeare";
    private static final String ALTERNATIVE_INDEX_NAME = "integration";

    private Client client;

    @Before
    public void before() throws IOException {
        client = getClient();
        // START SNIPPET: java-doc-admin-cluster-health
        ClusterHealthResponse health = client.admin().cluster().prepareHealth().setWaitForYellowStatus().get();
        String clusterName = health.getClusterName();
        int numberOfNodes = health.getNumberOfNodes();
        // END SNIPPET: java-doc-admin-cluster-health
        assertThat("cluster [" + clusterName + "] should have at least 1 node", numberOfNodes, greaterThan(0));

        boolean exists = client.admin().indices()
                .prepareExists(ALTERNATIVE_INDEX_NAME)
                .execute().actionGet().isExists();

        if (!exists) {
            logger.info("Creating index {}", ALTERNATIVE_INDEX_NAME);
            client.admin().indices().prepareCreate(ALTERNATIVE_INDEX_NAME).get();

            client.admin().indices().preparePutMapping(ALTERNATIVE_INDEX_NAME).setType("data").setSource("{\n" +
                    "    \"data\" : {\n" +
                    "        \"properties\" : {\n" +
                    "            \"registered\" : {\"type\" : \"date\", \"format\" : \"yyyy-MM-dd HH:mm:ss\"}\n" +
                    "        }\n" +
                    "    }\n" +
                    "}").get();

            ClassLoader classLoader = getClass().getClassLoader();
            File file = new File(classLoader.getResource("dataset.json").getFile());

            ObjectMapper mapper = new ObjectMapper();
            ArrayList<TestData> testData = mapper.readValue(file, new TypeReference<List<TestData>>(){});

            BulkRequestBuilder bulkRequest = client.prepareBulk();

            testData.forEach(data -> {
                String json = "";
                try {
                    json = mapper.writeValueAsString(data);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                bulkRequest.add(client.prepareIndex(ALTERNATIVE_INDEX_NAME, "data", data.getId())
                        .setSource(json));
            });
            BulkResponse bulkResponse = bulkRequest.get();
            if(bulkResponse.hasFailures()) {
                logger.error(bulkResponse.buildFailureMessage());
            } else {
                logger.error("Bulk indexing succeeded.");
            }
        }
    }

    @AfterClass
    public static void afterClass() {
        logger.info("Deleting created indices");
        getClient().admin().indices().prepareDelete(ALTERNATIVE_INDEX_NAME).get();
    }

    /**
     * match-phrase-query: Fuzziness 1 looking for speaker: All the worlds a.
     * original phrase: To be, or not to be
     */
    @Test
    public void testSimpleFuzzyMatch() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builder = QueryBuilders.matchPhraseQuery("text_entry", "To be or not to be")
                .fuzziness(Fuzziness.ONE)
                .operator(Operator.AND);
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();

        String elasticQuery = "{" + "\"query\": {" + "\"match_phrase\": {" + "\"text_entry\":{"
                + "\"query\":\"To be or not to be\"," + "\"fuzziness\":\"1\"" + "}" + "}" + "},"
                + "\"ryft_enabled\":true" + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * match-phrase-query: Fuzziness 1 looking for speaker: All the worlds a.
     * original phrase: All the world's a stage
     */
    @Test
    public void testSimpleFuzzyMatch2() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builder = QueryBuilders.matchPhraseQuery("text_entry", "All the worlds a")
                .fuzziness(Fuzziness.AUTO)
                .operator(Operator.AND);
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();

        String elasticQuery = "{" + "\"query\": {" + "\"match_phrase\": {" + "\"text_entry\":{"
                + "\"query\":\"All the worlds a\"," + "\"fuzziness\":\"2\"" + "}" + "}" + "},"
                + "\"ryft_enabled\":true" + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * fuzzy-query: Fuzziness 1 looking for speaker: marcelus
     * original speaker: Marcellus
     */
    @Test
    public void testSimpleFuzzyQuery() throws InterruptedException, ExecutionException {
        FuzzyQueryBuilder builder = QueryBuilders.fuzzyQuery("speaker", "marcelus")
                .fuzziness(Fuzziness.ONE);
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();

        String elasticQuery = "{\"query\": {\"fuzzy\": {\"speaker\": "
                + "{\"value\": \"marcelus\", \"fuzziness\": 1}}}, \"ryft_enabled\":true }";

        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * fuzzy-query: Fuzziness 2 looking for speaker: macelus
     * original speaker: Marcellus
     */
    @Test
    public void testSimpleFuzzyQuery2() throws InterruptedException, ExecutionException {
        FuzzyQueryBuilder builder = QueryBuilders.fuzzyQuery("speaker", "macelus")
                .fuzziness(Fuzziness.TWO);
        logger.info("Testing query: {}", builder.toString());

        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setSize(total).setFrom(0)
                .get();

        String elasticQuery = "{\"query\": {\"fuzzy\": {\"speaker\": "
                + "{\"value\": \"macelus\", \"fuzziness\": 2}}}, \"ryft_enabled\":true }";

        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Match-query: Fuzziness 1 Looking for: Lord Hmlet
     * original : Lord Hamlet
     */
    @Test
    public void testMatchMust() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builder = QueryBuilders
                .matchQuery("text_entry", "Lord Hmlet")
                .fuzziness(Fuzziness.ONE).operator(Operator.AND).fuzziness(Fuzziness.ONE);

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();

        String ryftQuery = "{\r\n\"query\": {\r\n\"match\" : {\r\n \"text_entry\": {\r\n\"query\":\"Lord Hmlet\",\r\n\"fuzziness\": \"1\",\r\n\"operator\":\"and\"\r\n}\r\n}\r\n},\r\n \"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();

        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Match-query: Fuzziness 2 Looking for: Lord mlet
     * original : Lord Hamlet
     */
    @Test
    public void testMatchMust2() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builder = QueryBuilders
                .matchQuery("text_entry", "Lord mlet")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();

        String ryftQuery = "{\r\n\"query\": {\r\n\"match\" : {\r\n \"text_entry\": {\r\n\"query\":\"Lord mlet\",\r\n\"fuzziness\": \"2\",\r\n\"operator\":\"and\"\r\n}\r\n}\r\n},\r\n\"size\":30000,\r\n \"ryft_enabled\": true}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }


    /**
     * Bool-match-must-query: Fuzziness 1 Looking for: Lrd amlet and speaker: LRD PLONIU
     * original : Lord Hamlet, Lord POLONIU
     */
    @Test
    public void testBoolMatchMust() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("speaker", "LRD POLONIU")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("text_entry", "Lrd amlet")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().must(builderSpeaker).must(builderText);

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();

        String ryftQuery = "{\"query\": {\"bool\": { \"must\": [{\"match\": {\"text_entry\": {\"query\": \"Lrd amlet\",\"fuzziness\": 1,\"operator\": \"and\"}}},{\"match\": {\"speaker\": {\"query\": \"LRD POLONIU\",\"fuzziness\": 1,\"operator\": \"and\"}}}]}},\r\n\"size\":10000, \"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-must-query: Fuzziness 2 Looking for: Lrd mlet and speaker: LRD PLONIU
     * original : Lord Hamlet, Lord POLONIUS
     */
    @Test
    public void testBoolMatchMust2() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("speaker", "LRD PLONIU")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("text_entry", "Lrd mlet")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().must(builderSpeaker).must(builderText);

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();

        String ryftQuery = "{\"query\": {\"bool\": { \"must\": [{\"match\": {\"text_entry\": {\"query\": \"Lrd mlet\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"speaker\": {\"query\": \"LRD PLONIU\",\"fuzziness\": 2,\"operator\": \"and\"}}}]}},\r\n  \"size\":20000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-must-query: Fuzziness 1 Looking for: 'tht is qetion' and speaker: mlet
     * original : that is the question: Hamlet
     */
    @Test
    public void testBoolMatchMust3() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("text_entry", "tht is qetion")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("speaker", "mlet")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().must(builderSpeaker).must(builderText);

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();

        String ryftQuery = "{\"query\": {\"bool\": { \"must\": [{\"match\": {\"speaker\": {\"query\": \"mlet\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"text_entry\": {\"query\": \"tht is qetion\",\"fuzziness\": 2,\"operator\": \"and\"}}}]}},\r\n  \"size\":20000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-must-query: Fuzziness 2 Looking for: 'tht is qetion' and speaker: mlet
     * original : that is the question: Hamlet
     */
    @Test
    public void testBoolMatchMust4() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("text_entry", "th is te qution")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("speaker", "mlet")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().must(builderSpeaker).must(builderText);

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();

        String ryftQuery = "{\"query\": {\"bool\": { \"must\": [{\"match\": {\"speaker\": {\"query\": \"mlet\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"text_entry\": {\"query\": \"th is te qution\",\"fuzziness\": 2,\"operator\": \"and\"}}}]}},\r\n  \"size\":20000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-should-query: Fuzziness 1 Looking for: 'that is the quetion' OR 'all the world a stage'
     * original : that is the question, all the worlds a stage
     */
    @Test
    public void testBoolMatchShould() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("text_entry", "that is the quetion")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("text_entry", "all the world a stage")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().should(builderSpeaker).should(builderText).minimumShouldMatch("1");

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();

        String ryftQuery = "{\"query\": {\"bool\": { \"should\": [{\"match\": {\"text_entry\": {\"query\": \"that is the quetion\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"text_entry\": {\"query\": \"all the world a stage\",\"fuzziness\": 1,\"operator\": \"and\"}}}]}},\r\n  \"size\":20000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-should-query: Fuzziness 2 Looking for: 'tht is th quetion' OR 'all the wld a stage'
     * original : that is the question, all the worlds a stage
     */
    @Test
    public void testBoolMatchShould2() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("text_entry", "tht is th quetion")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("text_entry", "all the wld a stage")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().should(builderSpeaker).should(builderText).minimumShouldMatch("1");

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();

        String ryftQuery = "{\"query\": {\"bool\": { \"should\": [{\"match\": {\"text_entry\": {\"query\": \"tht is th quetion\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"text_entry\": {\"query\": \"all the wld a stage\",\"fuzziness\": 2,\"operator\": \"and\"}}}]}},\r\n  \"size\":30000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-should-query: Fuzziness 2 Looking for: 'my lord' and speaker 'PONIUS' or for 'my lord' and speaker 'Mesenger'
     * Used minimum_should match parameter and type:'phrase'
     */
    @Test
    public void testBoolMatchShould3() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("text_entry", "my lord")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO).type(Type.PHRASE);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("speaker", "PONIUS")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        MatchQueryBuilder builder3 = QueryBuilders
                .matchQuery("speaker", "Mesenger")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        //"A horse! a horse! my kingdom for a horse!"

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().should(builderSpeaker).should(builderText).should(builder3).minimumShouldMatch("2");

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();

        String ryftQuery = "{\"query\": {\"bool\": { \"should\": [{\"match\": {\"text_entry\": {\"query\": \"my lrd\",\"fuzziness\": 2,\"type\":\"phrase\",\"operator\": \"and\"}}},{\"match\": {\"speaker\": {\"query\": \"PONIUS\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"speaker\": {\"query\": \"Mesenger\",\"fuzziness\": 2,\"operator\": \"and\"}}} ], \"minimum_should_match\":1 }},\r\n  \"size\":30000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }


    /**
     * Bool-match-should-query: Fuzziness 2 Looking for: 'my lord' and speaker 'PONIUS' or for 'my lord' and speaker 'Mesenger'
     * Used minimum_should match parameter and type:'phrase' AND MUST NOT:
     */
    @Test
    public void testBoolMatchShouldMustNot4() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("text_entry", "Hamlet hamlet")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO).type(Type.PHRASE);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("speaker", "LAERTES")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        MatchQueryBuilder builder3 = QueryBuilders
                .matchQuery("speaker", "QUEEN GERTRUDE")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO).type(Type.PHRASE);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().should(builderSpeaker).should(builder3).mustNot(builder3).minimumShouldMatch("1");

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();

        String ryftQuery = "{\"query\": {\"bool\": { \"must_not\":[{ \"match\":{\"speaker\":{\"query\":\"QUEEN GERTRUDE\",\"type\":\"phrase\"}}}], \"must\": [{\"match\": {\"text_entry\": {\"query\": \"Hamlet hamlet\",\"fuzziness\": 1,\"type\":\"phrase\",\"operator\": \"and\"}}},{\"match\": {\"speaker\": {\"query\": \"LAERTES\",\"fuzziness\": 1,\"operator\": \"and\"}}} ], \"minimum_should_match\":1 }},\r\n  \"size\":30000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testWildcardMatch() throws InterruptedException, ExecutionException {
        WildcardQueryBuilder builder = QueryBuilders.wildcardQuery("text_entry", "w?rlds");
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();

        String ryftQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"match_phrase\": {\n" +
                "      \"text_entry\": \"w\\\"?\\\"rlds\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"ryft_enabled\":true\n" +
                "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testRawTextSearch() throws InterruptedException, ExecutionException {
        String ryftQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"match_phrase\": {\n" +
                "      \"_all\": {\n" +
                "        \"query\": \"Jones\",\n" +
                "        \"fuzziness\": 1,\n" +
                "        \"width\": \"line\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"ryft\": {\n" +
                "    \"enabled\": true,\n" +
                "    \"files\": [\"passengers.txt\"],\n" +
                "    \"format\": \"utf8\"\n" +
                "  }\n" +
                "}\n";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        assertResponse(ryftResponse);
        assertTrue(ryftResponse.getHits().getHits().length > 0);
    }

    @Test
    public void testDatetimeTerm() throws InterruptedException, ExecutionException {
        TermQueryBuilder builder = QueryBuilders.termQuery("timestamp", "2014-01-01 07:00:00");
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(ALTERNATIVE_INDEX_NAME).setQuery(builder).get();

        String ryftQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"timestamp\": {\n" +
                "        \"value\": \"2014-01-01 07:00:00\",\n" +
                "        \"type\": \"datetime\",\n" +
                "        \"format\": \"yyyy-MM-dd HH:mm:ss\"\n" +
                "      }\n" +
                "    }\n" +
                "  }, \n" +
                "\"ryft_enabled\": true\n" +
                "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{ALTERNATIVE_INDEX_NAME}, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testDatetimeRange() throws InterruptedException, ExecutionException {
        RangeQueryBuilder builder = QueryBuilders.rangeQuery("timestamp").gt("2014-01-01 07:00:00").lt("2014-01-07 07:00:00");
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(ALTERNATIVE_INDEX_NAME).setQuery(builder).get();

        String ryftQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"range\" : {\n" +
                "      \"timestamp\" : {\n" +
                "        \"gt\" : \"2014-01-01 07:00:00\",\n" +
                "        \"lt\" : \"2014-01-07 07:00:00\",\n" +
                "        \"type\": \"datetime\",\n" +
                "        \"format\": \"yyyy-MM-dd HH:mm:ss\"\n" +
                "      }\n" +
                "    }\n" +
                "  }, \n" +
                "\"ryft_enabled\": true\n" +
                "}\n";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{ALTERNATIVE_INDEX_NAME}, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testNumericTerm() throws InterruptedException, ExecutionException {
        TermQueryBuilder builder = QueryBuilders.termQuery("age", 22);
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(ALTERNATIVE_INDEX_NAME).setQuery(builder).get();

        String ryftQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"age\": {\n" +
                "        \"value\": \"22\",\n" +
                "        \"type\": \"number\"\n" +
                "      }\n" +
                "    }\n" +
                "  }, \n" +
                "\"ryft_enabled\": true\n" +
                "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{ALTERNATIVE_INDEX_NAME}, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testNumericRange() throws InterruptedException, ExecutionException {
        RangeQueryBuilder builder = QueryBuilders.rangeQuery("age").gt(22).lt(29);
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(ALTERNATIVE_INDEX_NAME).setQuery(builder).get();

        String ryftQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"range\" : {\n" +
                "      \"age\" : {\n" +
                "        \"gt\" : \"22\",\n" +
                "        \"lt\" : \"29\",\n" +
                "        \"type\": \"number\"\n" +
                "      }\n" +
                "    }\n" +
                "  }, \n" +
                "\"ryft_enabled\": true\n" +
                "}\n";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{ALTERNATIVE_INDEX_NAME}, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testCurrencyTerm() throws InterruptedException, ExecutionException {
        QueryStringQueryBuilder builder = QueryBuilders.queryStringQuery("$1,158.96").field("balance");
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(ALTERNATIVE_INDEX_NAME).setQuery(builder).get();
        logger.info(String.valueOf(searchResponse.getHits().getTotalHits()));

        String ryftQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"age\": {\n" +
                "        \"value\": \"$1,158.96\",\n" +
                "        \"type\": \"currency\"\n" +
                "      }\n" +
                "    }\n" +
                "  }, \n" +
                "\"ryft_enabled\": true\n" +
                "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{ALTERNATIVE_INDEX_NAME}, ryftQuery.getBytes())).get();
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    public void ryftQuerySample() throws IOException, InterruptedException, ExecutionException {
        String elasticQuery = "{\"query\":{" + "\"match_phrase\": { " + "\"doc.text_entry\": {"
                + "\"query\":\"To be, or not to be\"," + "\"metric\": \"Fhs\"," + "\"fuzziness\": 5" + "}" + "}" + "}}";
        logger.info("Testing query: {}", elasticQuery);
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        assertNotNull(ryftResponse);
    }

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
