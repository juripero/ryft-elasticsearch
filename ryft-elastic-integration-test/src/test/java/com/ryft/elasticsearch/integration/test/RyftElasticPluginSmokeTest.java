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
import org.junit.Test;

public class RyftElasticPluginSmokeTest extends ESSmokeClientTestCase {
    private static final ESLogger logger = ESLoggerFactory.getLogger(ESSmokeClientTestCase.class.getName());
    // index field from super will be deleted after test
    private static final String INDEX_NAME = "integration";

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
                .prepareExists(INDEX_NAME)
                .execute().actionGet().isExists();

        if (!exists) {
            logger.info("Creating index {}", INDEX_NAME);
            client.admin().indices().prepareCreate(INDEX_NAME).get();

            client.admin().indices().preparePutMapping(INDEX_NAME).setType("data").setSource("{\n" +
                    "    \"data\" : {\n" +
                    "        \"properties\" : {\n" +
                    "            \"registered\" : {\"type\" : \"date\", \"format\" : \"yyyy-MM-dd HH:mm:ss\"}\n" +
                    "        }\n" +
                    "    }\n" +
                    "}").get();

            ClassLoader classLoader = getClass().getClassLoader();
            File file = new File(classLoader.getResource("dataset.json").getFile());

            ObjectMapper mapper = new ObjectMapper();
            ArrayList<TestData> testData = mapper.readValue(file, new TypeReference<List<TestData>>() {
            });

            BulkRequestBuilder bulkRequest = client.prepareBulk();

            testData.forEach(data -> {
                String json = "";
                try {
                    json = mapper.writeValueAsString(data);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                bulkRequest.add(client.prepareIndex(INDEX_NAME, "data", data.getId())
                        .setSource(json));
            });
            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                logger.error(bulkResponse.buildFailureMessage());
            } else {
                logger.error("Bulk indexing succeeded.");
            }
        }
    }

    @AfterClass
    public static void afterClass() {
        logger.info("Deleting created indices");
        getClient().admin().indices().prepareDelete(INDEX_NAME).get();
    }

    /**
     * match-phrase-query: Fuzziness 1 looking for about: Esse ipsum et laborum labore
     * original phrase: Esse ipsum et laborum labore
     */
    @Test
    public void testSimpleFuzzyMatch() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builder = QueryBuilders.matchPhraseQuery("about", "Esse ipsum et laborum labore")
                .fuzziness(Fuzziness.ONE)
                .operator(Operator.AND);
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String elasticQuery = "{" + "\"query\": {" + "\"match_phrase\": {" + "\"about\":{"
                + "\"query\":\"Esse ipsum et laborum labore\"," + "\"fuzziness\":\"1\"" + "}" + "}" + "},"
                + "\"ryft_enabled\":true" + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * match-phrase-query: Fuzziness 1 looking for about: Esse ipsum et laborum labore
     * original phrase: Esse ipsum et laborum labore
     */
    @Test
    public void testSimpleFuzzyMatch2() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builder = QueryBuilders.matchPhraseQuery("about", "Esse ipsum et laborum labore")
                .fuzziness(Fuzziness.AUTO)
                .operator(Operator.AND);
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String elasticQuery = "{" + "\"query\": {" + "\"match_phrase\": {" + "\"about\":{"
                + "\"query\":\"Esse ipsum et laborum labore\"," + "\"fuzziness\":\"2\"" + "}" + "}" + "},"
                + "\"ryft_enabled\":true" + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * fuzzy-query: Fuzziness 1 looking for first name: pitra
     * original first name: Petra
     */
    @Test
    public void testSimpleFuzzyQuery() throws InterruptedException, ExecutionException {
        FuzzyQueryBuilder builder = QueryBuilders.fuzzyQuery("firstName", "pitra")
                .fuzziness(Fuzziness.ONE);
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String elasticQuery = "{\"query\": {\"fuzzy\": {\"firstName\": "
                + "{\"value\": \"pitra\", \"fuzziness\": 1}}}, \"ryft_enabled\":true }";

        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * fuzzy-query: Fuzziness 2 looking for first name: pira
     * original first name: Petra
     */
    @Test
    public void testSimpleFuzzyQuery2() throws InterruptedException, ExecutionException {
        FuzzyQueryBuilder builder = QueryBuilders.fuzzyQuery("firstName", "pira")
                .fuzziness(Fuzziness.TWO);
        logger.info("Testing query: {}", builder.toString());

        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setSize(total).setFrom(0)
                .get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String elasticQuery = "{\"query\": {\"fuzzy\": {\"firstName\": "
                + "{\"value\": \"pira\", \"fuzziness\": 2}}}, \"ryft_enabled\":true }";

        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Match-query: Fuzziness 1 Looking for: Esse ipum
     * original : Esse ipsum
     */
    @Test
    public void testMatchMust() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builder = QueryBuilders
                .matchQuery("about", "Esse ipum")
                .fuzziness(Fuzziness.ONE).operator(Operator.AND).fuzziness(Fuzziness.ONE);

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\r\n\"query\": {\r\n\"match\" : {\r\n \"about\": {\r\n\"query\":\"Esse ipum\",\r\n\"fuzziness\": \"1\",\r\n\"operator\":\"and\"\r\n}\r\n}\r\n},\r\n \"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Match-query: Fuzziness 2 Looking for: Esse pum
     * original : Esse ipsum
     */
    @Test
    public void testMatchMust2() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builder = QueryBuilders
                .matchQuery("about", "Esse pum")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\r\n\"query\": {\r\n\"match\" : {\r\n \"about\": {\r\n\"query\":\"Esse pum\",\r\n\"fuzziness\": \"2\",\r\n\"operator\":\"and\"\r\n}\r\n}\r\n},\r\n\"size\":30000,\r\n \"ryft_enabled\": true}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }


    /**
     * Bool-match-must-query: Fuzziness 1 Looking for: Casillo and company: ATMICA
     * original : ATOMICA, Castillo
     */
    @Test
    public void testBoolMatchMust() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("company", "ATMICA")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("lastName", "Casillo")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().must(builderSpeaker).must(builderText);

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\"query\": {\"bool\": { \"must\": [{\"match\": {\"company\": {\"query\": \"ATMICA\",\"fuzziness\": 1,\"operator\": \"and\"}}},{\"match\": {\"lastName\": {\"query\": \"Casillo\",\"fuzziness\": 1,\"operator\": \"and\"}}}]}},\r\n\"size\":10000, \"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-must-query: Fuzziness 2 Looking for: Csillo and company: ATICA
     * original : ATOMICA, Castillo
     */
    @Test
    public void testBoolMatchMust2() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("company", "ATICA")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("lastName", "Csillo")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().must(builderSpeaker).must(builderText);

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\"query\": {\"bool\": { \"must\": [{\"match\": {\"company\": {\"query\": \"ATICA\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"lastName\": {\"query\": \"Csillo\",\"fuzziness\": 2,\"operator\": \"and\"}}}]}},\r\n  \"size\":20000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-must-query: Fuzziness 2 Looking for: 'Labors elt volutate' and company: OTHWAY
     * original : Laboris elit voluptate company: OTHERWAY
     */
    @Test
    public void testBoolMatchMust3() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("about", "Labors elt volutate")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("company", "OTHWAY")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().must(builderSpeaker).must(builderText);

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\"query\": {\"bool\": { \"must\": [{\"match\": {\"company\": {\"query\": \"OTHWAY\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"about\": {\"query\": \"Labors elt volutate\",\"fuzziness\": 2,\"operator\": \"and\"}}}]}},\r\n  \"size\":20000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-must-query: Fuzziness 2 Looking for: 'Labos el voluate' and company: OTHWAY
     * original : Laboris elit voluptate company: OTHERWAY
     */
    @Test
    public void testBoolMatchMust4() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("about", "Labos el voluate")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("company", "OTHWAY")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().must(builderSpeaker).must(builderText);

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\"query\": {\"bool\": { \"must\": [{\"match\": {\"company\": {\"query\": \"OTHWAY\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"about\": {\"query\": \"Labos el voluate\",\"fuzziness\": 2,\"operator\": \"and\"}}}]}},\r\n  \"size\":20000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-should-query: Fuzziness 1 Looking for: 'Offici fugia dolor commod' OR 'Lore sin incididnt'
     * original : Officia fugiat dolore commodo , Lorem sint incididunt
     */
    @Test
    public void testBoolMatchShould() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("about", "Offici fugia dolor commod")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("about", "Lore sin incididnt")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().should(builderSpeaker).should(builderText).minimumShouldMatch("1");

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\"query\": {\"bool\": { \"should\": [{\"match\": {\"about\": {\"query\": \"Offici fugia dolor commod\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"about\": {\"query\": \"Lore sin incididnt\",\"fuzziness\": 1,\"operator\": \"and\"}}}]}},\r\n  \"size\":20000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-should-query: Fuzziness 2 Looking for: 'Offic ugia olor ommod' OR 'ore si ncididnt'
     * original : Officia fugiat dolore commodo , Lorem sint incididunt
     */
    @Test
    public void testBoolMatchShould2() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("about", "Offic ugia olor ommod")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("about", "ore si ncididnt")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().should(builderSpeaker).should(builderText).minimumShouldMatch("1");

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\"query\": {\"bool\": { \"should\": [{\"match\": {\"about\": {\"query\": \"Offic ugia olor ommod\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"about\": {\"query\": \"ore si ncididnt\",\"fuzziness\": 2,\"operator\": \"and\"}}}]}},\r\n  \"size\":30000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-should-query: Fuzziness 2 Looking for: 'green' and first name 'Petra' or for 'green' and first name 'Hayden'
     * Used minimum_should match parameter and type:'phrase'
     */
    @Test
    public void testBoolMatchShould3() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("eyeColor", "gren")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("firstName", "Pera")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        MatchQueryBuilder builder3 = QueryBuilders
                .matchQuery("firstName", "Hyden")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        //"A horse! a horse! my kingdom for a horse!"

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().should(builderSpeaker).should(builderText).should(builder3).minimumShouldMatch("2");

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\"query\": {\"bool\": { \"should\": [{\"match\": {\"eyeColor\": {\"query\": \"gren\",\"fuzziness\": 2,\"type\":\"phrase\",\"operator\": \"and\"}}}," +
                "{\"match\": {\"firstName\": {\"query\": \"Pera\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"firstName\": {\"query\": \"Hyden\",\"fuzziness\": 2,\"operator\": \"and\"}}} ], \"minimum_should_match\":2 }},\r\n  \"size\":30000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }


    /**
     * Bool-match-should-query: Fuzziness 2 Looking for: 'green' and firstName 'Petra' or for 'green' and first name not 'Hayden'
     * Used minimum_should match parameter and type:'phrase' AND MUST NOT:
     */
    @Test
    public void testBoolMatchShouldMustNot4() throws InterruptedException, ExecutionException {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("eyeColor", "gren")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("firstName", "Pera")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        MatchQueryBuilder builder3 = QueryBuilders
                .matchQuery("firstName", "Hyden")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().must(builderSpeaker).must(builderText).mustNot(builder3).minimumShouldMatch("1");

        logger.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\"query\": {\"bool\": { \"must_not\":[{ \"match\":{\"firstName\":{\"query\":\"Hyden\",\"type\":\"phrase\"}}}], " +
                "\"must\": [{\"match\": {\"eyeColor\": {\"query\": \"gren\",\"fuzziness\": 1,\"type\":\"phrase\",\"operator\": \"and\"}}}," +
                "{\"match\": {\"firstName\": {\"query\": \"Pera\",\"fuzziness\": 1,\"operator\": \"and\"}}} ], \"minimum_should_match\":1 }},\r\n  \"size\":30000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testWildcardMatch() throws InterruptedException, ExecutionException {
        WildcardQueryBuilder builder = QueryBuilders.wildcardQuery("lastName", "Und?rwood");
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"match_phrase\": {\n" +
                "      \"lastName\": \"Und\\\"?\\\"rwood\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"ryft_enabled\":true\n" +
                "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testRawTextSearch() throws InterruptedException, ExecutionException {
        String ryftQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"match_phrase\": {\n" +
                "      \"_all\": {\n" +
                "        \"query\": \"BIFLEX\",\n" +
                "        \"fuzziness\": 1,\n" +
                "        \"width\": \"line\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"ryft\": {\n" +
                "    \"enabled\": true,\n" +
                "    \"files\": [\"/nodes/0/indices/integration/1/index/*.integrationjsonfld\"],\n" +
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
        TermQueryBuilder builder = QueryBuilders.termQuery("registered", "2014-01-01 07:00:00");
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"registered\": {\n" +
                "        \"value\": \"2014-01-01 07:00:00\",\n" +
                "        \"type\": \"datetime\",\n" +
                "        \"format\": \"yyyy-MM-dd HH:mm:ss\"\n" +
                "      }\n" +
                "    }\n" +
                "  }, \n" +
                "\"ryft_enabled\": true\n" +
                "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testDatetimeRange() throws InterruptedException, ExecutionException {
        RangeQueryBuilder builder = QueryBuilders.rangeQuery("registered").gt("2014-01-01 07:00:00").lt("2014-01-07 07:00:00");
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"range\" : {\n" +
                "      \"registered\" : {\n" +
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
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testNumericTerm() throws InterruptedException, ExecutionException {
        TermQueryBuilder builder = QueryBuilders.termQuery("age", 22);
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

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
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testNumericRange() throws InterruptedException, ExecutionException {
        RangeQueryBuilder builder = QueryBuilders.rangeQuery("age").gt(22).lt(29);
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

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
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testCurrencyTerm() throws InterruptedException, ExecutionException {
        QueryStringQueryBuilder builder = QueryBuilders.queryStringQuery("$1,158.96").field("balance");
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"balance\": {\n" +
                "        \"value\": \"$1,158.96\",\n" +
                "        \"type\": \"currency\", \n" +
                "        \"currency\": \"$\"\n" +
                "      }\n" +
                "    }\n" +
                "  }, \n" +
                "\"ryft_enabled\": true\n" +
                "}\n";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testIpv4Term() throws InterruptedException, ExecutionException {
        QueryStringQueryBuilder builder = QueryBuilders.queryStringQuery("122.176.86.200").field("ipv4");
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"ipv4\": {\n" +
                "        \"value\": \"122.176.86.200\",\n" +
                "        \"type\": \"ipv4\"\n" +
                "      }\n" +
                "    }\n" +
                "  }, \n" +
                "\"ryft_enabled\": true\n" +
                "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testIpv6Term() throws InterruptedException, ExecutionException {
        QueryBuilder builder = QueryBuilders.matchPhraseQuery("ipv6", "21DA:D3:0:2F3B:2AA:FF:FE28:9C5A");
        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        logger.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"ipv6\": {\n" +
                "        \"value\": \"21DA:D3:0:2F3B:2AA:FF:FE28:9C5A\",\n" +
                "        \"type\": \"ipv6\"\n" +
                "      }\n" +
                "    }\n" +
                "  }, \n" +
                "\"ryft_enabled\": true\n" +
                "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        logger.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
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
        assertTrue(ryftResponse.getHits().getTotalHits() >= searchResponse.getHits().getTotalHits());

        SearchHit[] elasticHits = searchResponse.getHits().getHits();
        Map<String, SearchHit> hitMap = new HashMap<>();
        for (SearchHit elasticHit : elasticHits) {
            hitMap.put(elasticHit.getId(), elasticHit);
        }

        SearchHit[] ryftHits = ryftResponse.getHits().getHits();
        for (SearchHit ryftHit : ryftHits) {
            hitMap.remove(ryftHit.getId());
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
