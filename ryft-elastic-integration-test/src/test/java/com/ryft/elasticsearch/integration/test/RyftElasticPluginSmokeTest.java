package com.ryft.elasticsearch.integration.test;

import static org.hamcrest.Matchers.greaterThan;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.nio.file.Files;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.unit.Fuzziness;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RyftElasticPluginSmokeTest extends ESSmokeClientTestCase {

    // index field from super will be deleted after test
    private static final String INDEX_NAME = "integration";

    private static ObjectMapper mapper;
    private static String testDataContent;
    private static ArrayList<TestData> testData;

    private Client client;

    @BeforeClass
    public static void prepareData() throws IOException {
        mapper = new ObjectMapper();
        ClassLoader classLoader = RyftElasticPluginSmokeTest.class.getClassLoader();
        File file = new File(classLoader.getResource("dataset.json").getFile());
        testDataContent = new String(Files.readAllBytes(file.toPath()));
        testData = mapper.readValue(testDataContent, new TypeReference<List<TestData>>() {
        });
    }

    @Before
    public void before() {
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
            LOGGER.info("Creating index {}", INDEX_NAME);
            client.admin().indices().prepareCreate(INDEX_NAME).get();

            client.admin().indices().preparePutMapping(INDEX_NAME).setType("data").setSource("{\n"
                    + "    \"data\" : {\n"
                    + "        \"properties\" : {\n"
                    + "            \"registered\" : {\"type\" : \"date\", \"format\" : \"yyyy-MM-dd HH:mm:ss\"}\n"
                    + "        }\n"
                    + "    }\n"
                    + "}").get();

            BulkRequestBuilder bulkRequest = client.prepareBulk();

            testData.forEach(data -> {
                String json = "";
                try {
                    json = mapper.writeValueAsString(data);
                } catch (JsonProcessingException e) {
                    LOGGER.error(e.toString());
                }
                bulkRequest.add(client.prepareIndex(INDEX_NAME, "data", data.getId())
                        .setSource(json));
            });
            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                LOGGER.error(bulkResponse.buildFailureMessage());
            } else {
                LOGGER.info("Bulk indexing succeeded.");
            }
        }
        client.admin().indices().prepareRefresh(INDEX_NAME).get();
        client.admin().cluster().prepareUpdateSettings().setTransientSettings(
                ImmutableMap.of("script.engine.groovy.inline.aggs", "true")).get();
    }

    @AfterClass
    public static void afterClass() {
        LOGGER.info("Deleting created indices");
        getClient().admin().indices().prepareDelete(INDEX_NAME).get();
    }

    /**
     * match-phrase-query: Fuzziness 1 looking for about: Esse ipsum et laborum
     * labore original phrase: Esse ipsum et laborum labore
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testSimpleFuzzyMatch() throws Exception {
        MatchQueryBuilder builder = QueryBuilders.matchPhraseQuery("about", "Esse ipsum et laborum labore")
                .fuzziness(Fuzziness.ONE)
                .operator(Operator.AND);
        LOGGER.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String elasticQuery = "{" + "\"query\": {" + "\"match_phrase\": {" + "\"about\":{"
                + "\"query\":\"Esse ipsum et laborum labore\"," + "\"fuzziness\":\"1\"" + "}" + "}" + "},"
                + "\"ryft_enabled\":true" + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * match-phrase-query: Fuzziness 1 looking for about: Esse ipsum et laborum
     * labore original phrase: Esse ipsum et laborum labore
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testSimpleFuzzyMatch2() throws Exception {
        MatchQueryBuilder builder = QueryBuilders.matchPhraseQuery("about", "Esse ipsum et laborum labore")
                .fuzziness(Fuzziness.AUTO)
                .operator(Operator.AND);
        LOGGER.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String elasticQuery = "{" + "\"query\": {" + "\"match_phrase\": {" + "\"about\":{"
                + "\"query\":\"Esse ipsum et laborum labore\"," + "\"fuzziness\":\"2\"" + "}" + "}" + "},"
                + "\"ryft_enabled\":true" + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * fuzzy-query: Fuzziness 1 looking for first name: pitra original first
     * name: Petra
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testSimpleFuzzyQuery() throws Exception {
        FuzzyQueryBuilder builder = QueryBuilders.fuzzyQuery("firstName", "pitra")
                .fuzziness(Fuzziness.ONE);
        LOGGER.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String elasticQuery = "{\"query\": {\"fuzzy\": {\"firstName\": "
                + "{\"value\": \"pitra\", \"fuzziness\": 1}}}, \"ryft_enabled\":true }";

        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * fuzzy-query: Fuzziness 2 looking for first name: pira original first
     * name: Petra
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testSimpleFuzzyQuery2() throws Exception {
        FuzzyQueryBuilder builder = QueryBuilders.fuzzyQuery("firstName", "pira")
                .fuzziness(Fuzziness.TWO);
        LOGGER.info("Testing query: {}", builder.toString());

        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setSize(total).setFrom(0)
                .get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String elasticQuery = "{\"query\": {\"fuzzy\": {\"firstName\": "
                + "{\"value\": \"pira\", \"fuzziness\": 2}}}, \"ryft_enabled\":true }";

        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Match-query: Fuzziness 1 Looking for: Esse ipum original : Esse ipsum
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testMatchMust() throws Exception {
        MatchQueryBuilder builder = QueryBuilders
                .matchQuery("about", "Esse ipum")
                .fuzziness(Fuzziness.ONE).operator(Operator.AND).fuzziness(Fuzziness.ONE);

        LOGGER.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\r\n\"query\": {\r\n\"match\" : {\r\n \"about\": {\r\n\"query\":\"Esse ipum\",\r\n\"fuzziness\": \"1\",\r\n\"operator\":\"and\"\r\n}\r\n}\r\n},\r\n \"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Match-query: Fuzziness 2 Looking for: Esse pum original : Esse ipsum
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testMatchMust2() throws Exception {
        MatchQueryBuilder builder = QueryBuilders
                .matchQuery("about", "Esse pum")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        LOGGER.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\r\n\"query\": {\r\n\"match\" : {\r\n \"about\": {\r\n\"query\":\"Esse pum\",\r\n\"fuzziness\": \"2\",\r\n\"operator\":\"and\"\r\n}\r\n}\r\n},\r\n\"size\":30000,\r\n \"ryft_enabled\": true}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-must-query: Fuzziness 1 Looking for: Casillo and company:
     * ATMICA original : ATOMICA, Castillo
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testBoolMatchMust() throws Exception {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("company", "ATMICA")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("lastName", "Casillo")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().must(builderSpeaker).must(builderText);

        LOGGER.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\"query\": {\"bool\": { \"must\": [{\"match\": {\"company\": {\"query\": \"ATMICA\",\"fuzziness\": 1,\"operator\": \"and\"}}},{\"match\": {\"lastName\": {\"query\": \"Casillo\",\"fuzziness\": 1,\"operator\": \"and\"}}}]}},\r\n\"size\":10000, \"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-must-query: Fuzziness 2 Looking for: Csillo and company: ATICA
     * original : ATOMICA, Castillo
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testBoolMatchMust2() throws Exception {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("company", "ATICA")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("lastName", "Csillo")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().must(builderSpeaker).must(builderText);

        LOGGER.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\"query\": {\"bool\": { \"must\": [{\"match\": {\"company\": {\"query\": \"ATICA\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"lastName\": {\"query\": \"Csillo\",\"fuzziness\": 2,\"operator\": \"and\"}}}]}},\r\n  \"size\":20000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-must-query: Fuzziness 2 Looking for: 'Labors elt volutate' and
     * company: OTHWAY original : Laboris elit voluptate company: OTHERWAY
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testBoolMatchMust3() throws Exception {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("about", "Labors elt volutate")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("company", "OTHWAY")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().must(builderSpeaker).must(builderText);

        LOGGER.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\"query\": {\"bool\": { \"must\": [{\"match\": {\"company\": {\"query\": \"OTHWAY\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"about\": {\"query\": \"Labors elt volutate\",\"fuzziness\": 2,\"operator\": \"and\"}}}]}},\r\n  \"size\":20000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-must-query: Fuzziness 2 Looking for: 'Labos el voluate' and
     * company: OTHWAY original : Laboris elit voluptate company: OTHERWAY
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testBoolMatchMust4() throws Exception {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("about", "Labos el voluate")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("company", "OTHWAY")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().must(builderSpeaker).must(builderText);

        LOGGER.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\"query\": {\"bool\": { \"must\": [{\"match\": {\"company\": {\"query\": \"OTHWAY\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"about\": {\"query\": \"Labos el voluate\",\"fuzziness\": 2,\"operator\": \"and\"}}}]}},\r\n  \"size\":20000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-should-query: Fuzziness 1 Looking for: 'Offici fugia dolor
     * commod' OR 'Lore sin incididnt' original : Officia fugiat dolore commodo
     * , Lorem sint incididunt
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testBoolMatchShould() throws Exception {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("about", "Offici fugia dolor commod")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("about", "Lore sin incididnt")
                .operator(Operator.AND).fuzziness(Fuzziness.AUTO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().should(builderSpeaker).should(builderText).minimumShouldMatch("1");

        LOGGER.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\"query\": {\"bool\": { \"should\": [{\"match\": {\"about\": {\"query\": \"Offici fugia dolor commod\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"about\": {\"query\": \"Lore sin incididnt\",\"fuzziness\": 1,\"operator\": \"and\"}}}]}},\r\n  \"size\":20000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-should-query: Fuzziness 2 Looking for: 'Offic ugia olor ommod'
     * OR 'ore si ncididnt' original : Officia fugiat dolore commodo , Lorem
     * sint incididunt
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testBoolMatchShould2() throws Exception {
        MatchQueryBuilder builderSpeaker = QueryBuilders
                .matchQuery("about", "Offic ugia olor ommod")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        MatchQueryBuilder builderText = QueryBuilders
                .matchQuery("about", "ore si ncididnt")
                .operator(Operator.AND).fuzziness(Fuzziness.TWO);

        BoolQueryBuilder builder = QueryBuilders
                .boolQuery().should(builderSpeaker).should(builderText).minimumShouldMatch("1");

        LOGGER.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\"query\": {\"bool\": { \"should\": [{\"match\": {\"about\": {\"query\": \"Offic ugia olor ommod\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"about\": {\"query\": \"ore si ncididnt\",\"fuzziness\": 2,\"operator\": \"and\"}}}]}},\r\n  \"size\":30000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-should-query: Fuzziness 2 Looking for: 'green' and first name
     * 'Petra' or for 'green' and first name 'Hayden' Used minimum_should match
     * parameter and type:'phrase'
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testBoolMatchShould3() throws Exception {
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

        LOGGER.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\"query\": {\"bool\": { \"should\": [{\"match\": {\"eyeColor\": {\"query\": \"gren\",\"fuzziness\": 2,\"type\":\"phrase\",\"operator\": \"and\"}}},"
                + "{\"match\": {\"firstName\": {\"query\": \"Pera\",\"fuzziness\": 2,\"operator\": \"and\"}}},{\"match\": {\"firstName\": {\"query\": \"Hyden\",\"fuzziness\": 2,\"operator\": \"and\"}}} ], \"minimum_should_match\":2 }},\r\n  \"size\":30000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    /**
     * Bool-match-should-query: Fuzziness 2 Looking for: 'green' and firstName
     * 'Petra' or for 'green' and first name not 'Hayden' Used minimum_should
     * match parameter and type:'phrase' AND MUST NOT:
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testBoolMatchShouldMustNot4() throws Exception {
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

        LOGGER.info("Testing query: {}", builder.toString());
        int total = getSize(builder);
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).setFrom(0).setSize(total)
                .get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\"query\": {\"bool\": { \"must_not\":[{ \"match\":{\"firstName\":{\"query\":\"Hyden\",\"type\":\"phrase\"}}}], "
                + "\"must\": [{\"match\": {\"eyeColor\": {\"query\": \"gren\",\"fuzziness\": 1,\"type\":\"phrase\",\"operator\": \"and\"}}},"
                + "{\"match\": {\"firstName\": {\"query\": \"Pera\",\"fuzziness\": 1,\"operator\": \"and\"}}} ], \"minimum_should_match\":1 }},\r\n  \"size\":30000,\"ryft_enabled\": true\r\n}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testWildcardMatch() throws Exception {
        WildcardQueryBuilder builder = QueryBuilders.wildcardQuery("lastName", "Und?rwood");
        LOGGER.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"match_phrase\": {\n"
                + "      \"lastName\": \"Und\\\"?\\\"rwood\"\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft_enabled\":true\n"
                + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testRawTextSearch() throws Exception {
        ClusterState clusterState = client.admin().cluster().prepareState().setIndices(INDEX_NAME).get().getState();
        String file = String.format("/%1$s/nodes/0/indices/%2$s/*/index/_*.%2$sjsonfld",
                clusterState.getClusterName().value(), INDEX_NAME);
        String query = "green";
        String ryftQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"match_phrase\": {\n"
                + "      \"_all\": {\n"
                + "        \"query\": \"" + query + "\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft\": {\n"
                + "    \"enabled\": true,\n"
                + "    \"files\": [\"" + file + "\"],\n"
                + "    \"format\": \"utf8\"\n"
                + "  }\n"
                + "}\n";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        assertResponse(ryftResponse);
        int expected = testDataContent.split(query, -1).length - 1;
        assertEquals(expected, ryftResponse.getHits().getHits().length);
    }

    @Test
    public void testDatetimeTerm() throws Exception {
        TermQueryBuilder builder = QueryBuilders.termQuery("registered", "2014-01-01 07:00:00");
        LOGGER.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"registered\": {\n"
                + "        \"value\": \"2014-01-01 07:00:00\",\n"
                + "        \"type\": \"datetime\",\n"
                + "        \"format\": \"yyyy-MM-dd HH:mm:ss\"\n"
                + "      }\n"
                + "    }\n"
                + "  }, \n"
                + "\"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testDatetimeRange() throws Exception {
        RangeQueryBuilder builder = QueryBuilders.rangeQuery("registered").gt("2014-01-01 07:00:00").lt("2014-01-07 07:00:00");
        LOGGER.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"range\" : {\n"
                + "      \"registered\" : {\n"
                + "        \"gt\" : \"2014-01-01 07:00:00\",\n"
                + "        \"lt\" : \"2014-01-07 07:00:00\",\n"
                + "        \"type\": \"datetime\",\n"
                + "        \"format\": \"yyyy-MM-dd HH:mm:ss\"\n"
                + "      }\n"
                + "    }\n"
                + "  }, \n"
                + "\"ryft_enabled\": true\n"
                + "}\n";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testNumericTerm() throws Exception {
        TermQueryBuilder builder = QueryBuilders.termQuery("age", 22);
        LOGGER.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"age\": {\n"
                + "        \"value\": \"22\",\n"
                + "        \"type\": \"number\"\n"
                + "      }\n"
                + "    }\n"
                + "  }, \n"
                + "\"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testNumericRange() throws Exception {
        RangeQueryBuilder builder = QueryBuilders.rangeQuery("age").gt(22).lt(29);
        LOGGER.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"range\" : {\n"
                + "      \"age\" : {\n"
                + "        \"gt\" : \"22\",\n"
                + "        \"lt\" : \"29\",\n"
                + "        \"type\": \"number\"\n"
                + "      }\n"
                + "    }\n"
                + "  }, \n"
                + "\"ryft_enabled\": true\n"
                + "}\n";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testCurrencyTerm() throws Exception {
        QueryStringQueryBuilder builder = QueryBuilders.queryStringQuery("$1,158.96").field("balance");
        LOGGER.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"balance\": {\n"
                + "        \"value\": \"$1,158.96\",\n"
                + "        \"type\": \"currency\", \n"
                + "        \"currency\": \"$\"\n"
                + "      }\n"
                + "    }\n"
                + "  }, \n"
                + "\"ryft_enabled\": true\n"
                + "}\n";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testIpv4Term() throws Exception {
        QueryStringQueryBuilder builder = QueryBuilders.queryStringQuery("122.176.86.200").field("ipv4");
        LOGGER.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"ipv4\": {\n"
                + "        \"value\": \"122.176.86.200\",\n"
                + "        \"type\": \"ipv4\"\n"
                + "      }\n"
                + "    }\n"
                + "  }, \n"
                + "\"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testIpv6Term() throws Exception {
        QueryBuilder builder = QueryBuilders.matchPhraseQuery("ipv6", "21DA:D3:0:2F3B:2AA:FF:FE28:9C5A");
        LOGGER.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"ipv6\": {\n"
                + "        \"value\": \"21DA:D3:0:2F3B:2AA:FF:FE28:9C5A\",\n"
                + "        \"type\": \"ipv6\"\n"
                + "      }\n"
                + "    }\n"
                + "  }, \n"
                + "\"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testFilters() throws Exception {
        QueryBuilder builder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchPhraseQuery("ipv6", "21DA:D3:0:2F3B:2AA:FF:FE28:9C5A"))
                .must(QueryBuilders.rangeQuery("registered").format("epoch_millis").from(1339168100654L).to(1496934500654L));
        LOGGER.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(builder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());

        String ryftQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"filtered\": {\n"
                + "      \"query\": {\n"
                + "        \"query\": {\n"
                + "          \"term\": {\n"
                + "            \"ipv6\": {\n"
                + "              \"type\": \"ipv6\",\n"
                + "              \"value\": \"21DA:D3:0:2F3B:2AA:FF:FE28:9C5A\"\n"
                + "            }\n"
                + "          }\n"
                + "        },\n"
                + "        \"ryft_enabled\": true\n"
                + "      },\n"
                + "      \"filter\": {\n"
                + "        \"bool\": {\n"
                + "          \"must\": [\n"
                + "            {\n"
                + "              \"range\": {\n"
                + "                \"registered\": {\n"
                + "                  \"gte\": 1339168100654,\n"
                + "                  \"lte\": 1496934500654,\n"
                + "                  \"format\": \"epoch_millis\"\n"
                + "                }\n"
                + "              }\n"
                + "            }\n"
                + "          ],\n"
                + "          \"must_not\": []\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, ryftQuery.getBytes())).get();
        LOGGER.info("Ryft response has {} hits", ryftResponse.getHits().getTotalHits());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testDateHistogramAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders
                .dateHistogram(aggregationName).field("registered").interval(DateHistogramInterval.YEAR);
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(queryBuilder)
                .addAggregation(aggregationBuilder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());
        InternalHistogram<InternalHistogram.Bucket> aggregation = (InternalHistogram) searchResponse.getAggregations().get(aggregationName);
        aggregation.getBuckets().forEach((bucket) -> {
            LOGGER.info("{} -> {}", bucket.getKeyAsString(), bucket.getDocCount());
        });

        String elasticQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"match\": {\n"
                + "      \"eyeColor\": {\n"
                + "        \"query\": \"green\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"aggs\": {\n"
                + "    \"" + aggregationName + "\": {\n"
                + "      \"date_histogram\": {\n"
                + "        \"field\": \"registered\",\n"
                + "        \"interval\": \"1y\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        InternalHistogram<InternalHistogram.Bucket> ryftHistogram = (InternalHistogram) ryftResponse.getAggregations().asList().get(0);
        ryftHistogram.getBuckets().forEach((bucket) -> {
            LOGGER.info("{} -> {}", bucket.getKeyAsString(), bucket.getDocCount());
        });
        assertEquals("Histograms should have same buckets", aggregation.getBuckets(), aggregation.getBuckets());
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testMinAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders
                .min(aggregationName).field("age");
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(queryBuilder)
                .addAggregation(aggregationBuilder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());
        Min aggregation = (Min) searchResponse.getAggregations().get(aggregationName);
        LOGGER.info("ES min value: {}", aggregation.getValue());

        String elasticQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"match\": {\n"
                + "      \"eyeColor\": {\n"
                + "        \"query\": \"green\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"aggs\": {\n"
                + "    \"" + aggregationName + "\": {\n"
                + "      \"min\": {\n"
                + "        \"field\": \"age\""
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        Min ryftAggregation = (Min) ryftResponse.getAggregations().asList().get(0);
        LOGGER.info("RYFT min value: {}", ryftAggregation.getValue());

        assertEquals("Min values should be equal", aggregation.getValue(), ryftAggregation.getValue(), 1e-10);
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testMaxAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders
                .max(aggregationName).field("age");
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(queryBuilder)
                .addAggregation(aggregationBuilder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());
        Max aggregation = (Max) searchResponse.getAggregations().get(aggregationName);
        LOGGER.info("ES max value: {}", aggregation.getValue());

        String elasticQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"match\": {\n"
                + "      \"eyeColor\": {\n"
                + "        \"query\": \"green\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"aggs\": {\n"
                + "    \"" + aggregationName + "\": {\n"
                + "      \"max\": {\n"
                + "        \"field\": \"age\""
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        Max ryftAggregation = (Max) ryftResponse.getAggregations().asList().get(0);
        LOGGER.info("RYFT max value: {}", ryftAggregation.getValue());

        assertEquals("Max values should be equal", aggregation.getValue(), ryftAggregation.getValue(), 1e-10);
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testAvgAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        Script script = new Script("_value * correction", ScriptService.ScriptType.INLINE, "groovy", ImmutableMap.of("correction", 1.2));
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders.avg(aggregationName)
                .field("age").script(script);
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(queryBuilder)
                .addAggregation(aggregationBuilder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());
        Avg aggregation = (Avg) searchResponse.getAggregations().get(aggregationName);
        LOGGER.info("ES avg value: {}", aggregation.getValue());

        String elasticQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"match\": {\n"
                + "      \"eyeColor\": {\n"
                + "        \"query\": \"green\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"aggs\": {"
                + "    \"" + aggregationName + "\": {"
                + "      \"avg\": {"
                + "        \"field\": \"age\","
                + "        \"script\" : {\n"
                + "          \"lang\": \"groovy\",\n"
                + "          \"inline\": \"_value * correction\",\n"
                + "          \"params\" : {\n"
                + "            \"correction\" : 1.2\n"
                + "          }\n"
                + "        }"
                + "      }"
                + "    }"
                + "  },"
                + "  \"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        Avg ryftAggregation = (Avg) ryftResponse.getAggregations().asList().get(0);
        LOGGER.info("RYFT avg value: {}", ryftAggregation.getValue());

        assertEquals("Avg values should be equal", aggregation.getValue(), ryftAggregation.getValue(), 1e-10);
        elasticSubsetRyft(searchResponse, ryftResponse);
    }

    @Test
    public void testStatsAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders.stats(aggregationName)
                .field("age");
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(queryBuilder)
                .addAggregation(aggregationBuilder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());
        Stats aggregation = (Stats) searchResponse.getAggregations().get(aggregationName);
        LOGGER.info("ES stats: avg={}, count={}, max={}, min={}, sum={}",
                aggregation.getAvg(), aggregation.getCount(), aggregation.getMax(),
                aggregation.getMin(), aggregation.getSum());

        String elasticQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"match\": {\n"
                + "      \"eyeColor\": {\n"
                + "        \"query\": \"green\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"aggs\": {"
                + "    \"" + aggregationName + "\": {"
                + "      \"stats\": {"
                + "        \"field\": \"age\""
                + "      }"
                + "    }"
                + "  },"
                + "  \"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = client.execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{INDEX_NAME}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        Stats ryftAggregation = (Stats) ryftResponse.getAggregations().asList().get(0);
        LOGGER.info("RYFT stats: avg={}, count={}, max={}, min={}, sum={}",
                ryftAggregation.getAvg(), ryftAggregation.getCount(), ryftAggregation.getMax(),
                ryftAggregation.getMin(), ryftAggregation.getSum());

        assertEquals("Avg values should be equal", aggregation.getAvg(), ryftAggregation.getAvg(), 1e-10);
        assertEquals("Count values should be equal", aggregation.getCount(), ryftAggregation.getCount(), 1e-10);
        assertEquals("Max values should be equal", aggregation.getMax(), ryftAggregation.getMax(), 1e-10);
        assertEquals("Min values should be equal", aggregation.getMin(), ryftAggregation.getMin(), 1e-10);
        assertEquals("Sum values should be equal", aggregation.getSum(), ryftAggregation.getSum(), 1e-10);
    }

    public void ryftQuerySample() throws Exception {
        String elasticQuery = "{\"query\":{" + "\"match_phrase\": { " + "\"doc.text_entry\": {"
                + "\"query\":\"To be, or not to be\"," + "\"metric\": \"Fhs\"," + "\"fuzziness\": 5" + "}" + "}" + "}}";
        LOGGER.info("Testing query: {}", elasticQuery);
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
                LOGGER.info("Not in Ryft response {}", v.getSourceAsString());
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
