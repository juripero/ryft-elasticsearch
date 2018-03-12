package com.ryft.elasticsearch.integration.test;

import com.google.common.collect.Lists;
import com.ryft.elasticsearch.integration.test.client.handler.FilesApi;
import com.ryft.elasticsearch.integration.test.client.invoker.ApiClient;
import com.ryft.elasticsearch.integration.test.client.invoker.ApiException;
import com.ryft.elasticsearch.integration.test.entity.TestData;
import com.ryft.elasticsearch.integration.test.util.DataGenerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class NonIndexedSearchTest extends RyftElasticTestCase {

    private static String testFilename;
    private static FilesApi filesApi;

    @BeforeClass
    static void prepareData() throws IOException, ApiException {
        testFilename = String.format("%s.json", indexName);
        byte[] testDataBytes = testDataStringsList.stream()
                .collect(Collectors.joining("\n")).getBytes();
        createFilesApi();
        try {
            filesApi.deleteFiles(null, Lists.newArrayList(testFilename), null, true);
        } catch (ApiException ex) {
        }
        filesApi.postRawFile(testDataBytes, testFilename, null, null, null, Long.valueOf(testDataBytes.length), null, "wait-10s", true);
    }

    @AfterClass
    static void cleanUp() throws ApiException {
        if (deleteIndex) {
            filesApi.deleteFiles(null, Lists.newArrayList(testFilename), null, true);
        }
    }

    private static void createFilesApi() {
        ApiClient apiClient = new ApiClient();
        apiClient.setApiKeyPrefix("Bearer");
        apiClient.setBasePath(String.format("http://%s:%d", transportAddresses[0].getAddress(), 8765));
        filesApi = new FilesApi(apiClient);
    }

    private void severalFiles(Consumer<Map<String, String>> testFunction) throws Exception {
        Map<String, String> fileContentsMap = Lists.partition(testDataStringsList, recordsNum / 10).stream()
                .map(list -> list.stream().collect(Collectors.joining("\n")))
                .collect(Collectors.toMap(d -> DataGenerator.DATA_FACTORY.getNumberText(8) + ".json", f -> f));
        for (Map.Entry<String, String> entry : fileContentsMap.entrySet()) {
            filesApi.postRawFile(entry.getValue().getBytes(), entry.getKey(), null, null, null, (long) entry.getValue().getBytes().length, null, "wait-10s", true);
        }
        Thread.sleep(1000L);
        try {
            testFunction.accept(fileContentsMap);
        } finally {
            if (deleteIndex) {
                filesApi.deleteFiles(null, new ArrayList(fileContentsMap.keySet()), null, true);
            }
        }
    }

    public void testRawTextSearch(Collection<String> files) {
        try {
            String query = "perspiciatis";
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
                    + "    \"files\": [" + files.stream().collect(Collectors.joining("\",\"", "\"", "\"")) + "],\n"
                    + "    \"format\": \"utf8\"\n"
                    + "  }\n"
                    + "}\n";
            SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                    new SearchRequest(files.toArray(new String[files.size()]), ryftQuery.getBytes())).get();
            assertResponse(ryftResponse);
            int expected = testDataStringsList.stream()
                    .collect(Collectors.joining("\n")).split(query, -1).length - 1;
            assertEquals(expected, ryftResponse.getHits().getHits().length);
        } catch (InterruptedException | ExecutionException | RuntimeException e) {
            assumeNoException("", e);
        }
    }

    private void testRecordSearch(Collection<String> files) {
        try {
            String query = "perspiciatis";
            String ryftQuery = "{\n"
                    + "  \"query\": {\n"
                    + "    \"match_phrase\": {\n"
                    + "      \"about\": {\n"
                    + "        \"query\": \"" + query + "\"\n"
                    + "      }\n"
                    + "    }\n"
                    + "  },\n"
                    + "  \"ryft\": {\n"
                    + "    \"enabled\": true,\n"
                    + "    \"files\": [" + files.stream().collect(Collectors.joining("\",\"", "\"", "\"")) + "],\n"
                    + "    \"format\": \"json\"\n"
                    + "  }\n"
                    + "}\n";
            SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                    new SearchRequest(files.toArray(new String[files.size()]), ryftQuery.getBytes())).get();
            assertResponse(ryftResponse);
            List<TestData> expectedList = testDataList.stream()
                    .filter(testData -> testData.getAbout().contains(query))
                    .collect(Collectors.toList());
            List<TestData> actualList = Stream.of(ryftResponse.getHits().getHits())
                    .map(hit -> {
                        try {
                            return TestData.fromJson(hit.source());
                        } catch (IOException ex) {
                            return null;
                        }
                    }).filter(Objects::nonNull).collect(Collectors.toList());
            assertTrue(actualList.containsAll(expectedList));
        } catch (InterruptedException | ExecutionException | RuntimeException e) {
            assumeNoException("", e);
        }
    }

    @Test
    public void testRawTextSearchOneFile() throws Exception {
        testRawTextSearch(Lists.newArrayList(testFilename));
    }

    @Test
    public void testRawTextSearchSeveralFiles() throws Exception {
        severalFiles(fileContentsMap -> testRawTextSearch(fileContentsMap.keySet()));
    }

    @Test
    public void testRecordSearchOneFile() throws Exception {
        testRecordSearch(Lists.newArrayList(testFilename));
    }

    @Test
    public void testRecordSearchSeveralFiles() throws Exception {
        severalFiles(fileContentsMap -> testRecordSearch(fileContentsMap.keySet()));
    }

    public void testAggregation(Collection<String> files) {
        try {
            String aggName = "1";
            String aggCount = "countAgg";
            String query = "perspiciatis";
            String ryftQuery = "{\n"
                    + "  \"query\": {\n"
                    + "    \"match_phrase\": {\n"
                    + "      \"about\": {\n"
                    + "        \"query\": \"" + query + "\"\n"
                    + "      }\n"
                    + "    }\n"
                    + "  },"
                    + "  \"aggs\": {\n"
                    + "    \"" + aggName + "\": {\n"
                    + "      \"sum\": {\n"
                    + "        \"field\": \"balance\"\n"
                    + "      }\n"
                    + "    },\n"
                    + "    \"" + aggCount + "\": {\n"
                    + "      \"value_count\": {\n"
                    + "        \"field\": \"balance\"\n"
                    + "      }\n"
                    + "    }\n"
                    + "  },\n"
                    + "  \"ryft\": {\n"
                    + "    \"enabled\": true,\n"
                    + "    \"files\": [" + files.stream().collect(Collectors.joining("\",\"", "\"", "\"")) + "],\n"
                    + "    \"format\": \"json\"\n"
                    + "  }\n"
                    + "}\n";
            SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                    new SearchRequest(files.toArray(new String[files.size()]), ryftQuery.getBytes())).get();
            assertResponse(ryftResponse);
            Sum aggregation = (Sum) ryftResponse.getAggregations().get(aggName);
            ValueCount countAggregation = (ValueCount) ryftResponse.getAggregations().get(aggCount);
            Long actualCount = countAggregation.getValue();
            Long expectedCount = testDataList.stream()
                    .filter(testData -> testData.getAbout().contains(query))
                    .mapToDouble(testData -> testData.getBalance())
                    .count();
            assertEquals("Aggregations should have the same amount of input entities", expectedCount, actualCount);
            Double actual = aggregation.getValue();
            Double expected = testDataList.stream()
                    .filter(testData -> testData.getAbout().contains(query))
                    .mapToDouble(testData -> testData.getBalance())
                    .reduce((b1, b2) -> b1 + b2).getAsDouble();
            assertEquals(expected, actual, 1e-6);
        } catch (InterruptedException | ExecutionException | RuntimeException e) {
            assumeNoException("", e);
        }
    }

    @Test
    public void testAggregation() throws Exception {
        testAggregation(Lists.newArrayList(testFilename));
    }

    @Test
    public void testAggregationSeveralFiles() throws Exception {
        severalFiles(fileContentsMap -> testAggregation(fileContentsMap.keySet()));
    }

    @Test
    public void testAggregationWithMetadata() throws Exception {
        String aggName = "1";
        String query = "perspiciatis";
        String ryftQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"match_phrase\": {\n"
                + "      \"about\": {\n"
                + "        \"query\": \"" + query + "\"\n"
                + "      }\n"
                + "    }\n"
                + "  },"
                + "  \"aggs\": {\n"
                + "    \"" + aggName + "\": {\n"
                + "      \"sum\": {\n"
                + "        \"field\": \"balance\"\n"
                + "      },\n"
                + "      \"meta\": {\n"
                + "        \"testkey\": \"testvalue\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft\": {\n"
                + "    \"enabled\": true,\n"
                + "    \"files\": [\"" + testFilename + "\"],\n"
                + "    \"format\": \"json\"\n"
                + "  }\n"
                + "}\n";
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{testFilename}, ryftQuery.getBytes())).get();
        assertResponse(ryftResponse);
        Map<String, Object> metadata = ryftResponse.getAggregations().get(aggName).getMetaData();
        assertTrue(metadata.containsKey("testkey"));
        assertEquals(metadata.get("testkey"), "testvalue");
    }

    @Test
    public void testAggregationWithMapping1() throws Exception {
        String aggName = "1";
        String query = "perspiciatis";
        String ryftQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"match_phrase\": {\n"
                + "      \"about\": {\n"
                + "        \"query\": \"" + query + "\"\n"
                + "      }\n"
                + "    }\n"
                + "  },"
                + "  \"aggs\": {\n"
                + "    \"" + aggName + "\": {\n"
                + "      \"date_histogram\": {\n"
                + "        \"field\": \"registered\",\n"
                + "        \"interval\": \"1y\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft\": {\n"
                + "    \"enabled\": true,\n"
                + "    \"files\": [\"" + testFilename + "\"],\n"
                + "    \"format\": \"json\",\n"
                + "    \"mapping\": {\n"
                + "      \"registered\": \"type=date,format=yyyy-MM-dd HH:mm:ss\",\n"
                + "      \"location\": \"type=geo_point\"\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{testFilename}, ryftQuery.getBytes())).get();
        assertResponse(ryftResponse);
        InternalHistogram<InternalHistogram.Bucket> actualAggregation = (InternalHistogram) ryftResponse.getAggregations().get(aggName);
        InternalHistogram<InternalHistogram.Bucket> expectedAggregation;
        String tmpIndexName = "test" + DataGenerator.DATA_FACTORY.getNumberText(8);
        try {
            createIndex(tmpIndexName, "test", testDataStringsList,
                    "registered", "type=date,format=yyyy-MM-dd HH:mm:ss");
            QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("about", query);
            AbstractAggregationBuilder aggregationBuilder = AggregationBuilders
                    .dateHistogram(aggName).field("registered").interval(DateHistogramInterval.YEAR);
            SearchResponse searchResponse = getClient().prepareSearch(tmpIndexName).setQuery(queryBuilder)
                    .addAggregation(aggregationBuilder).get();
            expectedAggregation = (InternalHistogram) searchResponse.getAggregations().get(aggName);
        } finally {
            deleteIndex(tmpIndexName);
        }
        assertEquals("Histograms should have same buckets size", expectedAggregation.getBuckets().size(), actualAggregation.getBuckets().size());
        for (int i = 0; i < expectedAggregation.getBuckets().size(); i++) {
            InternalHistogram.Bucket expectedBucket = expectedAggregation.getBuckets().get(i);
            InternalHistogram.Bucket actualBucket = actualAggregation.getBuckets().get(i);
            Date expectedKey = (expectedBucket.getKey() instanceof DateTime) ? ((DateTime) expectedBucket.getKey()).toDate() : new Date((Long) expectedBucket.getKey());
            Date actualKey = (actualBucket.getKey() instanceof DateTime) ? ((DateTime) actualBucket.getKey()).toDate() : new Date((Long) actualBucket.getKey());
            assertEquals(expectedKey, actualKey);
            assertEquals(expectedBucket.getDocCount(), actualBucket.getDocCount());
        }
    }

    @Test
    public void testAggregationWithMapping2() throws Exception {
        String aggName = "1";
        String query = "perspiciatis";
        String ryftQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"match_phrase\": {\n"
                + "      \"about\": {\n"
                + "        \"query\": \"" + query + "\"\n"
                + "      }\n"
                + "    }\n"
                + "  },"
                + "  \"aggs\": {\n"
                + "    \"" + aggName + "\": {\n"
                + "      \"geo_bounds\": {\n"
                + "        \"field\": \"location\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft\": {\n"
                + "    \"enabled\": true,\n"
                + "    \"files\": [\"" + testFilename + "\"],\n"
                + "    \"format\": \"json\",\n"
                + "    \"mapping\": {\n"
                + "      \"location\": {\n"
                + "        \"type\": \"geo_point\"\n"
                + "      },"
                + "      \"registered\": {\n"
                + "        \"type\": \"date\",\n"
                + "        \"format\": \"yyyy-MM-dd HH:mm:ss\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{testFilename}, ryftQuery.getBytes())).get();
        assertResponse(ryftResponse);
        GeoBounds actualAggregation = (GeoBounds) ryftResponse.getAggregations().get(aggName);
        GeoBounds expectedAggregation;
        String tmpIndexName = "test" + DataGenerator.DATA_FACTORY.getNumberText(8);
        try {
            createIndex(tmpIndexName, "test", testDataStringsList,
                    "location", "type=geo_point");
            QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("about", query);
            AbstractAggregationBuilder aggregationBuilder = AggregationBuilders
                    .geoBounds(aggName).field("location");
            SearchResponse searchResponse = getClient().prepareSearch(tmpIndexName).setQuery(queryBuilder)
                    .addAggregation(aggregationBuilder).get();
            expectedAggregation = (GeoBounds) searchResponse.getAggregations().get(aggName);
        } finally {
            deleteIndex(tmpIndexName);
        }
        assertEquals(expectedAggregation.topLeft().getLat(), actualAggregation.topLeft().getLat(), 1e-6);
        assertEquals(expectedAggregation.topLeft().getLon(), actualAggregation.topLeft().getLon(), 1e-6);
        assertEquals(expectedAggregation.bottomRight().getLat(), actualAggregation.bottomRight().getLat(), 1e-6);
        assertEquals(expectedAggregation.bottomRight().getLon(), actualAggregation.bottomRight().getLon(), 1e-6);
    }

    @Test
    public void testAggregationWithSizeZero() throws Exception {
        String aggName = "1";
        String query = "perspiciatis";
        String ryftQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"match_phrase\": {\n"
                + "      \"about\": {\n"
                + "        \"query\": \"" + query + "\"\n"
                + "      }\n"
                + "    }\n"
                + "  },"
                + "  \"aggs\": {\n"
                + "    \"" + aggName + "\": {\n"
                + "      \"sum\": {\n"
                + "        \"field\": \"balance\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft\": {\n"
                + "    \"enabled\": true,\n"
                + "    \"files\": [\"" + testFilename + "\"],\n"
                + "    \"format\": \"json\"\n"
                + "  },"
                + "  \"size\": 0\n"
                + "}\n";
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{testFilename}, ryftQuery.getBytes())).get();
        assertResponse(ryftResponse);
        long actualTotalHits = testDataList.stream().filter(td -> td.getAbout().contains(query)).count();
        assertEquals(actualTotalHits, ryftResponse.getHits().getTotalHits());
        assertEquals(0, ryftResponse.getHits().hits().length);
    }

    @Test
    @Ignore
    public void testCSVSearch() throws Exception {
        String fileContents = testDataList.stream().map(testdata -> testdata.toCsv())
                .collect(Collectors.joining("\n", "", "\n"));
        String fileName = "test/" + indexName + ".csv";
        filesApi.postRawFile(fileContents.getBytes(), fileName, null, null, null,
                Long.valueOf(fileContents.length()), null, "wait-10s", true);
        try {
            String aggName = "1";
            String query = "perspiciatis";
            String ryftQuery = "{\n"
                    + "  \"query\": {\n"
                    + "    \"match\": {\n"
                    + "      \"4\": {\n"
                    + "        \"query\": \"" + query + "\"\n"
                    + "      }\n"
                    + "    }\n"
                    + "  },\n"
                    + "  \"ryft\": {\n"
                    + "    \"enabled\": true,\n"
                    + "    \"files\": [\"" + fileName + "\"],\n"
                    + "    \"format\": \"csv\"\n"
                    + "  },"
                    + "  \"size\": 0\n"
                    + "}\n";
            SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                    new SearchRequest(new String[]{fileName}, ryftQuery.getBytes())).get();
            assertResponse(ryftResponse);
            long actualTotalHits = testDataList.stream().filter(td -> td.getAbout().contains(query)).count();
            assertEquals(actualTotalHits, ryftResponse.getHits().getTotalHits());
            assertEquals(0, ryftResponse.getHits().hits().length);
        } finally {
            if (deleteIndex) {
                filesApi.deleteFiles(null, Lists.newArrayList(fileName), null, true);
            }
        }
    }
}
