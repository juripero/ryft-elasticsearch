package com.ryft.elasticsearch.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import static com.ryft.elasticsearch.integration.test.ESSmokeClientTestCase.LOGGER;
import java.io.IOException;
import java.util.List;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.geocentroid.GeoCentroid;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesMethod;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AggregationTest extends ESSmokeClientTestCase {

    @BeforeClass
    static void prepareData() throws IOException {
        createIndex(indexName, "test", testDataStringsList,
                "registered", "type=date,format=yyyy-MM-dd HH:mm:ss",
                "location", "type=geo_point");
    }

    @AfterClass
    static void cleanUp() {
        cleanUp(indexName);
    }

    @Test
    public void testDateHistogramAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders
                .dateHistogram(aggregationName).field("registered").interval(DateHistogramInterval.YEAR);
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = getClient().prepareSearch(indexName).setQuery(queryBuilder)
                .addAggregation(aggregationBuilder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());
        InternalHistogram<InternalHistogram.Bucket> aggregation = searchResponse.getAggregations().get(aggregationName);
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
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{indexName}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        InternalHistogram<InternalHistogram.Bucket> ryftAggregation = (InternalHistogram) ryftResponse.getAggregations().asList().get(0);
        ryftAggregation.getBuckets().forEach((bucket) -> {
            LOGGER.info("{} -> {}", bucket.getKeyAsString(), bucket.getDocCount());
        });
        assertEquals("Histograms should have same buckets size", aggregation.getBuckets().size(), ryftAggregation.getBuckets().size());
        for (int i = 0; i < aggregation.getBuckets().size(); i++) {
            InternalHistogram.Bucket esBucket = aggregation.getBuckets().get(i);
            InternalHistogram.Bucket ryftBucket = ryftAggregation.getBuckets().get(i);
            assertEquals(esBucket.getKey(), ryftBucket.getKey());
            assertEquals(esBucket.getDocCount(), ryftBucket.getDocCount());
        }
    }

    @Test
    public void testMinAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders
                .min(aggregationName).field("age");
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = getClient().prepareSearch(indexName).setQuery(queryBuilder)
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
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{indexName}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        Min ryftAggregation = (Min) ryftResponse.getAggregations().asList().get(0);
        LOGGER.info("RYFT min value: {}", ryftAggregation.getValue());

        assertEquals("Min values should be equal", aggregation.getValue(), ryftAggregation.getValue(), 1e-10);
    }

    @Test
    public void testMaxAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders
                .max(aggregationName).field("age");
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = getClient().prepareSearch(indexName).setQuery(queryBuilder)
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
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{indexName}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        Max ryftAggregation = (Max) ryftResponse.getAggregations().asList().get(0);
        LOGGER.info("RYFT max value: {}", ryftAggregation.getValue());

        assertEquals("Max values should be equal", aggregation.getValue(), ryftAggregation.getValue(), 1e-10);
    }

    @Test
    public void testSumAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders
                .sum(aggregationName).field("age");
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = getClient().prepareSearch(indexName).setQuery(queryBuilder)
                .addAggregation(aggregationBuilder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());
        Sum aggregation = (Sum) searchResponse.getAggregations().get(aggregationName);
        LOGGER.info("ES sum value: {}", aggregation.getValue());

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
                + "      \"sum\": {\n"
                + "        \"field\": \"age\""
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{indexName}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        Sum ryftAggregation = (Sum) ryftResponse.getAggregations().asList().get(0);
        LOGGER.info("RYFT sum value: {}", ryftAggregation.getValue());

        assertEquals("Sum values should be equal", aggregation.getValue(), ryftAggregation.getValue(), 1e-10);
    }

    @Test
    public void testAvgAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders.avg(aggregationName)
                .field("age");
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = getClient().prepareSearch(indexName).setQuery(queryBuilder)
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
                + "  \"aggs\": {\n"
                + "    \"" + aggregationName + "\": {\n"
                + "      \"avg\": {\n"
                + "        \"field\": \"age\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{indexName}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        Avg ryftAggregation = (Avg) ryftResponse.getAggregations().asList().get(0);
        LOGGER.info("RYFT avg value: {}", ryftAggregation.getValue());

        assertEquals("Avg values should be equal", aggregation.getValue(), ryftAggregation.getValue(), 1e-10);
    }

    @Test
    public void testCountAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders.count(aggregationName)
                .field("age");
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = getClient().prepareSearch(indexName).setQuery(queryBuilder)
                .addAggregation(aggregationBuilder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());
        ValueCount aggregation = (ValueCount) searchResponse.getAggregations().get(aggregationName);
        LOGGER.info("ES count value: {}", aggregation.getValue());

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
                + "      \"value_count\": {"
                + "        \"field\": \"age\""
                + "      }"
                + "    }"
                + "  },"
                + "  \"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{indexName}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        ValueCount ryftAggregation = (ValueCount) ryftResponse.getAggregations().asList().get(0);
        LOGGER.info("RYFT count value: {}", ryftAggregation.getValue());

        assertEquals("Count values should be equal", aggregation.getValue(), ryftAggregation.getValue(), 1e-10);
    }

    @Test
    public void testStatsAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders.stats(aggregationName)
                .field("age");
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = getClient().prepareSearch(indexName).setQuery(queryBuilder)
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
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{indexName}, elasticQuery.getBytes())).get();
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

    @Test
    public void testExtStatsAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders.extendedStats(aggregationName)
                .field("age").sigma(3.1415926);
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = getClient().prepareSearch(indexName).setQuery(queryBuilder)
                .addAggregation(aggregationBuilder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());
        ExtendedStats aggregation = (ExtendedStats) searchResponse.getAggregations().get(aggregationName);
        LOGGER.info("ES extended stats: avg={}, count={}, max={}, min={}, sum={},\n"
                + "stddev={}, lower_stddev={}, upper_stddev={}, sqsum={}, variance={}",
                aggregation.getAvg(), aggregation.getCount(), aggregation.getMax(),
                aggregation.getMin(), aggregation.getSum(), aggregation.getStdDeviation(),
                aggregation.getStdDeviationBound(ExtendedStats.Bounds.LOWER),
                aggregation.getStdDeviationBound(ExtendedStats.Bounds.UPPER),
                aggregation.getSumOfSquares(), aggregation.getVariance());

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
                + "      \"extended_stats\": {"
                + "        \"field\": \"age\","
                + "        \"sigma\": 3.1415926"
                + "      }"
                + "    }"
                + "  },"
                + "  \"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{indexName}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        ExtendedStats ryftAggregation = (ExtendedStats) ryftResponse.getAggregations().asList().get(0);
        LOGGER.info("ES extended stats: avg={}, count={}, max={}, min={}, sum={},\n"
                + "stddev={}, lower_stddev={}, upper_stddev={}, sqsum={}, variance={}",
                aggregation.getAvg(), aggregation.getCount(), aggregation.getMax(),
                aggregation.getMin(), aggregation.getSum(), aggregation.getStdDeviation(),
                aggregation.getStdDeviationBound(ExtendedStats.Bounds.LOWER),
                aggregation.getStdDeviationBound(ExtendedStats.Bounds.UPPER),
                aggregation.getSumOfSquares(), aggregation.getVariance());

        assertEquals("avg values should be equal", aggregation.getAvg(), ryftAggregation.getAvg(), 1e-10);
        assertEquals("count values should be equal", aggregation.getCount(), ryftAggregation.getCount(), 1e-10);
        assertEquals("max values should be equal", aggregation.getMax(), ryftAggregation.getMax(), 1e-10);
        assertEquals("min values should be equal", aggregation.getMin(), ryftAggregation.getMin(), 1e-10);
        assertEquals("sum values should be equal", aggregation.getSum(), ryftAggregation.getSum(), 1e-10);
        assertEquals("stddev values should be equal", aggregation.getStdDeviation(), ryftAggregation.getStdDeviation(), 1e-10);
        assertEquals("lower_stddev values should be equal", aggregation.getStdDeviationBound(ExtendedStats.Bounds.LOWER),
                ryftAggregation.getStdDeviationBound(ExtendedStats.Bounds.LOWER), 1e-10);
        assertEquals("upper_stddev values should be equal", aggregation.getStdDeviationBound(ExtendedStats.Bounds.UPPER),
                ryftAggregation.getStdDeviationBound(ExtendedStats.Bounds.UPPER), 1e-10);
        assertEquals("sqsum values should be equal", aggregation.getSumOfSquares(), ryftAggregation.getSumOfSquares(), 1e-10);
        assertEquals("variance values should be equal", aggregation.getVariance(), ryftAggregation.getVariance(), 1e-10);
    }

    @Test
    public void testGeoBoundsAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders
                .geoBounds(aggregationName).field("location");
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = getClient().prepareSearch(indexName).setQuery(queryBuilder)
                .addAggregation(aggregationBuilder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());
        GeoBounds aggregation = (GeoBounds) searchResponse.getAggregations().get(aggregationName);
        LOGGER.info("ES top left: {}", aggregation.topLeft());
        LOGGER.info("ES bottom right: {}", aggregation.bottomRight());
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
                + "      \"geo_bounds\": {\n"
                + "        \"field\": \"location\""
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{indexName}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        GeoBounds ryftAggregation = (GeoBounds) ryftResponse.getAggregations().asList().get(0);
        LOGGER.info("RYFT top left: {}", ryftAggregation.topLeft());
        LOGGER.info("RYFT bottom right: {}", ryftAggregation.bottomRight());
        assertEquals(aggregation.topLeft().getLat(), ryftAggregation.topLeft().getLat(), 1e-10);
        assertEquals(aggregation.topLeft().getLon(), ryftAggregation.topLeft().getLon(), 1e-10);
        assertEquals(aggregation.bottomRight().getLat(), ryftAggregation.bottomRight().getLat(), 1e-10);
        assertEquals(aggregation.bottomRight().getLon(), ryftAggregation.bottomRight().getLon(), 1e-10);
    }

    @Test
    public void testGeoCentroidAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders
                .geoCentroid(aggregationName).field("location");
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = getClient().prepareSearch(indexName).setQuery(queryBuilder)
                .addAggregation(aggregationBuilder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());
        GeoCentroid aggregation = (GeoCentroid) searchResponse.getAggregations().get(aggregationName);
        LOGGER.info("ES centroid: {}", aggregation.centroid());
        LOGGER.info("ES count: {}", aggregation.count());
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
                + "      \"geo_centroid\": {\n"
                + "        \"field\": \"location\""
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{indexName}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        GeoCentroid ryftAggregation = (GeoCentroid) ryftResponse.getAggregations().asList().get(0);
        LOGGER.info("RYFT centroid: {}", ryftAggregation.centroid());
        LOGGER.info("RYFT count: {}", ryftAggregation.count());
        assertEquals(aggregation.centroid().getLat(), ryftAggregation.centroid().getLat(), 1e-5);
        assertEquals(aggregation.centroid().getLon(), ryftAggregation.centroid().getLon(), 1e-5);
        assertEquals(aggregation.count(), ryftAggregation.count(), 1e-10);
    }

    @Test
    public void testPercentileTDigestAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders
                .percentiles(aggregationName).field("age").percentiles(20, 40, 60, 80, 95)
                .method(PercentilesMethod.TDIGEST).compression(200.0);
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = getClient().prepareSearch(indexName).setQuery(queryBuilder)
                .addAggregation(aggregationBuilder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());
        List<Percentile> esPercentiles = Lists.newArrayList((Percentiles) searchResponse.getAggregations().get(aggregationName));
        esPercentiles.forEach((percentile) -> {
            LOGGER.info("percent: {}, value: {}", percentile.getPercent(), percentile.getValue());
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
                + "      \"percentiles\": {\n"
                + "        \"field\": \"age\","
                + "        \"percents\": [20, 40, 60, 80, 95],"
                + "        \"tdigest\": { \n"
                + "          \"compression\" : 200 \n"
                + "        }"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{indexName}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        List<Percentile> ryftPercentiles = Lists.newArrayList((Percentiles) ryftResponse.getAggregations().asList().get(0));
        ryftPercentiles.forEach((percentile) -> {
            LOGGER.info("percent: {}, value: {}", percentile.getPercent(), percentile.getValue());
        });
        assertEquals(esPercentiles.size(), ryftPercentiles.size());
        for (int i = 0; i < esPercentiles.size(); i++) {
            Percentile esPercentile = esPercentiles.get(i);
            Percentile ryftPercentile = ryftPercentiles.get(i);
            assertEquals(esPercentile.getPercent(), ryftPercentile.getPercent(), 1e-10);
            assertEquals(esPercentile.getValue(), ryftPercentile.getValue(), 1e-10);
        }
    }

    @Test
    public void testPercentileRanksAggregation() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders
                .percentileRanks(aggregationName).field("age").percentiles(20, 25, 30, 35, 40);
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = getClient().prepareSearch(indexName).setQuery(queryBuilder)
                .addAggregation(aggregationBuilder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());
        List<Percentile> esPercentiles = Lists.newArrayList((PercentileRanks) searchResponse.getAggregations().get(aggregationName));
        esPercentiles.forEach((percentile) -> {
            LOGGER.info("percent: {}, value: {}", percentile.getPercent(), percentile.getValue());
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
                + "      \"percentile_ranks\": {\n"
                + "        \"field\": \"age\","
                + "        \"values\": [20, 25, 30, 35, 40]"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{indexName}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        List<Percentile> ryftPercentiles = Lists.newArrayList((PercentileRanks) ryftResponse.getAggregations().asList().get(0));
        ryftPercentiles.forEach((percentile) -> {
            LOGGER.info("percent: {}, value: {}", percentile.getPercent(), percentile.getValue());
        });
        assertEquals(esPercentiles.size(), ryftPercentiles.size());
        for (int i = 0; i < esPercentiles.size(); i++) {
            Percentile esPercentile = esPercentiles.get(i);
            Percentile ryftPercentile = ryftPercentiles.get(i);
            assertEquals(esPercentile.getPercent(), ryftPercentile.getPercent(), 1e-10);
            assertEquals(esPercentile.getValue(), ryftPercentile.getValue(), 1e-10);
        }
    }

    @Test
    public void testSeveralAggregations() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder1 = AggregationBuilders
                .min("1").field("age");
        AbstractAggregationBuilder aggregationBuilder2 = AggregationBuilders
                .max("2").field("age");
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation1: {}", aggregationBuilder1.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());
        LOGGER.info("Testing aggregation2: {}", aggregationBuilder2.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = getClient().prepareSearch(indexName).setQuery(queryBuilder)
                .addAggregation(aggregationBuilder1).addAggregation(aggregationBuilder2).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());
        Min aggregation1 = (Min) searchResponse.getAggregations().get("1");
        Max aggregation2 = (Max) searchResponse.getAggregations().get("2");
        LOGGER.info("ES min value: {}", aggregation1.getValue());
        LOGGER.info("ES max value: {}", aggregation2.getValue());
        String elasticQuery = "{\n"
                + "  \"query\": {\n"
                + "    \"match\": {\n"
                + "      \"eyeColor\": {\n"
                + "        \"query\": \"green\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"aggs\": {\n"
                + "    \"1\": {\n"
                + "      \"min\": {\n"
                + "        \"field\": \"age\""
                + "      }\n"
                + "    },\n"
                + "    \"2\": {\n"
                + "      \"max\": {\n"
                + "        \"field\": \"age\""
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{indexName}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        Min ryftAggregation1 = (Min) ryftResponse.getAggregations().asMap().get("1");
        Max ryftAggregation2 = (Max) ryftResponse.getAggregations().asMap().get("2");
        LOGGER.info("RYFT min value: {}", ryftAggregation1.getValue());
        LOGGER.info("RYFT max value: {}", ryftAggregation2.getValue());
        assertEquals("Min values should be equal", aggregation1.getValue(), ryftAggregation1.getValue(), 1e-10);
        assertEquals("Max values should be equal", aggregation2.getValue(), ryftAggregation2.getValue(), 1e-10);
    }

    @Test
    public void testAggregationWithMetadata() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders.count(aggregationName)
                .field("age").setMetaData(ImmutableMap.of("color", "green"));
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = getClient().prepareSearch(indexName).setQuery(queryBuilder)
                .addAggregation(aggregationBuilder).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());
        ValueCount aggregation = (ValueCount) searchResponse.getAggregations().get(aggregationName);
        LOGGER.info("ES count value: {}", aggregation.getValue());

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
                + "      \"value_count\": {"
                + "        \"field\": \"age\""
                + "      },"
                + "      \"meta\": {"
                + "        \"color\": \"green\""
                + "      }"
                + "    }"
                + "  },"
                + "  \"ryft_enabled\": true\n"
                + "}";
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{indexName}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        ValueCount ryftAggregation = (ValueCount) ryftResponse.getAggregations().asList().get(0);
        LOGGER.info("RYFT count value: {}", ryftAggregation.getValue());
        assertEquals("Count values should be equal", aggregation.getValue(), ryftAggregation.getValue(), 1e-10);
        assertEquals(aggregation.getMetaData().get("color"), ryftAggregation.getMetaData().get("color"));
    }

    @Test
    public void testAggregationWithScript() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        Script script = new Script("_value * correction", ScriptService.ScriptType.INLINE, "groovy", ImmutableMap.of("correction", 1.2));
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders.avg(aggregationName)
                .field("age").script(script);
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = getClient().prepareSearch(indexName).setQuery(queryBuilder)
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
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{indexName}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        Avg ryftAggregation = (Avg) ryftResponse.getAggregations().asList().get(0);
        LOGGER.info("RYFT avg value: {}", ryftAggregation.getValue());

        assertEquals("Avg values should be equal", aggregation.getValue(), ryftAggregation.getValue(), 1e-10);
    }

    @Test
    public void testAggregationWithSizeZero() throws Exception {
        String aggregationName = "1";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("eyeColor", "green");
        AbstractAggregationBuilder aggregationBuilder = AggregationBuilders
                .sum(aggregationName).field("age");
        LOGGER.info("Testing query: {}", queryBuilder.toString());
        LOGGER.info("Testing aggregation: {}", aggregationBuilder.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());

        SearchResponse searchResponse = getClient().prepareSearch(indexName).setQuery(queryBuilder)
                .addAggregation(aggregationBuilder).setSize(0).get();
        LOGGER.info("ES response has {} hits", searchResponse.getHits().getTotalHits());
        Sum aggregation = (Sum) searchResponse.getAggregations().get(aggregationName);
        LOGGER.info("ES sum value: {}", aggregation.getValue());

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
                + "      \"sum\": {\n"
                + "        \"field\": \"age\""
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft_enabled\": true,\n"
                + "  \"size\": 0"
                + "}";
        SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                new SearchRequest(new String[]{indexName}, elasticQuery.getBytes())).get();
        LOGGER.info("RYFT response has {} hits", ryftResponse.getHits().getTotalHits());
        assertEquals(searchResponse.getHits().getTotalHits(), ryftResponse.getHits().getTotalHits());
        assertEquals(0, ryftResponse.getHits().hits().length);
        Sum ryftAggregation = (Sum) ryftResponse.getAggregations().asList().get(0);
        LOGGER.info("RYFT sum value: {}", ryftAggregation.getValue());

        assertEquals("Sum values should be equal", aggregation.getValue(), ryftAggregation.getValue(), 1e-10);
    }

}
