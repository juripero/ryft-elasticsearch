package com.ryft.elasticsearch.converter;

import com.google.common.collect.ImmutableMap;
import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import com.ryft.elasticsearch.utils.JSR250Module;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Inject;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

@RunWith(BlockJUnit4ClassRunner.class)
public class ElasticConverterAggregationsTest {

    @Inject
    public ElasticConverter elasticConverter;

    @Before
    public void setUp() {
        Guice.createInjector(
                new AbstractModule() {
            @Override
            protected void configure() {
                install(new JSR250Module());
                install(new ElasticConversionModule());
            }
        }).injectMembers(this);
    }

    @Test
    public void dateHistogramAggregationTest() throws Exception {
        String query = "{"
                + "  \"query\": {"
                + "    \"match\": {"
                + "      \"_all\": {"
                + "        \"query\": \"test\""
                + "      }"
                + "    }"
                + "  },"
                + "  \"aggs\": {"
                + "    \"agg_name\": {"
                + "      \"date_histogram\": {"
                + "        \"field\": \"date\","
                + "        \"interval\": \"1.5h\","
                + "        \"offset\": \"+6h\","
                + "        \"time_zone\": \"-01:00\","
                + "        \"format\": \"yyyy-MM-dd\","
                + "        \"extended_bounds\" : {"
                + "          \"min\" : \"2016-01-22\","
                + "          \"max\" : \"2017-08-01\""
                + "        },"
                + "        \"order\": {"
                + "          \"_count\" : \"asc\""
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{"test"}, query.getBytes());
        RyftRequestParameters ryftRequestParameters = elasticConverter.convert(request);
        assertNotNull(ryftRequestParameters);
        assertNotNull(ryftRequestParameters.getAggregations());
        assertFalse(ryftRequestParameters.getAggregations().isEmpty());
        AbstractAggregationBuilder actualAgg = ryftRequestParameters.getAggregations().get(0);
        AbstractAggregationBuilder expectedAgg = AggregationBuilders.dateHistogram("agg_name")
                .field("date").interval(new DateHistogramInterval("1.5h")).offset("+6h")
                .timeZone("-01:00").format("yyyy-MM-dd").extendedBounds("2016-01-22", "2017-08-01")
                .order(Histogram.Order.COUNT_ASC);
        assertEquals(expectedAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string(),
                actualAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());
    }

    @Test
    public void minAggregationTest() throws Exception {
        String query = "{"
                + "  \"query\": {"
                + "    \"match\": {"
                + "      \"_all\": {"
                + "        \"query\": \"test\""
                + "      }"
                + "    }"
                + "  },"
                + "  \"aggs\": {"
                + "    \"agg_name\": {"
                + "      \"min\": {"
                + "        \"field\": \"value\","
                + "        \"missing\": 1,"
                + "        \"script\": {"
                + "          \"file\": \"my_script\","
                + "          \"params\": {"
                + "            \"field\": \"price\""
                + "          }"
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{"test"}, query.getBytes());
        RyftRequestParameters ryftRequestParameters = elasticConverter.convert(request);
        assertNotNull(ryftRequestParameters);
        assertNotNull(ryftRequestParameters.getAggregations());
        assertFalse(ryftRequestParameters.getAggregations().isEmpty());
        AbstractAggregationBuilder actualAgg = ryftRequestParameters.getAggregations().get(0);
        Script script = new Script("my_script", ScriptService.ScriptType.FILE, null, ImmutableMap.of("field", "price"));
        AbstractAggregationBuilder expectedAgg = AggregationBuilders.min("agg_name")
                .field("value").missing(1).script(script);
        assertEquals(expectedAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string(),
                actualAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());
    }

    @Test
    public void maxAggregationTest() throws Exception {
        String query = "{"
                + "  \"query\": {"
                + "    \"match\": {"
                + "      \"_all\": {"
                + "        \"query\": \"test\""
                + "      }"
                + "    }"
                + "  },"
                + "  \"aggs\": {"
                + "    \"agg_name\": {"
                + "      \"max\": {"
                + "        \"field\": \"value\","
                + "        \"missing\": 1,"
                + "        \"script\": {"
                + "          \"id\": \"my_script\","
                + "          \"lang\": \"groovy\","
                + "          \"params\": {"
                + "            \"field\": \"price\""
                + "          }"
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{"test"}, query.getBytes());
        RyftRequestParameters ryftRequestParameters = elasticConverter.convert(request);
        assertNotNull(ryftRequestParameters);
        assertNotNull(ryftRequestParameters.getAggregations());
        assertFalse(ryftRequestParameters.getAggregations().isEmpty());
        AbstractAggregationBuilder actualAgg = ryftRequestParameters.getAggregations().get(0);
        Script script = new Script("my_script", ScriptService.ScriptType.INDEXED, "groovy", ImmutableMap.of("field", "price"));
        AbstractAggregationBuilder expectedAgg = AggregationBuilders.max("agg_name")
                .field("value").missing(1).script(script);
        assertEquals(expectedAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string(),
                actualAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());
    }

    @Test
    public void sumAggregationTest() throws Exception {
        String query = "{"
                + "  \"query\": {"
                + "    \"match\": {"
                + "      \"_all\": {"
                + "        \"query\": \"test\""
                + "      }"
                + "    }"
                + "  },"
                + "  \"aggs\": {"
                + "    \"agg_name\": {"
                + "      \"sum\": {"
                + "        \"field\": \"value\","
                + "        \"missing\": 0"
                + "      }"
                + "    }"
                + "  }"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{"test"}, query.getBytes());
        RyftRequestParameters ryftRequestParameters = elasticConverter.convert(request);
        assertNotNull(ryftRequestParameters);
        assertNotNull(ryftRequestParameters.getAggregations());
        assertFalse(ryftRequestParameters.getAggregations().isEmpty());
        AbstractAggregationBuilder actualAgg = ryftRequestParameters.getAggregations().get(0);
        AbstractAggregationBuilder expectedAgg = AggregationBuilders.sum("agg_name")
                .field("value").missing(0);
        assertEquals(expectedAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string(),
                actualAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());
    }

    @Test
    public void avgAggregationTest() throws Exception {
        String query = "{"
                + "  \"query\": {"
                + "    \"match\": {"
                + "      \"_all\": {"
                + "        \"query\": \"test\""
                + "      }"
                + "    }"
                + "  },"
                + "  \"aggs\": {"
                + "    \"agg_name\": {"
                + "      \"avg\": {"
                + "        \"field\": \"value\","
                + "        \"missing\": 1,"
                + "        \"script\" : {\n"
                + "          \"lang\": \"groovy\",\n"
                + "          \"inline\": \"_value * params.correction\",\n"
                + "          \"params\" : {\n"
                + "            \"correction\" : 1.2\n"
                + "          }\n"
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{"test"}, query.getBytes());
        RyftRequestParameters ryftRequestParameters = elasticConverter.convert(request);
        assertNotNull(ryftRequestParameters);
        assertNotNull(ryftRequestParameters.getAggregations());
        assertFalse(ryftRequestParameters.getAggregations().isEmpty());
        AbstractAggregationBuilder actualAgg = ryftRequestParameters.getAggregations().get(0);
        Script script = new Script("_value * params.correction", ScriptService.ScriptType.INLINE, "groovy", ImmutableMap.of("correction", 1.2));
        AbstractAggregationBuilder expectedAgg = AggregationBuilders.avg("agg_name")
                .field("value").missing(1).script(script);
        assertEquals(expectedAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string(),
                actualAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());
    }

    @Test
    public void statsAggregationTest() throws Exception {
        String query = "{"
                + "  \"query\": {"
                + "    \"match\": {"
                + "      \"_all\": {"
                + "        \"query\": \"test\""
                + "      }"
                + "    }"
                + "  },"
                + "  \"aggs\": {"
                + "    \"agg_name\": {"
                + "      \"stats\": {"
                + "        \"field\": \"value\","
                + "        \"missing\": 1"
                + "      }"
                + "    }"
                + "  }"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{"test"}, query.getBytes());
        RyftRequestParameters ryftRequestParameters = elasticConverter.convert(request);
        assertNotNull(ryftRequestParameters);
        assertNotNull(ryftRequestParameters.getAggregations());
        assertFalse(ryftRequestParameters.getAggregations().isEmpty());
        AbstractAggregationBuilder actualAgg = ryftRequestParameters.getAggregations().get(0);
        AbstractAggregationBuilder expectedAgg = AggregationBuilders.stats("agg_name")
                .field("value").missing(1);
        assertEquals(expectedAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string(),
                actualAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());
    }

    @Test
    public void extStatsAggregationTest() throws Exception {
        String query = "{"
                + "  \"query\": {"
                + "    \"match\": {"
                + "      \"_all\": {"
                + "        \"query\": \"test\""
                + "      }"
                + "    }"
                + "  },"
                + "  \"aggs\": {"
                + "    \"agg_name\": {"
                + "      \"extended_stats\": {"
                + "        \"field\": \"value\","
                + "        \"missing\": 1,"
                + "        \"sigma\": 3.1415926"
                + "      }"
                + "    }"
                + "  }"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{"test"}, query.getBytes());
        RyftRequestParameters ryftRequestParameters = elasticConverter.convert(request);
        assertNotNull(ryftRequestParameters);
        assertNotNull(ryftRequestParameters.getAggregations());
        assertFalse(ryftRequestParameters.getAggregations().isEmpty());
        AbstractAggregationBuilder actualAgg = ryftRequestParameters.getAggregations().get(0);
        AbstractAggregationBuilder expectedAgg = AggregationBuilders.extendedStats("agg_name")
                .field("value").missing(1).sigma(3.1415926);
        assertEquals(expectedAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string(),
                actualAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());
    }

    @Test
    public void geoBoundsAggregationTest() throws Exception {
        String query = "{"
                + "  \"query\": {"
                + "    \"match\": {"
                + "      \"_all\": {"
                + "        \"query\": \"test\""
                + "      }"
                + "    }"
                + "  },"
                + "  \"aggs\": {"
                + "    \"agg_name\": {"
                + "      \"geo_bounds\": {"
                + "        \"field\": \"location\","
                + "        \"wrap_longitude\": false"
                + "      }"
                + "    }"
                + "  }"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{"test"}, query.getBytes());
        RyftRequestParameters ryftRequestParameters = elasticConverter.convert(request);
        assertNotNull(ryftRequestParameters);
        assertNotNull(ryftRequestParameters.getAggregations());
        assertFalse(ryftRequestParameters.getAggregations().isEmpty());
        AbstractAggregationBuilder actualAgg = ryftRequestParameters.getAggregations().get(0);
        AbstractAggregationBuilder expectedAgg = AggregationBuilders.geoBounds("agg_name")
                .field("location").wrapLongitude(false);
        assertEquals(expectedAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string(),
                actualAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());
    }

    @Test
    public void severalAggregationTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"match\": {\n"
                + "      \"eyeColor\": {\n"
                + "        \"query\": \"green\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"aggs\": {\n"
                + "    \"xx\": {\n"
                + "      \"min\": {\n"
                + "        \"field\": \"age\"\n"
                + "      }\n"
                + "    },\n"
                + "    \"yy\": {\n"
                + "      \"max\": {\n"
                + "        \"field\": \"age\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft_enabled\": true\n"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{"test"}, query.getBytes());
        RyftRequestParameters ryftRequestParameters = elasticConverter.convert(request);
        assertNotNull(ryftRequestParameters);
        assertNotNull(ryftRequestParameters.getAggregations());
        assertFalse(ryftRequestParameters.getAggregations().isEmpty());
        List<AbstractAggregationBuilder> actualAggregations = ryftRequestParameters.getAggregations();
        List<String> actualAggregationStrings = new ArrayList<>();
        for (AbstractAggregationBuilder actualAggregation : actualAggregations) {
            actualAggregationStrings.add(actualAggregation.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());
        }
        String expectedAgg1 = AggregationBuilders.min("xx")
                .field("age").toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string();
        String expectedAgg2 = AggregationBuilders.max("yy")
                .field("age").toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string();
        assertTrue(actualAggregationStrings.contains(expectedAgg1));
        assertTrue(actualAggregationStrings.contains(expectedAgg2));
    }
}
