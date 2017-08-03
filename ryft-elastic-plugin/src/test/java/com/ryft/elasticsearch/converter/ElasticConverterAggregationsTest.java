package com.ryft.elasticsearch.converter;

import com.google.common.collect.ImmutableMap;
import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import com.ryft.elasticsearch.utils.JSR250Module;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Inject;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.avg.AvgBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinBuilder;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

@RunWith(BlockJUnit4ClassRunner.class)
public class ElasticConverterAggregationsTest {

    @Inject
    public ElasticConverter elasticConverter;

    @Inject
    public ContextFactory contextFactory;

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
        DateHistogramBuilder actualAgg = (DateHistogramBuilder) ryftRequestParameters.getAggregations().get(0);
        DateHistogramBuilder expectedAgg = AggregationBuilders.dateHistogram("agg_name")
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
        MinBuilder actualAgg = (MinBuilder) ryftRequestParameters.getAggregations().get(0);
        Script script = new Script("my_script", ScriptService.ScriptType.FILE, null, ImmutableMap.of("field", "price"));
        MinBuilder expectedAgg = AggregationBuilders.min("agg_name")
                .field("value").missing(1).script(script);
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
        AvgBuilder actualAgg = (AvgBuilder) ryftRequestParameters.getAggregations().get(0);
        Script script = new Script("_value * params.correction", ScriptService.ScriptType.INLINE, "groovy", ImmutableMap.of("correction", 1.2));
        AvgBuilder expectedAgg = AggregationBuilders.avg("agg_name")
                .field("value").missing(1).script(script);
        assertEquals(expectedAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string(),
                actualAgg.toXContent(jsonBuilder().startObject(), EMPTY_PARAMS).string());
    }

}
