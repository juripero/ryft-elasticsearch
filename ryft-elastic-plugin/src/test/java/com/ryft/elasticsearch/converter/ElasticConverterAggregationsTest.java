package com.ryft.elasticsearch.converter;

import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import com.ryft.elasticsearch.utils.JSR250Module;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Inject;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
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
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"match\": {\n"
                + "      \"_all\": {\n"
                + "        \"query\": \"test\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"aggs\": {\n"
                + "    \"agg_name\": {\n"
                + "      \"date_histogram\": {\n"
                + "        \"field\": \"date\",\n"
                + "        \"interval\": \"1.5h\",\n"
                + "        \"offset\": \"+6h\","
                + "        \"time_zone\": \"-01:00\","
                + "        \"format\": \"yyyy-MM-dd\","
                + "        \"extended_bounds\" : {\n"
                + "          \"min\" : \"2016-01-22\",\n"
                + "          \"max\" : \"2017-08-01\"\n"
                + "        },"
                + "        \"order\": {"
                + "          \"_count\" : \"asc\""
                + "        }"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{"test"}, query.getBytes());
        RyftRequestParameters ryftRequestParameters = elasticConverter.convert(request);
        assertNotNull(ryftRequestParameters);
        assertEquals("(RECORD CONTAINS \"test\")",
                ryftRequestParameters.getQuery().buildRyftString());
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

}
