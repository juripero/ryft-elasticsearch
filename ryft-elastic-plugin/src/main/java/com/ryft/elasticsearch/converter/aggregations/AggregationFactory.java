package com.ryft.elasticsearch.converter.aggregations;

import java.util.Map;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

public class AggregationFactory {

    private static final String DATE_HISTOGRAM = "date_histogram";

    public AggregationBuilder get(String aggType, String aggName,
            Map<String, String> properties) {
        switch (aggType) {
            case DATE_HISTOGRAM:
                Long minDocCount = (properties.get("min_doc_count") == null) ? null :
                        Long.getLong(properties.get("min_doc_count"));
                return AggregationBuilders.dateHistogram(aggName)
                        .offset(properties.get("offset"))
                        .field(properties.get("field"))
                        .interval(new DateHistogramInterval(properties.get("interval")))
                        .timeZone(properties.get("time_zone"));
            default:
                return null;
        }
    }
}
