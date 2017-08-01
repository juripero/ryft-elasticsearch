package com.ryft.elasticsearch.converter.aggregations;

import com.ryft.elasticsearch.plugin.RyftProperties;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;

public class AggregationFactory {

    private static final String DATE_HISTOGRAM = "date_histogram";

    public AggregationBuilder get(String aggType, String aggName,
            RyftProperties properties) {
        switch (aggType) {
            case DATE_HISTOGRAM:
                return getDateHistogram(aggName, properties);
            default:
                return null;
        }
    }

    private AggregationBuilder getDateHistogram(String aggName, RyftProperties properties) {
        DateHistogramBuilder result = AggregationBuilders.dateHistogram(aggName);
        RyftProperties innerProperties;
        if (properties.containsKey("min_doc_count")) {
            result.minDocCount(properties.getLong("min_doc_count"));
        }
        if (properties.containsKey("extended_bounds")) {
            innerProperties = properties.getRyftProperties("extended_bounds");
            result.extendedBounds(innerProperties.getStr("min"), innerProperties.getStr("max"));
        }
        if (properties.containsKey("order")) {
            innerProperties = properties.getRyftProperties("order");
            if (innerProperties.size() == 1) {
                String path = innerProperties.keys().nextElement().toString();
                Boolean asc = innerProperties.getStr(path).toLowerCase().equals("asc");
                result.order(Histogram.Order.aggregation(path, asc));
            }
        }
        return result.offset(properties.getStr("offset"))
                .field(properties.getStr("field"))
                .interval(new DateHistogramInterval(properties.getStr("interval")))
                .timeZone(properties.getStr("time_zone"))
                .format(properties.getStr("format"));
    }
}
