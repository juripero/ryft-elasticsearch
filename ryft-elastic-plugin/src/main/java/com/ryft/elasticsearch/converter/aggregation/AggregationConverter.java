package com.ryft.elasticsearch.converter.aggregation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.geobounds.RyftInternalGeoBounds;
import org.elasticsearch.search.aggregations.metrics.geocentroid.InternalGeoCentroid;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.RyftInternalSum;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.util.Collections;
import java.util.Map;

public class AggregationConverter {

    public static InternalAggregation convertJsonToAggregation(Map.Entry<String, Map> aggregationQuery, String aggregationName, ObjectNode ryftAggregations) {
        String aggregationType = aggregationQuery.getKey();

        JsonNode resultNode = ryftAggregations.findValue(aggregationName);
        if (resultNode == null) {
            return null;
        }

        switch (aggregationType) {
            case "min":
                return convertMin(aggregationName, resultNode);
            case "max":
                return convertMax(aggregationName, resultNode);
            case "sum":
                return convertSum(aggregationName, resultNode);
            case "count":
                return convertCount(aggregationName, resultNode);
            case "avg":
                return convertAvg(aggregationName, resultNode);
            case "stats":
                return convertStats(aggregationName, resultNode);
            case "extended_stats":
                return convertExtendedStats(aggregationName, resultNode, aggregationQuery.getValue());
            case "geo_bounds":
                return convertGeoBounds(aggregationName, resultNode);
            case "geo_centroid":
                return convertGeoCentroid(aggregationName, resultNode);
        }

        return null;
    }

    //FIXME some methods return custom subclasses instead of ES aggregations because of non-public constructors. Check when we switch to ES 6+
    private static InternalMin convertMin(String name, JsonNode ryftAggregation) {
        Double value = ryftAggregation.get("value").doubleValue();
        return new InternalMin(name, value, ValueFormatter.RAW, Collections.EMPTY_LIST, null);
    }

    private static InternalMax convertMax(String name, JsonNode ryftAggregation) {
        Double value = ryftAggregation.get("value").doubleValue();
        return new InternalMax(name, value, ValueFormatter.RAW, Collections.EMPTY_LIST, null);
    }

    private static RyftInternalSum convertSum(String name, JsonNode ryftAggregation) {
        Double value = ryftAggregation.get("value").doubleValue();
        return new RyftInternalSum(name, value, ValueFormatter.RAW, Collections.EMPTY_LIST, null);
    }

    private static InternalValueCount convertCount(String name, JsonNode ryftAggregation) {
        Long value = ryftAggregation.get("value").longValue();
        return new InternalValueCount(name, value, ValueFormatter.RAW, Collections.EMPTY_LIST, null);
    }

    private static InternalAvg convertAvg(String name, JsonNode ryftAggregation) {
        Double value = ryftAggregation.get("value").doubleValue();
        return new InternalAvg(name, value, 1, ValueFormatter.RAW, Collections.EMPTY_LIST, null);
    }

    private static InternalStats convertStats(String name, JsonNode ryftAggregation) {
        Long count = ryftAggregation.get("count").longValue();
        Double min = ryftAggregation.get("min").doubleValue();
        Double max = ryftAggregation.get("max").doubleValue();
        Double sum = ryftAggregation.get("sum").doubleValue();
        return new InternalStats(name, count, sum, min, max, ValueFormatter.RAW, Collections.EMPTY_LIST, null);
    }

    private static InternalExtendedStats convertExtendedStats(String name, JsonNode ryftAggregation, Map<String, Object> statsQuery) {
        Long count = ryftAggregation.get("count").longValue();
        Double min = ryftAggregation.get("min").doubleValue();
        Double max = ryftAggregation.get("max").doubleValue();
        Double sum = ryftAggregation.get("sum").doubleValue();
        Double sumOfSqrs = ryftAggregation.get("sum_of_squares").doubleValue();
        Double sigma = (Double) statsQuery.get("sigma");
        if (sigma == null) sigma = 2.0;

        return new InternalExtendedStats(name, count, sum, min, max, sumOfSqrs, sigma, ValueFormatter.RAW, Collections.EMPTY_LIST, null);
    }

    private static RyftInternalGeoBounds convertGeoBounds(String name, JsonNode ryftAggregation) {
        JsonNode bounds = ryftAggregation.get("bounds");
        JsonNode bottomRight = bounds.get("bottom_right");
        JsonNode topLeft = bounds.get("top_left");

        Double top = topLeft.get("lat").asDouble();
        Double bottom = bottomRight.get("lat").asDouble();
        Double posLeft = topLeft.get("lon").asDouble();
        Double posRight = bottomRight.get("lon").asDouble();

        return new RyftInternalGeoBounds(name, top, bottom, posLeft, posRight, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, true, Collections.EMPTY_LIST, null);
    }

    private static InternalGeoCentroid convertGeoCentroid(String name, JsonNode ryftAggregation) {
        JsonNode centroid = ryftAggregation.get("centroid");
        JsonNode location = centroid.get("location");

        Long count = centroid.get("count").longValue();
        Double lat = location.get("lat").asDouble();
        Double lon = location.get("lon").asDouble();

        GeoPoint geoPoint = new GeoPoint(lat, lon);
        return new InternalGeoCentroid(name, geoPoint, count, Collections.EMPTY_LIST, null);
    }
}
