/**
 * ##License
 * Ryft-Customized BSD License
 * Copyright (c) 2018, Ryft Systems, Inc.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
 *   This product includes software developed by Ryft Systems, Inc.
 * 4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
 *   to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.ryft.elasticsearch.converter.aggregation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.RyftInternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.RyftInternalHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.RyftInternalOrder;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class AggregationConverter {

    public static InternalAggregation convertJsonToAggregation(ObjectNode aggregationQuery, String aggregationName, ObjectNode ryftAggregations) {
        String aggregationType = aggregationQuery.fieldNames().next();
        ObjectNode aggregationValue = (ObjectNode) aggregationQuery.get(aggregationType);

        JsonNode resultNode = ryftAggregations.findValue(aggregationName);
        if ((resultNode == null) || (resultNode.isEmpty(null))) {
            return null;
        }

        switch (aggregationType) {
            case "min":
                return convertMin(aggregationName, resultNode);
            case "max":
                return convertMax(aggregationName, resultNode);
            case "sum":
                return convertSum(aggregationName, resultNode);
            case "value_count":
                return convertCount(aggregationName, resultNode);
            case "avg":
                return convertAvg(aggregationName, resultNode);
            case "stats":
                return convertStats(aggregationName, resultNode);
            case "extended_stats":
                return convertExtendedStats(aggregationName, resultNode, aggregationValue);
            case "geo_bounds":
                return convertGeoBounds(aggregationName, resultNode);
            case "geo_centroid":
                return convertGeoCentroid(aggregationName, resultNode);
            case "date_histogram":
                return convertDateHistogram(aggregationName, resultNode);
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

    private static InternalExtendedStats convertExtendedStats(String name, JsonNode ryftAggregation, ObjectNode statsQuery) {
        Long count = ryftAggregation.get("count").longValue();
        Double min = ryftAggregation.get("min").doubleValue();
        Double max = ryftAggregation.get("max").doubleValue();
        Double sum = ryftAggregation.get("sum").doubleValue();
        Double sumOfSqrs = ryftAggregation.get("sum_of_squares").doubleValue();
        Double sigma;
        if (statsQuery.has("sigma")) {
            sigma = statsQuery.get("sigma").asDouble();
        } else {
            sigma = 2.0;
        }

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

    private static RyftInternalHistogram convertDateHistogram(String name, JsonNode ryftAggregation) {
        RyftInternalHistogram.Factory<RyftInternalDateHistogram.Bucket> histogramFactory = new RyftInternalHistogram.Factory<>();
        RyftInternalDateHistogram.Factory dateHistogramFactory = new RyftInternalDateHistogram.Factory();

        JsonNode bucketsNode = ryftAggregation.get("buckets");
        List<RyftInternalDateHistogram.Bucket> buckets = new ArrayList<>();

        if (bucketsNode.isArray()) {
            Iterator<JsonNode> iterator = bucketsNode.elements();
            while (iterator.hasNext()) {
                JsonNode element = iterator.next();
                Long key = element.get("key").asLong();
                Long docCount = element.get("doc_count").asLong();
                buckets.add(dateHistogramFactory.createBucket(key, docCount, InternalAggregations.EMPTY, false, ValueFormatter.RAW));
            }
        }
        // emptyBucketInfo can be null for minDocCount > 0
        return histogramFactory.create(name, buckets, (RyftInternalOrder) RyftInternalOrder.KEY_ASC,
                1, null, ValueFormatter.RAW, false, Collections.EMPTY_LIST, null);
    }
}
