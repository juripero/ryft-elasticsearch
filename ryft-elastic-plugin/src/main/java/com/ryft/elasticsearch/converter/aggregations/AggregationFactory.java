package com.ryft.elasticsearch.converter.aggregations;

import com.ryft.elasticsearch.plugin.RyftProperties;
import java.util.Map;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanksBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesMethod;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsBuilder;

public class AggregationFactory {

    private static final String DATE_HISTOGRAM_AGGREGATION = "date_histogram";
    private static final String MIN_AGGREGATION = "min";
    private static final String MAX_AGGREGATION = "max";
    private static final String AVG_AGGREGATION = "avg";
    private static final String SUM_AGGREGATION = "sum";
    private static final String STATS_AGGREGATION = "stats";
    private static final String EXT_STATS_AGGREGATION = "extended_stats";
    private static final String GEO_BOUNDS_AGGREGATION = "geo_bounds";
    private static final String GEO_CENTROID_AGGREGATION = "geo_centroid";
    private static final String PERCENTILES_AGGREGATION = "percentiles";
    private static final String PERCENTILE_RANKS_AGGREGATION = "percentile_ranks";
    private static final String VALUE_COUNT_AGGREGATION = "value_count";

    public AbstractAggregationBuilder get(String aggType, String aggName,
            RyftProperties aggregationProperties) {
        switch (aggType) {
            case DATE_HISTOGRAM_AGGREGATION:
                return getDateHistogramAggregation(aggName, aggregationProperties);
            case MIN_AGGREGATION:
                return getMinAggregation(aggName, aggregationProperties);
            case MAX_AGGREGATION:
                return getMaxAggregation(aggName, aggregationProperties);
            case SUM_AGGREGATION:
                return getSumAggregation(aggName, aggregationProperties);
            case AVG_AGGREGATION:
                return getAvgAggregation(aggName, aggregationProperties);
            case STATS_AGGREGATION:
                return getStatsAggregation(aggName, aggregationProperties);
            case EXT_STATS_AGGREGATION:
                return getExtStatsAggregation(aggName, aggregationProperties);
            case GEO_BOUNDS_AGGREGATION:
                return getGeoBoundsAggregation(aggName, aggregationProperties);
            case GEO_CENTROID_AGGREGATION:
                return getGeoCentroidAggregation(aggName, aggregationProperties);
            case PERCENTILES_AGGREGATION:
                return getPercentilesAggregation(aggName, aggregationProperties);
            case PERCENTILE_RANKS_AGGREGATION:
                return getPercentileRanksAggregation(aggName, aggregationProperties);
            case VALUE_COUNT_AGGREGATION:
                return getValueCountAggregation(aggName, aggregationProperties);
            default:
                return null;
        }
    }

    private AbstractAggregationBuilder getDateHistogramAggregation(String aggName, RyftProperties aggregationProperties) {
        DateHistogramBuilder result = AggregationBuilders.dateHistogram(aggName);
        RyftProperties innerProperties;
        if (aggregationProperties.containsKey("min_doc_count")) {
            result.minDocCount(aggregationProperties.getLong("min_doc_count"));
        }
        if (aggregationProperties.containsKey("extended_bounds")) {
            innerProperties = aggregationProperties.getRyftProperties("extended_bounds");
            result.extendedBounds(innerProperties.getStr("min"), innerProperties.getStr("max"));
        }
        if (aggregationProperties.containsKey("order")) {
            innerProperties = aggregationProperties.getRyftProperties("order");
            if (innerProperties.size() == 1) {
                String path = innerProperties.keys().nextElement().toString();
                Boolean asc = innerProperties.getStr(path).toLowerCase().equals("asc");
                result.order(Histogram.Order.aggregation(path, asc));
            }
        }
        return result.offset(aggregationProperties.getStr("offset"))
                .field(aggregationProperties.getStr("field"))
                .interval(new DateHistogramInterval(aggregationProperties.getStr("interval")))
                .timeZone(aggregationProperties.getStr("time_zone"))
                .format(aggregationProperties.getStr("format"));
    }

    private AbstractAggregationBuilder getMinAggregation(String aggName, RyftProperties aggregationProperties) {
        return initValuesSourceMetricAggregation(AggregationBuilders.min(aggName), aggregationProperties);
    }

    private AbstractAggregationBuilder getMaxAggregation(String aggName, RyftProperties aggregationProperties) {
        return initValuesSourceMetricAggregation(AggregationBuilders.max(aggName), aggregationProperties);
    }

    private AbstractAggregationBuilder getSumAggregation(String aggName, RyftProperties aggregationProperties) {
        return initValuesSourceMetricAggregation(AggregationBuilders.sum(aggName), aggregationProperties);
    }

    private AbstractAggregationBuilder getAvgAggregation(String aggName, RyftProperties aggregationProperties) {
        return initValuesSourceMetricAggregation(AggregationBuilders.avg(aggName), aggregationProperties);
    }

    private AbstractAggregationBuilder getStatsAggregation(String aggName, RyftProperties aggregationProperties) {
        return initValuesSourceMetricAggregation(AggregationBuilders.stats(aggName), aggregationProperties);
    }

    private AbstractAggregationBuilder getExtStatsAggregation(String aggName, RyftProperties aggregationProperties) {
        ExtendedStatsBuilder extendedStatsBuilder = 
                initValuesSourceMetricAggregation(AggregationBuilders.extendedStats(aggName), aggregationProperties);
        if (aggregationProperties.containsKey("sigma")) {
            extendedStatsBuilder.sigma(aggregationProperties.getDouble("sigma"));
        }
        return extendedStatsBuilder;
    }

    private AbstractAggregationBuilder getValueCountAggregation(String aggName, RyftProperties aggregationProperties) {
        return initValuesSourceMetricAggregation(AggregationBuilders.count(aggName), aggregationProperties);
    }

    private AbstractAggregationBuilder getGeoBoundsAggregation(String aggName, RyftProperties aggregationProperties) {
        GeoBoundsBuilder geoBoundsBuilder = initValuesSourceAggregation(
                AggregationBuilders.geoBounds(aggName), aggregationProperties);
        if (aggregationProperties.containsKey("wrap_longitude")) {
            geoBoundsBuilder.wrapLongitude(aggregationProperties.getBool("wrap_longitude"));
        }
        return geoBoundsBuilder;
    }

    private AbstractAggregationBuilder getGeoCentroidAggregation(String aggName, RyftProperties aggregationProperties) {
        return initValuesSourceMetricAggregation(
                AggregationBuilders.geoCentroid(aggName), aggregationProperties);
    }

    private AbstractAggregationBuilder getPercentilesAggregation(String aggName, RyftProperties aggregationProperties) {
        PercentilesBuilder percentilesBuilder = initValuesSourceMetricAggregation(
                AggregationBuilders.percentiles(aggName), aggregationProperties);
        if (aggregationProperties.containsKey("percents")) {
            double[] arr = aggregationProperties.getList("percents", Number.class).stream().mapToDouble(Number::doubleValue).toArray();
            percentilesBuilder.percentiles(arr);
        }
        if (aggregationProperties.containsKey(PercentilesMethod.HDR.getName())) {
            RyftProperties hdrProperties = aggregationProperties.getRyftProperties(PercentilesMethod.HDR.getName());
            percentilesBuilder.method(PercentilesMethod.HDR);
            percentilesBuilder.numberOfSignificantValueDigits(hdrProperties.getInt("number_of_significant_value_digits"));
        } else if (aggregationProperties.containsKey(PercentilesMethod.TDIGEST.getName())) {
            RyftProperties tdigestProperties = aggregationProperties.getRyftProperties(PercentilesMethod.TDIGEST.getName());
            percentilesBuilder.method(PercentilesMethod.TDIGEST);
            percentilesBuilder.compression(tdigestProperties.getDouble("compression"));
        }
        return percentilesBuilder;
    }

    private AbstractAggregationBuilder getPercentileRanksAggregation(String aggName, RyftProperties aggregationProperties) {
        PercentileRanksBuilder percentilesBuilder = initValuesSourceMetricAggregation(
                AggregationBuilders.percentileRanks(aggName), aggregationProperties);
        if (aggregationProperties.containsKey("values")) {
            double[] arr = aggregationProperties.getList("values", Number.class).stream().mapToDouble(Number::doubleValue).toArray();
            percentilesBuilder.percentiles(arr);
        }
        if (aggregationProperties.containsKey(PercentilesMethod.HDR.getName())) {
            RyftProperties hdrProperties = aggregationProperties.getRyftProperties(PercentilesMethod.HDR.getName());
            percentilesBuilder.method(PercentilesMethod.HDR);
            percentilesBuilder.numberOfSignificantValueDigits(hdrProperties.getInt("number_of_significant_value_digits"));
        } else if (aggregationProperties.containsKey(PercentilesMethod.TDIGEST.getName())) {
            RyftProperties tdigestProperties = aggregationProperties.getRyftProperties(PercentilesMethod.TDIGEST.getName());
            percentilesBuilder.method(PercentilesMethod.TDIGEST);
            percentilesBuilder.compression(tdigestProperties.getDouble("compression"));
        }
        return percentilesBuilder;
    }

    private <T extends ValuesSourceMetricsAggregationBuilder> T initValuesSourceMetricAggregation(
            T aggBuilder, RyftProperties aggregationProperties) {
        if (aggregationProperties.containsKey("script")) {
            RyftProperties scriptProperties = aggregationProperties.getRyftProperties("script");
            aggBuilder.script(getScript(scriptProperties));
        }
        aggBuilder.field(aggregationProperties.getStr("field"))
                .format(aggregationProperties.getStr("format"))
                .missing(aggregationProperties.getInt("missing"));
        return aggBuilder;
    }

    private <T extends ValuesSourceAggregationBuilder> T initValuesSourceAggregation(
            T aggBuilder, RyftProperties aggregationProperties) {
        if (aggregationProperties.containsKey("script")) {
            RyftProperties scriptProperties = aggregationProperties.getRyftProperties("script");
            aggBuilder.script(getScript(scriptProperties));
        }
        aggBuilder.field(aggregationProperties.getStr("field"))
                .missing(aggregationProperties.getInt("missing"));
        return aggBuilder;
    }

    private Script getScript(RyftProperties scriptProperties) {
        String script = null;
        ScriptType scriptType = null;
        Map<String, Object> scriptParams = null;
        if (scriptProperties.containsKey("params")) {
            scriptParams = scriptProperties.getRyftProperties("params").toMap();
        }
        if (scriptProperties.containsKey("inline")) {
            script = scriptProperties.getStr("inline");
            scriptType = ScriptType.INLINE;
        }
        if (scriptProperties.containsKey("file")) {
            script = scriptProperties.getStr("file");
            scriptType = ScriptType.FILE;
        }
        if (scriptProperties.containsKey("id")) {
            script = scriptProperties.getStr("id");
            scriptType = ScriptType.INDEXED;
        }
        return new Script(script, scriptType, scriptProperties.getStr("lang"), scriptParams);
    }
}
