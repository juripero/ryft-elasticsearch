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

public class AggregationFactory {

    private static final String DATE_HISTOGRAM_AGGREGATION = "date_histogram";
    private static final String MIN_AGGREGATION = "min";
    private static final String MAX_AGGREGATION = "max";
    private static final String AVG_AGGREGATION = "avg";
    private static final String SUM_AGGREGATION = "sum";
    private static final String STATS_AGGREGATION = "stats";
    private static final String EXT_STATS_AGGREGATION = "extended_stats";
    private static final String GEO_BOUNDS_AGGREGATION = "geo_bounds";

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
        return initValuesSourceMetricAggregation(AggregationBuilders.extendedStats(aggName), aggregationProperties)
                .sigma(aggregationProperties.getDouble("sigma"));
    }

    private AbstractAggregationBuilder getGeoBoundsAggregation(String aggName, RyftProperties aggregationProperties) {
        GeoBoundsBuilder geoBoundsBuilder = initValuesSourceAggregation(
                AggregationBuilders.geoBounds(aggName), aggregationProperties);
        if (aggregationProperties.containsKey("wrap_longitude")) {
            geoBoundsBuilder.wrapLongitude(aggregationProperties.getBool("wrap_longitude"));
        }
        return geoBoundsBuilder;
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
