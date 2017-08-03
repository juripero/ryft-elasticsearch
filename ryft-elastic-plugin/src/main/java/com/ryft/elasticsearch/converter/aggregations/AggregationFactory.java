package com.ryft.elasticsearch.converter.aggregations;

import com.ryft.elasticsearch.plugin.RyftProperties;
import java.util.Map;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.avg.AvgBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinBuilder;

public class AggregationFactory {

    private static final String DATE_HISTOGRAM_AGGREGATION = "date_histogram";
    private static final String MIN_AGGREGATION = "min";
    private static final String AVG_AGGREGATION = "avg";

    public AbstractAggregationBuilder get(String aggType, String aggName,
            RyftProperties aggregationProperties) {
        switch (aggType) {
            case DATE_HISTOGRAM_AGGREGATION:
                return getDateHistogramAggregation(aggName, aggregationProperties);
            case MIN_AGGREGATION:
                return getMinAggregation(aggName, aggregationProperties);
            case AVG_AGGREGATION:
                return getAvgAggregation(aggName, aggregationProperties);
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
        MinBuilder result = AggregationBuilders.min(aggName);
        if (aggregationProperties.containsKey("script")) {
            RyftProperties scriptProperties = aggregationProperties.getRyftProperties("script");
            result.script(getScript(scriptProperties));
        }
        result.field(aggregationProperties.getStr("field"))
                .format(aggregationProperties.getStr("format"))
                .missing(aggregationProperties.getInt("missing"));
        return result;
    }

    private AbstractAggregationBuilder getAvgAggregation(String aggName, RyftProperties aggregationProperties) {
        AvgBuilder result = AggregationBuilders.avg(aggName);
        if (aggregationProperties.containsKey("script")) {
            RyftProperties scriptProperties = aggregationProperties.getRyftProperties("script");
            result.script(getScript(scriptProperties));
        }
        result.field(aggregationProperties.getStr("field"))
                .format(aggregationProperties.getStr("format"))
                .missing(aggregationProperties.getInt("missing"));
        return result;
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
