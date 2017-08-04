package com.ryft.elasticsearch.converter.aggregations;

import com.ryft.elasticsearch.converter.ElasticConversionException;
import com.ryft.elasticsearch.converter.ElasticConversionUtil;
import com.ryft.elasticsearch.converter.ElasticConvertingContext;
import com.ryft.elasticsearch.converter.ElasticConvertingElement;
import com.ryft.elasticsearch.plugin.RyftProperties;
import java.util.Objects;
import java.util.stream.Collectors;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;

public class ElasticConverterAggs implements ElasticConvertingElement<Void> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterAggs.class);

    public static final String NAME = "aggs";
    public static final String DATE_HISTOGRAM = "date_histogram";

    private final AggregationFactory aggregationFactory;

    @Inject
    public ElasticConverterAggs(AggregationFactory aggregationFactory) {
        this.aggregationFactory = aggregationFactory;
    }

    @Override
    public Void convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        RyftProperties aggregationProperties = ElasticConversionUtil.getMap(convertingContext);
        aggregationProperties.entrySet().stream()
            .map((entry) -> entry.getKey().toString())
            .map((aggName) -> {
                RyftProperties innerProperties = aggregationProperties.getRyftProperties(aggName);
                String aggType = innerProperties.keys().nextElement().toString();
                innerProperties = innerProperties.getRyftProperties(aggType);
                AbstractAggregationBuilder aggregationBuilder = aggregationFactory.get(aggType, aggName, innerProperties);
                return aggregationBuilder;
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toCollection(() -> convertingContext.getAggregationBuilders()));
        return null;
    }

}
