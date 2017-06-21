package com.ryft.elasticsearch.converter;

import com.ryft.elasticsearch.converter.entities.AggregationParameters;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverterAggDateHistogram implements ElasticConvertingElement<RyftQuery> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterAggDateHistogram.class);

    public static final String NAME = "date_histogram";

    @Override
    public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));

        convertingContext.getAgg().setAggregationType(AggregationParameters.AggregationType.DATE_HISTOGRAM);

        String currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
        return (RyftQuery) convertingContext.getElasticConverter(currentName).convert(convertingContext);
    }

    public static class ElasticConverterAggField implements ElasticConvertingElement<RyftQuery> {

        public static final String NAME = "field";

        @Override
        public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));

            convertingContext.getAgg().setField(ElasticConversionUtil.getString(convertingContext));

            String currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
            return (RyftQuery) convertingContext.getElasticConverter(currentName).convert(convertingContext);
        }
    }

    public static class ElasticConverterAggInterval implements ElasticConvertingElement<RyftQuery> {

        public static final String NAME = "interval";

        @Override
        public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));

            convertingContext.getAgg().setInterval(ElasticConversionUtil.getString(convertingContext));

            String currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
            return (RyftQuery) convertingContext.getElasticConverter(currentName).convert(convertingContext);
        }
    }

    public static class ElasticConverterAggTimeZone implements ElasticConvertingElement<RyftQuery> {

        public static final String NAME = "time_zone";

        @Override
        public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));

            convertingContext.getAgg().setTimeZone(ElasticConversionUtil.getString(convertingContext));

            String currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
            return (RyftQuery) convertingContext.getElasticConverter(currentName).convert(convertingContext);
        }
    }

    public static class ElasticConverterAggMinDoc implements ElasticConvertingElement<RyftQuery> {

        public static final String NAME = "min_doc_count";

        @Override
        public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));

            convertingContext.getAgg().setMinDocCount(ElasticConversionUtil.getInteger(convertingContext));

            String currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
            return (RyftQuery) convertingContext.getElasticConverter(currentName).convert(convertingContext);
        }
    }


    public static class ElasticConverterAggBounds implements ElasticConvertingElement<Void> {

        public static final String NAME = "extended_bounds";

        @Override
        public Void convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));

            ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
            convertingContext.getAgg().setMinBound(ElasticConversionUtil.getLong(convertingContext));
            ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
            convertingContext.getAgg().setMaxBound(ElasticConversionUtil.getLong(convertingContext));
            return null;
        }
    }
}
