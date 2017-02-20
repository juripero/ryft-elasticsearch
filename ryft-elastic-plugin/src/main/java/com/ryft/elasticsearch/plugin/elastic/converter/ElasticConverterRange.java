package com.ryft.elasticsearch.plugin.elastic.converter;

import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQuery;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

/**
 * Determine that query is of type
 * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html">range</a>.
 * This is required so that {@link com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQueryFactory} knows which
 * RyftQuery type to use.
 */
public class ElasticConverterRange implements ElasticConvertingElement<RyftQuery> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterTerm.class);

    final static String NAME = "range";

    @Override
    public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        convertingContext.setSearchType(ElasticConvertingContext.ElasticSearchType.RANGE);
        ElasticConvertingElement<RyftQuery> converter =
                convertingContext.getElasticConverter(ElasticConverterRangeField.NAME);
        return ElasticConversionUtil.getObject(convertingContext, converter);
    }

}
