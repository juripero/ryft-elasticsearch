package com.ryft.elasticsearch.plugin.elastic.converter;

import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQuery;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

/**
 * Determine that query is of type
 * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/2.4/query-dsl-term-query.html">term</a>.
 * This is required so that {@link com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQueryFactory} knows which
 * RyftQuery type to use.
 */
public class ElasticConverterTerm implements ElasticConvertingElement<RyftQuery> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterTerm.class);

    final static String NAME = "term";

    @Override
    public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        convertingContext.setSearchType(ElasticConvertingContext.ElasticSearchType.TERM);
        ElasticConvertingElement<RyftQuery> converter =
                convertingContext.getElasticConverter(ElasticConverterField.NAME);
        return ElasticConversionUtil.getObject(convertingContext, converter);
    }

}
