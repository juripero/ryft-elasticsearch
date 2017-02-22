package com.ryft.elasticsearch.plugin.elastic.converter;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

/**
 * Contains converters that are shared between {@link ElasticConverterField} and {@link ElasticConverterRangeField}
 */
public class ElasticConverterShared {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterField.class);

    public static class ElasticConverterValue implements ElasticConvertingElement<String> {

        public static final String NAME = "value";

        public static final String NAME_ALTERNATIVE = "query";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }

    public static class ElasticConverterDateFormat implements ElasticConvertingElement<String> {

        public static final String NAME = "datetime_format";
        public static final String NAME_ALTERNATIVE = "format";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }

    public static class ElasticConverterType implements ElasticConvertingElement<Void> {

        public static final String NAME = "type";

        private final String TYPE_PHRASE = "phrase";
        private final String TYPE_DATETIME = "datetime";
        private final String TYPE_NUMBER = "number";

        @Override
        public Void convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            String type = ElasticConversionUtil.getString(convertingContext);

            if (TYPE_PHRASE.equals(type.toLowerCase())
                    && ElasticConvertingContext.ElasticSearchType.MATCH.equals(convertingContext.getSearchType())) {
                convertingContext.setSearchType(ElasticConvertingContext.ElasticSearchType.MATCH_PHRASE);
            } else {
                switch (type.toLowerCase()) {
                    case TYPE_DATETIME:
                        convertingContext.setDataType(ElasticConvertingContext.ElasticDataType.DATETIME);
                        break;
                    case TYPE_NUMBER:
                        convertingContext.setDataType(ElasticConvertingContext.ElasticDataType.NUMBER);
                        break;
                }
            }
            return null;

        }
    }

    public static class ElasticConverterSeparator implements ElasticConvertingElement<String> {

        public static final String NAME = "separator";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }

    public static class ElasticConverterDecimal implements ElasticConvertingElement<String> {

        public static final String NAME = "decimal";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }
}
