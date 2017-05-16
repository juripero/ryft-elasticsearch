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
        private final String TYPE_CURRENCY = "currency";
        private final String TYPE_IPV4 = "ipv4";
        private final String TYPE_IPV6 = "ipv6";

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
                    case TYPE_CURRENCY:
                        convertingContext.setDataType(ElasticConvertingContext.ElasticDataType.CURRENCY);
                        break;
                    case TYPE_IPV4:
                        convertingContext.setDataType(ElasticConvertingContext.ElasticDataType.IPV4);
                        break;
                    case TYPE_IPV6:
                        convertingContext.setDataType(ElasticConvertingContext.ElasticDataType.IPV6);
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

    public static class ElasticConverterCurrency implements ElasticConvertingElement<String> {

        public static final String NAME = "currency";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }

    public static class ElasticConverterWidth implements ElasticConvertingElement<Void> {

        public static final String NAME = "width";

        @Override
        public Void convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            Object width = ElasticConversionUtil.getObject(convertingContext);
            if (width instanceof String && width.equals("line")) {
                convertingContext.setLine(true);
            } else if (width instanceof Integer) {
                convertingContext.setWidth((Integer) width);
            }
            return null;
        }
    }
}
