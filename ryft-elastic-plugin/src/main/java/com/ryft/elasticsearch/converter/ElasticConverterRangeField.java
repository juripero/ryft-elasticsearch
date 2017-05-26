package com.ryft.elasticsearch.converter;

import com.ryft.elasticsearch.converter.entities.RangeQueryParameters;
import com.ryft.elasticsearch.converter.ryftdsl.RyftExpressionRange;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class ElasticConverterRangeField extends ElasticConverterField {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterRangeField.class);

    public static class ElasticConverterGreaterThanEquals implements ElasticConvertingElement<String> {

        public static final String NAME = "gte";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }

    public static class ElasticConverterGreaterThan implements ElasticConvertingElement<String> {

        public static final String NAME = "gt";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }

    public static class ElasticConverterLessThanEquals implements ElasticConvertingElement<String> {

        public static final String NAME = "lte";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }

    public static class ElasticConverterLessThan implements ElasticConvertingElement<String> {

        public static final String NAME = "lt";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }

    final static String NAME = "range_field";

    @Override
    protected RyftQuery getRyftQuery(ElasticConvertingContext convertingContext, Map<String, Object> fieldQueryMap) throws ElasticConversionException {
        try {
            RangeQueryParameters rangeQueryParameters = new RangeQueryParameters();
            rangeQueryParameters.setDataType(convertingContext.getDataType());

            String fieldName = convertingContext.getContentParser().currentName();
            rangeQueryParameters.setFieldName(fieldName);

            for (Map.Entry<String, Object> entry : fieldQueryMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                switch (key) {
                    case ElasticConverterShared.ElasticConverterValue.NAME:
                        rangeQueryParameters.setSearchValue((String) value);
                        break;
                    case ElasticConverterShared.ElasticConverterDateFormat.NAME:
                        rangeQueryParameters.setFormat((String) value);
                        break;
                    case ElasticConverterGreaterThanEquals.NAME:
                        rangeQueryParameters.setLowerBound(Collections.singletonMap(RyftExpressionRange.RyftOperatorCompare.GTE, (String) value));
                        break;
                    case ElasticConverterGreaterThan.NAME:
                        rangeQueryParameters.setLowerBound(Collections.singletonMap(RyftExpressionRange.RyftOperatorCompare.GT, (String) value));
                        break;
                    case ElasticConverterLessThanEquals.NAME:
                        rangeQueryParameters.setUpperBound(Collections.singletonMap(RyftExpressionRange.RyftOperatorCompare.LTE, (String) value));
                        break;
                    case ElasticConverterLessThan.NAME:
                        rangeQueryParameters.setUpperBound(Collections.singletonMap(RyftExpressionRange.RyftOperatorCompare.LT, (String) value));
                        break;
                    case ElasticConverterShared.ElasticConverterSeparator.NAME:
                        rangeQueryParameters.setSeparator((String) value);
                        break;
                    case ElasticConverterShared.ElasticConverterDecimal.NAME:
                        rangeQueryParameters.setDecimal((String) value);
                        break;
                    case ElasticConverterShared.ElasticConverterCurrency.NAME:
                        rangeQueryParameters.setCurrency((String) value);
                        break;
                }
            }
            //FIXME - workaround for timeseries
            if (convertingContext.getSearchArray() != null) {
                rangeQueryParameters.setSearchArray(convertingContext.getSearchArray());
            }
            return convertingContext.getQueryFactory().buildRangeQuery(rangeQueryParameters);
        } catch (IOException ex) {
            throw new ElasticConversionException("Request parsing error");
        }

    }
}
