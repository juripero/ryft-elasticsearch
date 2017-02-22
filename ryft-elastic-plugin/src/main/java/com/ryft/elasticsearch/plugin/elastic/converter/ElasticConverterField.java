package com.ryft.elasticsearch.plugin.elastic.converter;

import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConvertingContext.ElasticSearchType;
import com.ryft.elasticsearch.plugin.elastic.converter.entities.FuzzyQueryParameters;
import com.ryft.elasticsearch.plugin.elastic.converter.entities.TermQueryParameters;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQueryComplex.RyftLogicalOperator;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQueryFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;

public class ElasticConverterField implements ElasticConvertingElement<RyftQuery> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterField.class);

    public static class ElasticConverterMetric implements ElasticConvertingElement<RyftFuzzyMetric> {

        public static final String NAME = "metric";

        @Override
        public RyftFuzzyMetric convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getEnum(convertingContext, RyftFuzzyMetric.class);
        }
    }

    public static class ElasticConverterFuzziness implements ElasticConvertingElement<Integer> {

        public static final String NAME = "fuzziness";
        private final String VALUE_FUZZINESS_AUTO = "auto";

        @Override
        public Integer convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            String fuzziness = ElasticConversionUtil.getString(convertingContext);
            if (fuzziness.toLowerCase().equals(VALUE_FUZZINESS_AUTO)) {
                return RyftQueryFactory.FUZZYNESS_AUTO_VALUE;
            } else {
                return ElasticConversionUtil.getInteger(convertingContext);
            }
        }
    }

    public static class ElasticConverterOperator implements ElasticConvertingElement<RyftLogicalOperator> {

        public static final String NAME = "operator";

        @Override
        public RyftLogicalOperator convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getEnum(convertingContext, RyftLogicalOperator.class);
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

    static final String NAME = "field_name";

    private static final String ALL_FIELDS = "_all";

    @Override
    public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug("Start field primitive parsing");
        XContentParser parser = convertingContext.getContentParser();
        try {
            parser.nextToken();
        } catch (IOException ex) {
            throw new ElasticConversionException("Request parsing error");
        }
        Map<String, Object> fieldParametersMap = new HashMap<>();
        switch (parser.currentToken()) {
            case START_OBJECT:
                return convertFromObject(convertingContext, fieldParametersMap);
            case VALUE_STRING:
                return convertFromString(convertingContext, fieldParametersMap);
            case VALUE_NUMBER:
                convertingContext.setDataType(ElasticConvertingContext.ElasticDataType.NUMBER);
                return convertFromString(convertingContext, fieldParametersMap);
            case START_ARRAY:
                //Will need to refactor if we ever need to support searching for arrays of not numbers
                convertingContext.setDataType(ElasticConvertingContext.ElasticDataType.NUMBER_ARRAY);
                return convertFromArray(convertingContext, fieldParametersMap);
            default:
                throw new ElasticConversionException("Request parsing error");
        }
    }

    protected RyftQuery getRyftQuery(ElasticConvertingContext convertingContext, Map<String, Object> fieldQueryMap) throws ElasticConversionException {
        if (fieldQueryMap.containsKey(ElasticConverterMetric.NAME)
                || fieldQueryMap.containsKey(ElasticConverterFuzziness.NAME)
                || ElasticConvertingContext.ElasticSearchType.FUZZY.equals(convertingContext.getSearchType())) {
            if (!fieldQueryMap.containsKey(ElasticConverterMetric.NAME)) {
                fieldQueryMap.put(ElasticConverterMetric.NAME, FuzzyQueryParameters.METRIC_DEFAULT);
            }
            if (!fieldQueryMap.containsKey(ElasticConverterFuzziness.NAME)) {
                fieldQueryMap.put(ElasticConverterFuzziness.NAME, FuzzyQueryParameters.FUZZINESS_DEFAULT);
            }
            return getRyftFullQuery(convertingContext, fieldQueryMap);
        } else {
            fieldQueryMap.put(ElasticConverterFuzziness.NAME, 0);
            return getRyftFullQuery(convertingContext, fieldQueryMap);
        }
    }

    private RyftQuery convertFromObject(ElasticConvertingContext convertingContext,
            Map<String, Object> fieldParametersMap) throws ElasticConversionException {
        String currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
        do {
            if (currentName.equals(ElasticConverterShared.ElasticConverterValue.NAME_ALTERNATIVE)) {
                currentName = ElasticConverterShared.ElasticConverterValue.NAME;
            } else if (currentName.equals(ElasticConverterShared.ElasticConverterDateFormat.NAME_ALTERNATIVE)) {
                currentName = ElasticConverterShared.ElasticConverterDateFormat.NAME;
            }
            Object parameterValue = convertingContext.getElasticConverter(currentName)
                    .convert(convertingContext);
            fieldParametersMap.put(currentName, parameterValue);
            currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
        } while (!XContentParser.Token.END_OBJECT.equals(convertingContext.getContentParser().currentToken()));
        return getRyftQuery(convertingContext, fieldParametersMap);
    }

    private RyftQuery convertFromString(ElasticConvertingContext convertingContext,
            Map<String, Object> fieldParametersMap) throws ElasticConversionException {
        String value = ElasticConversionUtil.getString(convertingContext);
        fieldParametersMap.put(ElasticConverterShared.ElasticConverterValue.NAME, value);
        return getRyftQuery(convertingContext, fieldParametersMap);
    }

    private RyftQuery convertFromArray(ElasticConvertingContext convertingContext,
                                        Map<String, Object> fieldParametersMap) throws ElasticConversionException {
        List<String> values = ElasticConversionUtil.getStringArray(convertingContext);
        fieldParametersMap.put(ElasticConverterShared.ElasticConverterValue.NAME, values);
        return getRyftQuery(convertingContext, fieldParametersMap);
    }

    private RyftQuery getRyftFullQuery(ElasticConvertingContext convertingContext, Map<String, Object> fieldQueryMap) throws ElasticConversionException {
        try {
            if (convertingContext.getSearchType().equals(ElasticSearchType.TERM)) {
                TermQueryParameters termQueryParameters = new TermQueryParameters();
                termQueryParameters.setDataType(convertingContext.getDataType());

                String fieldName = convertingContext.getContentParser().currentName();
                if (!fieldName.equals(ALL_FIELDS)) {
                    termQueryParameters.setFieldName(fieldName);
                }

                for (Map.Entry<String, Object> entry : fieldQueryMap.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    switch (key) {
                        case ElasticConverterShared.ElasticConverterValue.NAME:
                            if (value instanceof String) {
                                termQueryParameters.setSearchValue((String) value);
                            } else if (value instanceof List) {
                                termQueryParameters.setSearchArray((List<String>) value);
                            }
                            break;
                        case ElasticConverterShared.ElasticConverterDateFormat.NAME:
                            termQueryParameters.setFormat((String) value);
                            break;
                        case ElasticConverterShared.ElasticConverterSeparator.NAME:
                            termQueryParameters.setSeparator((String) value);
                            break;
                        case ElasticConverterShared.ElasticConverterDecimal.NAME:
                            termQueryParameters.setDecimal((String) value);
                            break;
                    }
                }
                return convertingContext.getQueryFactory().buildTermQuery(termQueryParameters);
            } else {
                FuzzyQueryParameters fieldParameters = new FuzzyQueryParameters();
                fieldParameters.setRyftOperator(convertingContext.getRyftOperator());
                String fieldName = convertingContext.getContentParser().currentName();
                if (!fieldName.equals(ALL_FIELDS)) {
                    fieldParameters.setFieldName(fieldName);
                }
                fieldParameters.setSearchType(convertingContext.getSearchType());

                if (convertingContext.getLine() != null) {
                    fieldParameters.setLine(convertingContext.getLine());
                } else if (convertingContext.getWidth() != null) {
                    fieldParameters.setWidth(convertingContext.getWidth());
                }

                for (Map.Entry<String, Object> entry : fieldQueryMap.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    switch (key) {
                        case ElasticConverterOperator.NAME:
                            fieldParameters.setOperator((RyftLogicalOperator) value);
                            break;
                        case ElasticConverterFuzziness.NAME:
                            fieldParameters.setFuzziness((Integer) value);
                            break;
                        case ElasticConverterMetric.NAME:
                            fieldParameters.setMetric((RyftFuzzyMetric) value);
                            break;
                        case ElasticConverterShared.ElasticConverterValue.NAME:
                            fieldParameters.setSearchValue((String) value);
                            break;
                    }
                }
                return convertingContext.getQueryFactory().buildFuzzyQuery(fieldParameters);
            }

        } catch (IOException ex) {
            throw new ElasticConversionException("Request parsing error");
        }
    }
}
