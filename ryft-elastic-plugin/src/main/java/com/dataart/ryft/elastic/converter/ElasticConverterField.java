package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ElasticConvertingContext.ElasticSearchType;
import com.dataart.ryft.elastic.converter.entities.FuzzyQueryParameters;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryComplex.RyftLogicalOperator;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryFactory;
import com.dataart.ryft.utils.Try;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;

public class ElasticConverterField implements ElasticConvertingElement<RyftQuery> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterField.class);

    public static class ElasticConverterValue implements ElasticConvertingElement<String> {

        public static final String NAME = "value";

        private static final String NAME_ALTERNATIVE = "query";

        @Override
        public Try<String> convert(ElasticConvertingContext convertingContext) {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return Try.apply(() -> {
                return ElasticConversionUtil.getString(convertingContext);
            });
        }
    }

    public static class ElasticConverterMetric implements ElasticConvertingElement<RyftFuzzyMetric> {

        public static final String NAME = "metric";

        @Override
        public Try<RyftFuzzyMetric> convert(ElasticConvertingContext convertingContext) {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return Try.apply(() -> {
                return ElasticConversionUtil.getEnum(convertingContext, RyftFuzzyMetric.class);
            });
        }
    }

    public static class ElasticConverterFuzziness implements ElasticConvertingElement<Integer> {

        public static final String NAME = "fuzziness";
        private final String VALUE_FUZZINESS_AUTO = "auto";

        @Override
        public Try<Integer> convert(ElasticConvertingContext convertingContext) {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return Try.apply(() -> {
                String fuzziness = ElasticConversionUtil.getString(convertingContext);
                if (fuzziness.toLowerCase().equals(VALUE_FUZZINESS_AUTO)) {
                    return RyftQueryFactory.FUZZYNESS_AUTO_VALUE;
                } else {
                    return ElasticConversionUtil.getInteger(convertingContext);
                }
            });
        }
    }

    public static class ElasticConverterOperator implements ElasticConvertingElement<RyftLogicalOperator> {

        public static final String NAME = "operator";

        @Override
        public Try<RyftLogicalOperator> convert(ElasticConvertingContext convertingContext) {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return Try.apply(() -> {
                return ElasticConversionUtil.getEnum(convertingContext, RyftLogicalOperator.class);
            });
        }
    }

    public static class ElasticConverterType implements ElasticConvertingElement<Void> {

        public static final String NAME = "type";
        
        private final String TYPE_PHRASE = "phrase";

        @Override
        public Try<Void> convert(ElasticConvertingContext convertingContext) {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return Try.apply(() -> {
                String type = ElasticConversionUtil.getString(convertingContext);
                if (TYPE_PHRASE.equals(type.toLowerCase())
                        && ElasticSearchType.MATCH.equals(convertingContext.getSearchType())) {
                    convertingContext.setSearchType(ElasticSearchType.MATCH_PHRASE);
                }
                return null;
            });
        }
    }

    static final String NAME = "field_name";

    @Override
    public Try<RyftQuery> convert(ElasticConvertingContext convertingContext) {
        LOGGER.debug("Start field primitive parsing");
        return Try.apply(() -> {
            XContentParser parser = convertingContext.getContentParser();
            parser.nextToken();
            Map<String, Object> fieldParametersMap = new HashMap<>();
            switch (parser.currentToken()) {
                case START_OBJECT:
                    return convertFromObject(convertingContext, fieldParametersMap);
                case VALUE_STRING:
                    return convertFromString(convertingContext, fieldParametersMap);
                default:
                    throw new ElasticConversionException("Request parsing error");
            }
        });
    }

    private RyftQuery convertFromObject(ElasticConvertingContext convertingContext,
            Map<String, Object> fieldParametersMap) throws Exception {
        String currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
        do {
            if (currentName.equals(ElasticConverterValue.NAME_ALTERNATIVE)) {
                currentName = ElasticConverterValue.NAME;
            }
            Object parameterValue = convertingContext.getElasticConverter(currentName)
                    .map(converter -> converter.convert(convertingContext).getResultOrException())
                    .getResultOrException();
            fieldParametersMap.put(currentName, parameterValue);
            currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
        } while (!XContentParser.Token.END_OBJECT.equals(convertingContext.getContentParser().currentToken()));
        return getRyftQuery(convertingContext, fieldParametersMap);
    }

    private RyftQuery convertFromString(ElasticConvertingContext convertingContext,
            Map<String, Object> fieldParametersMap) throws Exception {
        String value = ElasticConversionUtil.getString(convertingContext);
        fieldParametersMap.put(ElasticConverterValue.NAME, value);
        return getRyftQuery(convertingContext, fieldParametersMap);
    }

    private RyftQuery getRyftQuery(ElasticConvertingContext convertingContext, Map<String, Object> fieldQueryMap) throws IOException, ElasticConversionException {
        if (fieldQueryMap.containsKey(ElasticConverterMetric.NAME)
                || fieldQueryMap.containsKey(ElasticConverterFuzziness.NAME)
                || ElasticConvertingContext.ElasticSearchType.FUZZY.equals(convertingContext.getSearchType())) {
            if (!fieldQueryMap.containsKey(ElasticConverterMetric.NAME)) {
                fieldQueryMap.put(ElasticConverterMetric.NAME, FuzzyQueryParameters.METRIC_DEFAULT);
            }
            if (!fieldQueryMap.containsKey(ElasticConverterFuzziness.NAME)) {
                fieldQueryMap.put(ElasticConverterFuzziness.NAME, FuzzyQueryParameters.FUZZINESS_DEFAULT);
            }
            return getRyftFuzzyQuery(convertingContext, fieldQueryMap);
        } else {
            fieldQueryMap.put(ElasticConverterFuzziness.NAME, 0);
            return getRyftFuzzyQuery(convertingContext, fieldQueryMap);
        }
    }

    private RyftQuery getRyftFuzzyQuery(ElasticConvertingContext convertingContext, Map<String, Object> fieldQueryMap) throws IOException, ElasticConversionException {
        FuzzyQueryParameters fieldParameters = new FuzzyQueryParameters();
        fieldParameters.setRyftOperator(convertingContext.getRyftOperator());
        fieldParameters.setFieldName(convertingContext.getContentParser().currentName());
        fieldParameters.setSearchType(convertingContext.getSearchType());
        fieldQueryMap.forEach((key, value) -> {
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
                case ElasticConverterValue.NAME:
                    fieldParameters.setSearchValue((String) value);
                    break;
            }
        });
        return convertingContext.getQueryFactory().buildFuzzyQuery(fieldParameters);
    }
}
