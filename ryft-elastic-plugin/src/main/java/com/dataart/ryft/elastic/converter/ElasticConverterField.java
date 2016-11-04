package com.dataart.ryft.elastic.converter;

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

        private static final String PARAMETER_NAME = "query";

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

    static final String NAME = "field_name";
    private static final String PARAMETER_QUERY = "query";
    private static final String PARAMETER_VALUE = "value";
    private static final String PARAMETER_FUZZINESS = "fuzziness";
    private static final String PARAMETER_METRIC = "metric";
    private static final String PARAMETER_OPERATOR = "operator";
    private static final String VALUE_FUZZINESS_AUTO = "auto";

    @Override
    public Try<RyftQuery> convert(ElasticConvertingContext convertingContext) {
        LOGGER.debug("Start field primitive parsing");
        return Try.apply(() -> {
            XContentParser parser = convertingContext.getContentParser();
            parser.nextToken();
            Map<String, Object> fieldParametersMap = new HashMap<>();
            if (XContentParser.Token.START_OBJECT.equals(parser.currentToken())) {
                String currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
                do {
                    if (currentName.equals(ElasticConverterValue.PARAMETER_NAME)) {
                        currentName = ElasticConverterValue.NAME;
                    }
                    Object parameterValue = convertingContext.getElasticConverter(currentName)
                            .flatMap(converter -> converter.convert(convertingContext))
                            .getResultOrException();
                    fieldParametersMap.put(currentName, parameterValue);
                    currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
                } while (!XContentParser.Token.END_OBJECT.equals(parser.currentToken()));
                return getRyftQuery(convertingContext, fieldParametersMap);
            }
            throw new ElasticConversionException();
        });
    }

    private RyftQuery getRyftQuery(ElasticConvertingContext convertingContext, Map<String, Object> fieldQueryMap) throws IOException, ElasticConversionException {
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
