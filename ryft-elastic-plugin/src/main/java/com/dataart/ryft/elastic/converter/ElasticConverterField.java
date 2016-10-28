package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ElasticConvertingContext.ElasticSearchType;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryComplex.RyftLogicalOperator;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryFactory;
import com.dataart.ryft.utils.Try;
import java.io.IOException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

public class ElasticConverterField implements ElasticConvertingElement {

    private class ElasticFieldParameters {

        private RyftFuzzyMetric metric = null;
        private Integer fuzziness = null;
        private String searchText = null;
        private String fieldName = null;
        private RyftLogicalOperator operator = null;
    }

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterField.class);

    private final RyftQueryFactory queryFactory;

    static final String NAME = "field_name";
    private static final String PARAMETER_QUERY = "query";
    private static final String PARAMETER_VALUE = "value";
    private static final String PARAMETER_FUZZINESS = "fuzziness";
    private static final String PARAMETER_METRIC = "metric";
    private static final String PARAMETER_OPERATOR = "operator";
    private static final String VALUE_FUZZINESS_AUTO = "auto";

    @Inject
    public ElasticConverterField(RyftQueryFactory injectedQueryFactory) {
        queryFactory = injectedQueryFactory;
    }

    @Override
    public Try<RyftQuery> convert(ElasticConvertingContext convertingContext) {
        LOGGER.debug("Start field primitive parsing");
        return Try.apply(() -> {
            XContentParser parser = convertingContext.getContentParser();
            Token token = parser.nextToken();
            if (Token.START_OBJECT.equals(token)) {
                ElasticFieldParameters fieldParameters = new ElasticFieldParameters();
                fieldParameters.fieldName = parser.currentName();
                while (!Token.END_OBJECT.equals(token)) {
                    token = parser.nextToken();
                    //ignore unknown parameters
                    parseSearchText(fieldParameters, convertingContext);
                    parseMetric(fieldParameters, convertingContext);
                    parseFuzziness(fieldParameters, convertingContext);
                    parseOperator(fieldParameters, convertingContext);
                }
                ElasticSearchType searchType = convertingContext.getSearchType();
                if (ElasticSearchType.FUZZY.equals(searchType)
                        || ElasticSearchType.MATCH_PHRASE.equals(searchType)) {
                    return queryFactory.buildFuzzyQuery(fieldParameters.searchText,
                            fieldParameters.fieldName, fieldParameters.metric, fieldParameters.fuzziness);
                }
                if (ElasticSearchType.MATCH.equals(searchType)) {
                    return queryFactory.buildFuzzyTokenizedQuery(fieldParameters.searchText,
                            fieldParameters.fieldName, fieldParameters.operator, fieldParameters.metric, fieldParameters.fuzziness);
                }

            }
            throw new ElasticConversionException();
        });
    }

    private void parseSearchText(ElasticFieldParameters fieldParameters,
            ElasticConvertingContext convertingContext) throws ElasticConversionException, IOException {
        String name = convertingContext.getContentParser().currentName();
        if (PARAMETER_QUERY.equals(name)
                && ElasticSearchType.MATCH_PHRASE.equals(convertingContext.getSearchType())) {
            fieldParameters.searchText = ElasticConversionUtil.getString(convertingContext);
        }
        if (PARAMETER_VALUE.equals(name)
                && ElasticSearchType.FUZZY.equals(convertingContext.getSearchType())) {
            fieldParameters.searchText = ElasticConversionUtil.getString(convertingContext);
        }
    }

    private void parseMetric(ElasticFieldParameters fieldParameters,
            ElasticConvertingContext convertingContext) throws ElasticConversionException {
        RyftFuzzyMetric metric = null;
        try {
            String name = convertingContext.getContentParser().currentName();
            if (PARAMETER_METRIC.equals(name)) {
                fieldParameters.metric = ElasticConversionUtil.getEnum(convertingContext, RyftFuzzyMetric.class);
            }
        } catch (IOException | ElasticConversionException ex) {
            throw new ElasticConversionCriticalException(
                    String.format("Can not covert metric value: %s", metric), ex);
        }
    }

    private void parseFuzziness(ElasticFieldParameters fieldParameters,
            ElasticConvertingContext convertingContext) throws IOException, ElasticConversionException {
        String name = convertingContext.getContentParser().currentName();
        if (PARAMETER_FUZZINESS.equals(name)) {
            String fuzziness = ElasticConversionUtil.getString(convertingContext);
            if (fuzziness.toLowerCase().equals(VALUE_FUZZINESS_AUTO)) {
                fieldParameters.fuzziness = RyftQueryFactory.FUZZYNESS_AUTO_VALUE;
            } else {
                fieldParameters.fuzziness = ElasticConversionUtil.getInteger(convertingContext);
            }
        }
    }

    private void parseOperator(ElasticFieldParameters fieldParameters,
            ElasticConvertingContext convertingContext) throws IOException, ElasticConversionException {
        String name = convertingContext.getContentParser().currentName();
        if (PARAMETER_OPERATOR.equals(name)) {
            fieldParameters.operator = ElasticConversionUtil.getEnum(convertingContext, RyftLogicalOperator.class);
        }
    }

}
