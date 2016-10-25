package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryFactory;
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
    }

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterField.class);

    private final RyftQueryFactory queryFactory;

    static final String NAME = "field_name";
    private static final String PARAMETER_QUERY = "query";
    private static final String PARAMETER_VALUE = "value";
    private static final String PARAMETER_FUZZINESS = "fuzziness";
    private static final String PARAMETER_METRIC = "metric";

    @Inject
    public ElasticConverterField(RyftQueryFactory injectedQueryFactory) {
        queryFactory = injectedQueryFactory;
    }

    @Override
    public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        try {
            LOGGER.debug("Start field parsing");
            XContentParser parser = convertingContext.getContentParser();
            Token token = parser.nextToken();
            if (Token.START_OBJECT.equals(token)) {
                ElasticFieldParameters fieldParameters = new ElasticFieldParameters();
                fieldParameters.fieldName = parser.currentName();
                while (!Token.END_OBJECT.equals(token)) {
                    token = parser.nextToken();
                    //ignore unknown parameters
                    parseSearchText(fieldParameters, parser);
                    parseMetric(fieldParameters, parser);
                    parseFuzziness(fieldParameters, parser);
                }
                return queryFactory.buildFuzzyQuery(fieldParameters.metric,
                        fieldParameters.fuzziness, fieldParameters.searchText, fieldParameters.fieldName);
            }
        } catch (Exception ex) {
            throw new ElasticConversionException(ex);
        }
        return null;
    }

    private void parseSearchText(ElasticFieldParameters fieldParameters, XContentParser parser) throws IOException {
        if (PARAMETER_QUERY.equals(parser.currentName())
                || PARAMETER_VALUE.equals(parser.currentName())) {
            if (Token.FIELD_NAME.equals(parser.currentToken())) {
                parser.nextToken();
            }
            if (Token.VALUE_STRING.equals(parser.currentToken())) {
                String searchText = parser.text();
                LOGGER.debug("Find search text: {}", searchText);
                fieldParameters.searchText = searchText;
            }
        }
    }

    private void parseMetric(ElasticFieldParameters fieldParameters, XContentParser parser) throws IOException {
        if (PARAMETER_METRIC.equals(parser.currentName())) {
            if (Token.FIELD_NAME.equals(parser.currentToken())) {
                parser.nextToken();
            }
            if (Token.VALUE_STRING.equals(parser.currentToken())) {
                String metricString = parser.text().toUpperCase();
                LOGGER.debug("Find metric: {}", metricString);
                try {
                    RyftFuzzyMetric metric = RyftFuzzyMetric.valueOf(metricString);
                    fieldParameters.metric = metric;
                } catch (Exception e) {
                    throw new ElasticConversionException(String.format("Unknown metric: %s", metricString), e);
                }
            }
        }
    }

    private void parseFuzziness(ElasticFieldParameters fieldParameters, XContentParser parser) throws IOException {
        if (PARAMETER_FUZZINESS.equals(parser.currentName())) {
            if (Token.FIELD_NAME.equals(parser.currentToken())) {
                parser.nextToken();
            }
            String fuzzinessString = parser.text();
            LOGGER.debug("Find fuzziness: {}", fuzzinessString);
            try {
                Integer fuzziness = Integer.parseInt(fuzzinessString);
                fieldParameters.fuzziness = fuzziness;
            } catch (Exception e) {
                throw new ElasticConversionException(String.format("Can not parse fuzziness: %s", fuzzinessString), e);
            }
        }
    }

}
