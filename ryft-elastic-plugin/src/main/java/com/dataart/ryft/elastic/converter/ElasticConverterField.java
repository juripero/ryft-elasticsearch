package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

public class ElasticConverterField implements ElasticConvertingElement {

    static final String NAME = "field_name";
    private static final String PARAMETER_QUERY = "query";
    private static final String PARAMETER_VALUE = "value";
    private static final String PARAMETER_FUZZINESS = "fuzziness";
    private static final String PARAMETER_METRIC = "metric";

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        convertingContext.clean();
        try {
            XContentParser parser = convertingContext.getContentParser();
            Token token = parser.nextToken();
            convertingContext.setFieldName(parser.currentName());
            if (Token.START_OBJECT.equals(token)) {
                while (!Token.END_OBJECT.equals(token)) {
                    token = parser.nextToken();
                    //ignore unknown parameters
                    parseSearchText(convertingContext);
                    parseMetric(convertingContext);
                    parseFuzziness(convertingContext);
                }
                return convertingContext.constructFuzzyQuery();
            }
        } catch (Exception ex) {
            throw new ElasticConversionException(ex);
        }
        return null;
    }

    private void parseSearchText(ElasticConvertingContext convertingContext) throws IOException {
        XContentParser parser = convertingContext.getContentParser();
        if (convertingContext.getSearchText() == null) {
            if (PARAMETER_QUERY.equals(parser.currentName())
                    || PARAMETER_VALUE.equals(parser.currentName())) {
                if (Token.FIELD_NAME.equals(parser.currentToken())) {
                    parser.nextToken();
                }
                if (Token.VALUE_STRING.equals(parser.currentToken())) {
                    convertingContext.setSearchText(parser.text());
                }
            }
        }
    }

    private void parseMetric(ElasticConvertingContext convertingContext) throws IOException {
        XContentParser parser = convertingContext.getContentParser();
        if (PARAMETER_METRIC.equals(parser.currentName())) {
            if (Token.FIELD_NAME.equals(parser.currentToken())) {
                parser.nextToken();
            }
            if (Token.VALUE_STRING.equals(parser.currentToken())) {
                String metricString = parser.text().toUpperCase();
                try {
                    RyftFuzzyMetric metric = RyftFuzzyMetric.valueOf(metricString);
                    convertingContext.setMetric(metric);
                } catch (Exception e) {
                    throw new ElasticConversionException(String.format("Unknown metric: %s", metricString), e);
                }
            }
        }
    }

    private void parseFuzziness(ElasticConvertingContext convertingContext) throws IOException {
        XContentParser parser = convertingContext.getContentParser();
        if (PARAMETER_FUZZINESS.equals(parser.currentName())) {
            if (Token.FIELD_NAME.equals(parser.currentToken())) {
                parser.nextToken();
            }
            String fuzzinessString = parser.text();
            try {
                Integer fuzziness = Integer.parseInt(fuzzinessString);
                convertingContext.setFuzziness(fuzziness);
            } catch (Exception e) {
                throw new ElasticConversionException(String.format("Can not parse fuzziness: %s", fuzzinessString), e);
            }
        }
    }
}
