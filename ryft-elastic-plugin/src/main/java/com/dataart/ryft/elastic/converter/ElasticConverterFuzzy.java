package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import java.io.IOException;
import java.util.Arrays;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

public class ElasticConverterFuzzy implements ElasticConvertingElement {

    @Override
    public String[] names() {
        return new String[]{"match_phrase", "fuzzy"};
    }

    @Override
    public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        try {
            XContentParser parser = convertingContext.getContentParser();
            Token token = parser.nextToken();
            String currentName = parser.currentName();
            if (Token.START_OBJECT.equals(token) && Arrays.asList(names()).contains(currentName)) {
                parser.nextToken();
                return convertingContext.getElasticConverter(ElasticConverterField.NAME)
                        .convert(convertingContext);
            }
        } catch (IOException | ClassNotFoundException ex) {
            throw new ElasticConversionException(ex);
        }
        return null;
    }

}
