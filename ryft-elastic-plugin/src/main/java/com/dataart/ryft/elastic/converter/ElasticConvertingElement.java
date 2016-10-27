package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.utils.Try;
import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentParser;

public interface ElasticConvertingElement {

    Try<RyftQuery> convert(ElasticConvertingContext convertingContext);

    default String getNextElasticPrimitive(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        try {
            XContentParser parser = convertingContext.getContentParser();
            XContentParser.Token token;
            do {
                token = parser.nextToken();
            } while (!XContentParser.Token.FIELD_NAME.equals(token));
            return  parser.currentName();
        } catch (IOException ex) {
            throw new ElasticConversionException("Elastic request parsing error.", ex);
        }
    }

}
