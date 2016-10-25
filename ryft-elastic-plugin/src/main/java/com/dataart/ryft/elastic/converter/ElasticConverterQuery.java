package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import java.io.IOException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

public class ElasticConverterQuery implements ElasticConvertingElement {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterQuery.class);

    static final String NAME = "query";

    @Override
    public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        try {
            LOGGER.debug("Start \"query\" parsing");
            XContentParser parser = convertingContext.getContentParser();
            Token token = parser.nextToken();
            String currentName = parser.currentName();
            if (Token.START_OBJECT.equals(token) && NAME.equals(currentName)) {
                parser.nextToken();
                currentName = parser.currentName();
                return convertingContext.getElasticConverter(currentName).convert(convertingContext);
            }
        } catch (IOException | ClassNotFoundException ex) {
            throw new ElasticConversionException(ex);
        }
        return null;
    }
}
