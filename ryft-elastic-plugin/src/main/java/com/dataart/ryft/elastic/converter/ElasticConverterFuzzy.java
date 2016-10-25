package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import java.io.IOException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

public class ElasticConverterFuzzy implements ElasticConvertingElement {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterFuzzy.class);

    final static String NAME1 = "match_phrase";
    final static String NAME2 = "fuzzy";

    @Override
    public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        try {
            LOGGER.debug("Start fuzzy parsing");
            XContentParser parser = convertingContext.getContentParser();
            Token token = parser.nextToken();
            String currentName = parser.currentName();
            if (Token.START_OBJECT.equals(token)
                    && (currentName.equals(NAME1) || currentName.equals(NAME2))) {
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
