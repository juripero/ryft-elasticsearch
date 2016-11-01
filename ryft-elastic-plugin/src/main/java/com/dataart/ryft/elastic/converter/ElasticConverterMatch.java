package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ElasticConvertingContext.ElasticSearchType;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.utils.Try;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;

public class ElasticConverterMatch implements ElasticConvertingElement<RyftQuery> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterFuzzy.class);

    static final String NAME = "match";

    @Override
    public Try<RyftQuery> convert(ElasticConvertingContext convertingContext) {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        return Try.apply(() -> {
            XContentParser parser = convertingContext.getContentParser();
            XContentParser.Token token = parser.nextToken();
            String currentName = parser.currentName();
            if (XContentParser.Token.START_OBJECT.equals(token) && NAME.equals(currentName)) {
                parser.nextToken();
                convertingContext.setSearchType(ElasticSearchType.MATCH);
                return convertingContext.getElasticConverter(ElasticConverterField.NAME)
                        .flatMap(converter -> (Try<RyftQuery>) converter.convert(convertingContext))
                        .getResultOrException();
            }
            throw new ElasticConversionException();
        });
    }

}
