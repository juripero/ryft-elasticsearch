package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryComplex;
import static com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryComplex.RyftLogicalOperator.AND;
import com.dataart.ryft.utils.Try;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;

public class ElasticConverterBool implements ElasticConvertingElement {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterBool.class);

    public static class ElasticConverterMust implements ElasticConvertingElement {

        static final String NAME = "must";

        @Override
        public Try<RyftQuery> convert(ElasticConvertingContext convertingContext) {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return Try.apply(() -> {
                XContentParser parser = convertingContext.getContentParser();
                XContentParser.Token token = parser.nextToken();
                String currentName = parser.currentName();
                if (XContentParser.Token.START_ARRAY.equals(token) && NAME.equals(currentName)) {
                    List<RyftQuery> ryftQueryParts = new ArrayList<>();
                    while (!XContentParser.Token.END_ARRAY.equals(token)) {
                        token = parser.currentToken();
                        currentName = parser.currentName();
                        Try<RyftQuery> tryRyftQueryPart;
                        if ((currentName != null) && (XContentParser.Token.FIELD_NAME.equals(token))) {
                            tryRyftQueryPart = convertingContext.getElasticConverter(parser.currentName())
                                    .flatMap(converter -> converter.convert(convertingContext));
                            if (!tryRyftQueryPart.hasError()) {
                                ryftQueryParts.add(tryRyftQueryPart.getResult());
                            }
                        }
                        parser.nextToken();
                    }
                    return new RyftQueryComplex(AND, ryftQueryParts);
                }
                throw new ElasticConversionException();
            });
        }

    }

    static final String NAME = "bool";

    @Override
    public Try<RyftQuery> convert(ElasticConvertingContext convertingContext) {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        return Try.apply(() -> {
            String currentName = getNextElasticPrimitive(convertingContext);
            return convertingContext.getElasticConverter(currentName)
                    .flatMap(converter -> converter.convert(convertingContext))
                    .getResultOrException();

        });
    }

}
