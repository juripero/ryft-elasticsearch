package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.plugin.PropertiesProvider;
import static com.dataart.ryft.elastic.plugin.PropertiesProvider.*;
import com.dataart.ryft.utils.Try;
import java.util.List;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;

public class ElasticConverterRyft implements ElasticConvertingElement<Void> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterBool.class);
    static final String NAME = "ryft";

    public static class ElasticConverterEnabled implements ElasticConvertingElement<Void> {

        static final String NAME = "enabled";

        @Override
        public Try<Void> convert(ElasticConvertingContext convertingContext) {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return Try.apply(() -> {
                Boolean isRyftIntegrationElabled = ElasticConversionUtil.getBoolean(convertingContext);
                convertingContext.getQueryProperties().put(RYFT_INTEGRATION_ENABLED, isRyftIntegrationElabled);
                return null;
            });
        }
    }

    public static class ElasticConverterFiles implements ElasticConvertingElement<Void> {

        final static String NAME = "files";

        @Override
        public Try<Void> convert(ElasticConvertingContext convertingContext) {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return Try.apply(() -> {
                List<String> ryftFiles = ElasticConversionUtil.getArray(convertingContext);
                convertingContext.getQueryProperties().put(RYFT_FILES_TO_SEARCH, ryftFiles);
                return null;
            });
        }
    }

    public static class ElasticConverterFormat implements ElasticConvertingElement<Void> {

        final static String NAME = "format";

        private enum RyftFormat {
            JSON, XML;
        }

        @Override
        public Try<Void> convert(ElasticConvertingContext convertingContext) {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return Try.apply(() -> {
                RyftFormat format = ElasticConversionUtil.getEnum(convertingContext, RyftFormat.class);
                convertingContext.getQueryProperties().put(PropertiesProvider.RYFT_FORMAT, format.name().toLowerCase());
                return null;
            });
        }
    }

    @Override
    public Try<Void> convert(ElasticConvertingContext convertingContext) {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        return Try.apply(() -> {
            XContentParser parser = convertingContext.getContentParser();
            parser.nextToken();
            if (XContentParser.Token.START_OBJECT.equals(parser.currentToken())) {
                String currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
                do {
                    convertingContext.getElasticConverter(currentName)
                            .map(converter -> converter.convert(convertingContext).getResultOrException())
                            .getResultOrException();
                    currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
                } while (!XContentParser.Token.END_OBJECT.equals(convertingContext.getContentParser().currentToken()));
                return null;
            } else {
                throw new ElasticConversionException("Request parsing error");
            }
        });
    }

}
