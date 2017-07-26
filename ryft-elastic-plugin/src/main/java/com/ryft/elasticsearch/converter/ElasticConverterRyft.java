package com.ryft.elasticsearch.converter;

import com.ryft.elasticsearch.plugin.PropertiesProvider;
import static com.ryft.elasticsearch.plugin.PropertiesProvider.*;
import java.io.IOException;
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
        public Void convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            Boolean isRyftIntegrationElabled = ElasticConversionUtil.getBoolean(convertingContext);
            convertingContext.getQueryProperties().put(RYFT_INTEGRATION_ENABLED, isRyftIntegrationElabled);
            return null;
        }
    }

    public static class ElasticConverterFiles implements ElasticConvertingElement<Void> {

        final static String NAME = "files";


        @Override
        public Void convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            List<String> ryftFiles = ElasticConversionUtil.getArray(convertingContext);
            convertingContext.getQueryProperties().put(RYFT_FILES_TO_SEARCH, ryftFiles);
            return null;
        }
    }

    public static class ElasticConverterFormat implements ElasticConvertingElement<Void> {

        final static String NAME = "format";

        @Override
        public Void convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            RyftFormat format;
            try {
                format = ElasticConversionUtil.getEnum(convertingContext, RyftFormat.class);
            } catch (Exception e) {
                LOGGER.warn("Unknown format. Please use one of the following formats: json, xml, utf8, raw");
                format = RyftFormat.UNKNOWN_FORMAT;
            }
            convertingContext.getQueryProperties().put(PropertiesProvider.RYFT_FORMAT, format);
            return null;
        }

        public static enum RyftFormat {
            JSON, XML, UTF8, RAW, UNKNOWN_FORMAT
        }

    }

    public static class ElasticConverterCaseSensitive implements ElasticConvertingElement<Void> {

        static final String NAME = "case_sensitive";

        @Override
        public Void convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            Boolean isCaseSensitive = ElasticConversionUtil.getBoolean(convertingContext);
            convertingContext.getQueryProperties().put(RYFT_CASE_SENSITIVE, isCaseSensitive);
            return null;
        }
    }

    @Override
    public Void convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));

        XContentParser parser = convertingContext.getContentParser();
        try {
            parser.nextToken();
        } catch (IOException ex) {
            throw new ElasticConversionException("Request parsing error");
        }
        if (XContentParser.Token.START_OBJECT.equals(parser.currentToken())) {
            String currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
            do {
                convertingContext.getElasticConverter(currentName).convert(convertingContext);
                currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
            } while (!XContentParser.Token.END_OBJECT.equals(convertingContext.getContentParser().currentToken()));
            return null;
        } else {
            throw new ElasticConversionException("Request parsing error");
        }
    }

}
