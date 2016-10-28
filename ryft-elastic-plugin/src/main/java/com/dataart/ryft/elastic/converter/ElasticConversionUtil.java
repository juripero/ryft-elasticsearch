package com.dataart.ryft.elastic.converter;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentParser;

public abstract class ElasticConversionUtil {

    static String getNextElasticPrimitive(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        try {
            XContentParser parser = convertingContext.getContentParser();
            XContentParser.Token token;
            do {
                token = parser.nextToken();
            } while (!XContentParser.Token.FIELD_NAME.equals(token));
            return parser.currentName();
        } catch (IOException ex) {
            throw new ElasticConversionException("Elastic request parsing error.", ex);
        }
    }

    static String getString(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        XContentParser parser = convertingContext.getContentParser();
        try {
            if (XContentParser.Token.FIELD_NAME.equals(parser.currentToken())) {
                parser.nextToken();
            }
            if (XContentParser.Token.VALUE_NULL.equals(parser.currentToken())) {
                throw new ElasticConversionException(
                        String.format("Value %s should not be null.", parser.currentName()));
            }
            return parser.text();
        } catch (IOException ex) {
            throw new ElasticConversionException("Elastic request parsing error.", ex);
        }
    }

    static Integer getInteger(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        String value = getString(convertingContext);
        try {
            return Integer.parseInt(value);
        } catch (RuntimeException ex) {
            throw new ElasticConversionException(
                    String.format("Can not parse value \"%s\" as Integer.", value), ex);
        }
    }

    static <T extends Enum<T>> T getEnum(ElasticConvertingContext convertingContext, Class<T> enumType) throws ElasticConversionException {
        String value = getString(convertingContext).toUpperCase();
        try {
            return Enum.valueOf(enumType, value);
        } catch (RuntimeException ex) {
            throw new ElasticConversionException(
                    String.format("Can not parse value \"%s\" as %s.", value, enumType.getSimpleName()), ex);
        }
    }

}
