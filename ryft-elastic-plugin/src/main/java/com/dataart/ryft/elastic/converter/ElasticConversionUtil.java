package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.utils.Try;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentParser;

public abstract class ElasticConversionUtil {

    static String getNextElasticPrimitive(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        try {
            XContentParser parser = convertingContext.getContentParser();
            String currentPrimitive = parser.currentName();
            if (currentPrimitive == null) {
                parser.nextToken();
            } else {
                do {
                    parser.nextToken();
                } while (currentPrimitive.equals(parser.currentName()));
            }
            return parser.currentName();
        } catch (IOException ex) {
            throw new ElasticConversionException("Elastic request parsing error.", ex);
        }
    }

    static Boolean isClosePrimitive(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        XContentParser parser = convertingContext.getContentParser();
        return (parser.currentToken().equals(XContentParser.Token.END_OBJECT)
                || parser.currentToken().equals(XContentParser.Token.END_ARRAY));
    }

    static <T> List<T> getArray(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        XContentParser parser = convertingContext.getContentParser();
        try {
            if (XContentParser.Token.FIELD_NAME.equals(parser.currentToken())) {
                parser.nextToken();
            }
            List<T> result = new ArrayList<>();
            if (XContentParser.Token.START_ARRAY.equals(parser.currentToken())) {
                parser.nextToken();
                while (!XContentParser.Token.END_ARRAY.equals(parser.currentToken())) {
                    result.add(getObject(convertingContext));
                    getNextElasticPrimitive(convertingContext);
                }
                return result;
            }
        } catch (IOException | ElasticConversionException ex) {
            throw new ElasticConversionException("Elastic request parsing error.", ex);
        }
        throw new ElasticConversionException("Can not extract array.");
    }

    static <T> T getObject(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        XContentParser parser = convertingContext.getContentParser();
        try {
            if (XContentParser.Token.FIELD_NAME.equals(parser.currentToken())) {
                parser.nextToken();
            }
            switch (parser.currentToken()) {
                case START_OBJECT:
                    String currentName = getNextElasticPrimitive(convertingContext);
                    if ((currentName != null) && (XContentParser.Token.FIELD_NAME.equals(parser.currentToken()))) {
                        Try<T> tryArrayElement = convertingContext.getElasticConverter(currentName)
                                .map(converter -> (T) converter.convert(convertingContext).getResultOrException());
                        parser.nextToken();
                        return tryArrayElement.getResultOrException();
                    }
                case VALUE_STRING:
                    return (T) getString(convertingContext);
                case VALUE_NUMBER:
                    return (T) getInteger(convertingContext);
                case VALUE_BOOLEAN:
                    return (T) getBoolean(convertingContext);
            }
        } catch (Exception ex) {
            throw new ElasticConversionException("Elastic request parsing error.", ex);
        }
        throw new ElasticConversionException("Can not extract object.");
    }

    static <T> T getObject(ElasticConvertingContext convertingContext, ElasticConvertingElement<T> converter) throws Exception {
        XContentParser parser = convertingContext.getContentParser();
        try {
            if (XContentParser.Token.FIELD_NAME.equals(parser.currentToken())) {
                parser.nextToken();
            }
            getNextElasticPrimitive(convertingContext);
            T result = converter.convert(convertingContext).getResultOrException();
            convertingContext.getContentParser().nextToken();
            return result;
        } catch (Exception ex) {
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

    static Boolean getBoolean(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        String value = getString(convertingContext);
        try {
            return Boolean.parseBoolean(value);
        } catch (RuntimeException ex) {
            throw new ElasticConversionException(
                    String.format("Can not parse value \"%s\" as Boolean.", value), ex);
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
