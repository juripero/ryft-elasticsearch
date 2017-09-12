package com.ryft.elasticsearch.converter;

import com.ryft.elasticsearch.plugin.RyftProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentParser;

public abstract class ElasticConversionUtil {

    public static String getNextElasticPrimitive(ElasticConvertingContext convertingContext) throws ElasticConversionException {
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

    public static RyftProperties getMap(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        XContentParser parser = convertingContext.getContentParser();
        try {
            if (XContentParser.Token.FIELD_NAME.equals(parser.currentToken())) {
                parser.nextToken();
            }
            RyftProperties result = new RyftProperties();
            if (XContentParser.Token.START_OBJECT.equals(parser.currentToken())) {
                parser.nextToken();
                while (!XContentParser.Token.END_OBJECT.equals(parser.currentToken())) {
                    String key = parser.currentName();
                    parser.nextToken();
                    Object value;
                    switch (parser.currentToken()) {
                        case START_OBJECT:
                            value = getMap(convertingContext);
                            break;
                        case VALUE_STRING:
                            value = getString(convertingContext);
                            break;
                        case VALUE_NUMBER:
                            value = getNumber(convertingContext);
                            break;
                        case VALUE_BOOLEAN:
                            value = getBoolean(convertingContext);
                            break;
                        case START_ARRAY:
                            value = getArray(convertingContext);
                            break;
                        default:
                            value = null;
                    }
                    result.put(key, value);
                    parser.nextToken();
                }
                return result;
            }
        } catch (IOException ex) {
            throw new ElasticConversionException("Elastic request parsing error.", ex);
        }
        throw new ElasticConversionException("Can not extract map.");
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
                    result.add((T) getObject(convertingContext));
                    getNextElasticPrimitive(convertingContext);
                }
                return result;
            }
        } catch (IOException | ElasticConversionException ex) {
            throw new ElasticConversionException("Elastic request parsing error.", ex);
        }
        throw new ElasticConversionException("Can not extract array.");
    }

    static List<String> getStringArray(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        XContentParser parser = convertingContext.getContentParser();
        try {
            if (XContentParser.Token.FIELD_NAME.equals(parser.currentToken())) {
                parser.nextToken();
            }
            List<String> result = new ArrayList<>();
            if (XContentParser.Token.START_ARRAY.equals(parser.currentToken())) {
                parser.nextToken();
                while (!XContentParser.Token.END_ARRAY.equals(parser.currentToken())) {
                    result.add(getString(convertingContext));
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
                        T arrayElement = (T) convertingContext.getElasticConverter(currentName).convert(convertingContext);
                        parser.nextToken();
                        return arrayElement;
                    }
                case VALUE_STRING:
                    return (T) getString(convertingContext);
                case VALUE_NUMBER:
                    return (T) getNumber(convertingContext);
                case VALUE_BOOLEAN:
                    return (T) getBoolean(convertingContext);
            }
        } catch (IOException | ElasticConversionException ex) {
            throw new ElasticConversionException("Elastic request parsing error.", ex);
        }
        throw new ElasticConversionException("Can not extract object.");
    }

    static <T> T getObject(ElasticConvertingContext convertingContext, ElasticConvertingElement<T> converter) throws ElasticConversionException {
        XContentParser parser = convertingContext.getContentParser();
        try {
            if (XContentParser.Token.FIELD_NAME.equals(parser.currentToken())) {
                parser.nextToken();
            }
            getNextElasticPrimitive(convertingContext);
            T result = converter.convert(convertingContext);
            parser.nextToken();
            return result;
        } catch (IOException | ElasticConversionException ex) {
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

    static Number getNumber(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        String value = getString(convertingContext);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ex) {
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ex) {
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException ex) {
            throw new ElasticConversionException(
                    String.format("Can not parse value \"%s\" as Number.", value), ex);
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