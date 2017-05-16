package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConversionException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RyftExpressionDate extends RyftExpressionRange {

    public static final String DEFAULT_FORMAT = "yyyy-MM-dd";

    public RyftExpressionDate(Date valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, Date valueB, String format) throws ElasticConversionException {
        super("DATE");
        DateFormat dateFormat = getDateFormat(format);
        variableName = getVariableName(format);
        this.valueA = dateFormat.format(valueA);
        this.operatorA = operatorA;
        this.valueB = Optional.ofNullable(valueB).map(dateFormat::format);
        this.operatorB = Optional.ofNullable(operatorB);
        constructValue();
    }

    public RyftExpressionDate(Date valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, Date valueB) throws ElasticConversionException {
        this(valueA, operatorA, operatorB, valueB, DEFAULT_FORMAT);
    }

    public RyftExpressionDate(Date valueA, RyftOperatorCompare operatorA, String format) throws ElasticConversionException {
        this(valueA, operatorA, null, null, format);
    }

    public RyftExpressionDate(Date valueA, RyftOperatorCompare operatorA) throws ElasticConversionException {
        this(valueA, operatorA, null, null, DEFAULT_FORMAT);
    }

    public static DateFormat getDateFormat(String format) {
        String datePattern = getDatePattern(format);
        return (datePattern == null) ? null : new SimpleDateFormat(datePattern);
    }

    private String getVariableName(String format) throws ElasticConversionException {
        String datePattern = getDatePattern(format);
        String separator = getSeparator(datePattern);
        return datePattern.toUpperCase().replace(separator.toUpperCase(), separator);
    }

    private String getSeparator(String datePattern) throws ElasticConversionException {
        Pattern pattern = Pattern.compile("[^yMd]");
        Matcher matcher = pattern.matcher(datePattern);
        String separator = "";
        Boolean isEqual = true;
        Integer count = 0;
        while (matcher.find()) {
            if (count > 0) {
                isEqual &= separator.equals(matcher.group());
            }
            separator = matcher.group();
            count++;
        }
        if (isEqual && (count == 2)) {
            return separator;
        } else {
            throw new ElasticConversionException("Date pattern should have consistent separator");
        }
    }

    private static String getDatePattern(String format) {
        Pattern pattern = Pattern.compile("(y{4}|y{2}).(M{2}).(d{2})|(M{2}).(d{2}).(y{4}|y{2})|(d{2}).(M{2}).(y{4}|y{2})");
        Matcher matcher = pattern.matcher(format);
        if (matcher.find()) {
            return matcher.group();
        }
        return null;
    }
}
