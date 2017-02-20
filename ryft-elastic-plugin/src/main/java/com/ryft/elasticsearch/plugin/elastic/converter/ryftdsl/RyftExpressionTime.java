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

public class RyftExpressionTime extends RyftExpressionRange {

    public static final String DEFAULT_FORMAT = "HH:mm:ss";

    public RyftExpressionTime(Date valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, Date valueB, String format) throws ElasticConversionException {
        super("TIME");
        DateFormat timeFormat = getTimeFormat(format);
        variableName = getVariableName(format);
        this.valueA = timeFormat.format(valueA);
        this.operatorA = operatorA;
        this.valueB = Optional.ofNullable(valueB).map(timeFormat::format);
        this.operatorB = Optional.ofNullable(operatorB);
        // milliseconds should have 2 digits
        if (this.valueA.length() > 11) {
            this.valueA = this.valueA.substring(0, 11);
            this.valueB = this.valueB.map(s -> s.substring(0, 11));
        }
        constructValue();
    }

    public RyftExpressionTime(Date valueA, RyftOperatorCompare operatorA, RyftOperatorCompare operatorB, Date valueB) throws ElasticConversionException {
        this(valueA, operatorA, operatorB, valueB, DEFAULT_FORMAT);
    }

    public RyftExpressionTime(Date valueA, RyftOperatorCompare operatorA, String format) throws ElasticConversionException {
        this(valueA, operatorA, null, null, format);
    }

    public RyftExpressionTime(Date valueA, RyftOperatorCompare operatorA) throws ElasticConversionException {
        this(valueA, operatorA, null, null, DEFAULT_FORMAT);
    }

    public static DateFormat getTimeFormat(String format) {
        String timePattern = getTimePattern(format);
        return (timePattern == null) ? null : new SimpleDateFormat(timePattern);
    }

    private String getVariableName(String format) throws ElasticConversionException {
        String timePattern = getTimePattern(format);
        String separator = getSeparator(timePattern);
        Pattern pattern = Pattern.compile("S{2}");
        Matcher matcher = pattern.matcher(timePattern);
        if (matcher.find()) {
            return String.format("HH%1$sMM%1$sSS%1$sss", separator);
        }

        pattern = Pattern.compile("s{2}");
        matcher = pattern.matcher(timePattern);

        if (matcher.find()) {
            return String.format("HH%1$sMM%1$sSS", separator);
        } else {
            return String.format("HH%1$sMM", separator);
        }
    }

    private String getSeparator(String datePattern) throws ElasticConversionException {
        Pattern pattern = Pattern.compile("[^HmsS]");
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
        if (isEqual && ((count == 1) || (count == 2) || (count == 3))) {
            return separator;
        } else {
            throw new ElasticConversionException("Time pattern should have consistent separator");
        }
    }

    private static String getTimePattern(String format) {
        Pattern pattern = Pattern.compile("H{2}.m{2}(.s{2})?(.S{2})?");
        Matcher matcher = pattern.matcher(format);
        if (matcher.find()) {
            return matcher.group();
        }
        return null;
    }

    @Override
    protected List<String> getParameters() {
        return new ArrayList<>();
    }

}
