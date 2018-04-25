/**
 * ##License
 * Ryft-Customized BSD License
 * Copyright (c) 2018, Ryft Systems, Inc.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
 *   This product includes software developed by Ryft Systems, Inc.
 * 4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
 *   to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.ryft.elasticsearch.converter.ryftdsl;

import com.ryft.elasticsearch.converter.ElasticConversionException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Optional;
import java.util.TimeZone;
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
        DateFormat df = null;
        if (datePattern != null) {
            df = new SimpleDateFormat(datePattern);
            df.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
        }
        return df;
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
