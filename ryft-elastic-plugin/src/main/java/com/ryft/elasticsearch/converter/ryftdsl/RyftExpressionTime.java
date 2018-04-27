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
        DateFormat df = null;
        if (timePattern != null) {
            df = new SimpleDateFormat(timePattern);
            df.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
        }
        return df;
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
}
