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
import com.ryft.elasticsearch.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import com.ryft.elasticsearch.converter.ryftdsl.RyftExpressionRange.RyftOperatorCompare;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import static org.junit.Assert.*;
import org.junit.Test;

public class RyftDslExpressionTest {

    private final static ESLogger LOGGER = Loggers.getLogger(RyftDslExpressionTest.class);

    @Test
    public void TestExpressionNumeric() {
        String valueA = "1.01e2";
        RyftExpression ryftExpression = new RyftExpressionNumeric(valueA, RyftOperatorCompare.EQ);
        String expected = String.format("NUMBER(NUM = \"%s\", \",\", \".\")", valueA);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());

        String valueB = "32432.332";
        ryftExpression = new RyftExpressionNumeric(valueA, RyftOperatorCompare.LT, RyftOperatorCompare.LTE, valueB);
        expected = String.format("NUMBER(\"%s\" < NUM <= \"%s\", \",\", \".\")", valueA, valueB);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());

        valueA = "-0.00001234";
        valueB = "123.456e-6";
        ryftExpression = new RyftExpressionNumeric(valueA, RyftOperatorCompare.LTE, RyftOperatorCompare.LT, valueB);
        expected = String.format("NUMBER(\"%s\" <= NUM < \"%s\", \",\", \".\")", valueA, valueB);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());

        valueA = "10011928.0";
        ryftExpression = new RyftExpressionNumeric("10011928.0", RyftOperatorCompare.GTE, "-", ".");
        expected = String.format("NUMBER(NUM >= \"%s\", \"-\", \".\")", valueA);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());
    }

    @Test
    public void TestExpressionCurrency() {
        String valueA = "1.01e2";
        RyftExpression ryftExpression = new RyftExpressionCurrency(valueA, RyftOperatorCompare.EQ);
        String expected = String.format("CURRENCY(CUR = \"$%s\", \"$\", \",\", \".\")", valueA);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());

        String valueB = "32432.332";
        ryftExpression = new RyftExpressionCurrency(valueA, RyftOperatorCompare.LT, RyftOperatorCompare.LTE, valueB);
        expected = String.format("CURRENCY(\"$%s\" < CUR <= \"$%s\", \"$\", \",\", \".\")", valueA, valueB);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());

        valueA = "-0.00001234";
        valueB = "123.456e-6";
        ryftExpression = new RyftExpressionCurrency(valueA, RyftOperatorCompare.LTE, RyftOperatorCompare.LT, valueB, "%", ",", ".");
        expected = String.format("CURRENCY(\"%%%s\" <= CUR < \"%%%s\", \"%%\", \",\", \".\")", valueA, valueB);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());

        valueA = "10011928.0";
        ryftExpression = new RyftExpressionCurrency(valueA, RyftOperatorCompare.GTE, "$", "-", ".");
        expected = String.format("CURRENCY(CUR >= \"$%s\", \"$\", \"-\", \".\")", valueA);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());
    }

    @Test
    public void TestExpressionDate() throws ElasticConversionException {
        Date dateNow = new Date();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
        RyftExpression ryftExpression = new RyftExpressionDate(dateNow, RyftOperatorCompare.EQ);
        String expected = String.format("DATE(YYYY-MM-DD = %s)", dateFormat.format(dateNow));
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());

        Date dateBeforeNow = new Date(dateNow.getTime() - 3600000L * 48);
        dateFormat = new SimpleDateFormat("MM_dd_yyyy");
        dateFormat.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
        ryftExpression = new RyftExpressionDate(dateBeforeNow, RyftOperatorCompare.LT, RyftOperatorCompare.LTE, dateNow, "MM_dd_yyyy HH:mm:ss.SS");
        expected = String.format("DATE(%s < MM_DD_YYYY <= %s)", dateFormat.format(dateBeforeNow), dateFormat.format(dateNow));
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());
    }

    @Test
    public void TestExpressionTime() throws ElasticConversionException {
        Random random = new Random();
        Date dateNow = new Date();
        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
        RyftExpression ryftExpression = new RyftExpressionTime(dateNow, RyftOperatorCompare.EQ);
        String expected = String.format("TIME(HH:MM:SS = %s)", dateFormat.format(dateNow));
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());

        Date dateBeforeNow = new Date(dateNow.getTime() - random.nextInt(1000000));
        dateFormat = new SimpleDateFormat("HH.mm.ss.SS");
        dateFormat.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
        ryftExpression = new RyftExpressionTime(dateBeforeNow, RyftOperatorCompare.LT, RyftOperatorCompare.LTE, dateNow, "HH.mm.ss.SS");
        expected = String.format("TIME(%s < HH.MM.SS.ss <= %s)", dateFormat.format(dateBeforeNow).substring(0, 11), dateFormat.format(dateNow).substring(0, 11));
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());
    }

    @Test
    public void TestExpressionRegex() throws ElasticConversionException {
        String value = "[^HmsS]";
        RyftExpression ryftExpression = new RyftExpressionRegex(value);
        String expected = String.format("PCRE2(%s)", value);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());
    }

    @Test
    public void TestExpressionIPv4() throws ElasticConversionException {
        String valueA = "127.0.0.1";
        RyftExpression ryftExpression = new RyftExpressionIPv4(valueA, RyftOperatorCompare.EQ);
        String expected = String.format("IPV4(IP = \"%s\")", valueA);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());

        String valueB = "127.0.2.165";
        ryftExpression = new RyftExpressionIPv4(valueA, RyftOperatorCompare.LT, RyftOperatorCompare.LTE, valueB);
        expected = String.format("IPV4(\"%s\" < IP <= \"%s\")", valueA, valueB);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());
    }

    @Test
    public void TestExpressionIPv6() throws ElasticConversionException {
        String valueA = "FF01:1234::1";
        RyftExpression ryftExpression = new RyftExpressionIPv6(valueA, RyftOperatorCompare.EQ);
        String expected = String.format("IPV6(IP = \"%s\")", valueA);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());

        String valueB = "FF01:1234::FF::1";
        ryftExpression = new RyftExpressionIPv6(valueA, RyftOperatorCompare.LT, RyftOperatorCompare.LTE, valueB);
        expected = String.format("IPV6(\"%s\" < IP <= \"%s\")", valueA, valueB);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());
    }

    @Test
    public void TestExpressionExact() throws ElasticConversionException {
        String valueA = "test\"??\"\"";
        RyftExpression ryftExpression = new RyftExpressionExactSearch(valueA);
        String expected = String.format("\"%s\"", valueA);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());

        ryftExpression = new RyftExpressionExactSearch(valueA, 10);
        expected = String.format("ES(\"%s\", WIDTH=10)", valueA);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());

        ryftExpression = new RyftExpressionExactSearch(valueA, true);
        expected = String.format("ES(\"%s\", LINE=true)", valueA);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());
    }

    @Test
    public void TestExpressionFuzzy() throws ElasticConversionException {
        String valueA = "Lorem ipsum";
        RyftExpression ryftExpression = new RyftExpressionFuzzySearch(valueA, RyftFuzzyMetric.FEDS, 2);
        String expected = String.format("FEDS(\"%s\", DIST=2)", valueA);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());

        ryftExpression = new RyftExpressionFuzzySearch(valueA, RyftFuzzyMetric.FEDS, 2, 10);
        expected = String.format("FEDS(\"%s\", WIDTH=10, DIST=2)", valueA);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());

        ryftExpression = new RyftExpressionFuzzySearch(valueA, RyftFuzzyMetric.FHS, 2, true);
        expected = String.format("FHS(\"%s\", LINE=true, DIST=2)", valueA);
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());
    }

}
