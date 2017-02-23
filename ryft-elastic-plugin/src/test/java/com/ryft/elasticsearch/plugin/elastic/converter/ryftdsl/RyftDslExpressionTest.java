package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConversionException;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftExpressionRange.RyftOperatorCompare;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
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
        RyftExpression ryftExpression = new RyftExpressionDate(dateNow, RyftOperatorCompare.EQ);
        String expected = String.format("DATE(YYYY-MM-DD = %s)", dateFormat.format(dateNow));
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());

        Date dateBeforeNow = new Date(dateNow.getTime() - 3600000L * 48);
        dateFormat = new SimpleDateFormat("MM_dd_yyyy");
        ryftExpression = new RyftExpressionDate(dateBeforeNow, RyftOperatorCompare.LT, RyftOperatorCompare.LTE, dateNow, "MM_dd_yyyy HH:mm:ss.SS");
        expected = String.format("DATE(%s < MM_DD_YYYY <= %s)", dateFormat.format(dateBeforeNow), dateFormat.format(dateNow));
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());
    }

    @Test
    public void TestExpressionTime() throws ElasticConversionException {
        Random random = new Random();
        Date dateNow = new Date();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        RyftExpression ryftExpression = new RyftExpressionDate(dateNow, RyftOperatorCompare.EQ);
        String expected = String.format("DATE(YYYY-MM-DD = %s)", dateFormat.format(dateNow));
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());

        Date dateBeforeNow = new Date(dateNow.getTime() - random.nextInt(1000000));
        dateFormat = new SimpleDateFormat("HH.mm.ss.SS");
        ryftExpression = new RyftExpressionTime(dateBeforeNow, RyftOperatorCompare.LT, RyftOperatorCompare.LTE, dateNow, "HH.mm.ss.SS");
        expected = String.format("TIME(%s < HH.MM.SS.ss <= %s)", dateFormat.format(dateBeforeNow).substring(0, 11), dateFormat.format(dateNow).substring(0, 11));
        LOGGER.info(expected);
        assertEquals(expected, ryftExpression.buildRyftString());
    }

    @Test
    public void TestExpressionRegex() throws ElasticConversionException {
        String value = "[^HmsS]";
        RyftExpression ryftExpression = new RyftExpressionRegex(value);
        String expected = String.format("REGEX(%s)", value);
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
