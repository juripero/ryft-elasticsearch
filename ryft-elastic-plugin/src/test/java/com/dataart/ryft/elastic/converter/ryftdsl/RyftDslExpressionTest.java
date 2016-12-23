package com.dataart.ryft.elastic.converter.ryftdsl;

import com.dataart.ryft.elastic.converter.ElasticConversionException;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import static org.junit.Assert.*;
import org.junit.Test;

public class RyftDslExpressionTest {

    private final static ESLogger LOGGER = Loggers.getLogger(RyftDslExpressionTest.class);

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
