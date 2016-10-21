package com.dataart.ryft.elastic.converter.ryftdsl;

import static com.dataart.ryft.elastic.converter.ryftdsl.RyftOperator.*;
import static com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryComplex.RyftLogicalOperator.*;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class RyftDslTest {

    @Test
    public void TestQuerySimple() {
        RyftInputSpecifier inputSpecifier = new RyftInputSpecifierRawText();
        RyftExpression expression = new RyftExpressionExactSearch("test");
        RyftQuery ryftQuery;
        for (RyftOperator operator : RyftOperator.values()) {
            ryftQuery = new RyftQuerySimple(inputSpecifier, operator, expression);
            assertEquals(ryftQuery.buildRyftString(), "(RAW_TEXT " + operator.name() + " \"test\")");
        }
        inputSpecifier = new RyftInputSpecifierRecord();
        ryftQuery = new RyftQuerySimple(inputSpecifier, EQUALS, expression);
        assertEquals(ryftQuery.buildRyftString(), "(RECORD EQUALS \"test\")");
        inputSpecifier = new RyftInputSpecifierRecord("parameter");
        ryftQuery = new RyftQuerySimple(inputSpecifier, EQUALS, expression);
        assertEquals(ryftQuery.buildRyftString(), "(RECORD.parameter EQUALS \"test\")");
    }

    @Test
    public void TestQueryComplex() {
        RyftQuery query1 = new RyftQuerySimple(new RyftInputSpecifierRawText(),
                CONTAINS, new RyftExpressionExactSearch("test1"));
        RyftQuery query2 = new RyftQuerySimple(new RyftInputSpecifierRawText(),
                NOT_CONTAINS, new RyftExpressionExactSearch("test2"));
        RyftQuery complexQuery1 = new RyftQueryComplex(query1, AND, query2);
        assertEquals(complexQuery1.buildRyftString(), "((RAW_TEXT CONTAINS \"test1\") AND (RAW_TEXT NOT_CONTAINS \"test2\"))");
        RyftQuery complexQuery2 = new RyftQueryComplex(query1, OR, complexQuery1);
        assertEquals(complexQuery2.buildRyftString(), "((RAW_TEXT CONTAINS \"test1\") OR ((RAW_TEXT CONTAINS \"test1\") AND (RAW_TEXT NOT_CONTAINS \"test2\")))");
        RyftQuery complexQuery3 = new RyftQueryComplex(complexQuery1, XOR, query2);
        assertEquals(complexQuery3.buildRyftString(), "(((RAW_TEXT CONTAINS \"test1\") AND (RAW_TEXT NOT_CONTAINS \"test2\")) XOR (RAW_TEXT NOT_CONTAINS \"test2\"))");
    }
}
