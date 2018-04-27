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

import static com.ryft.elasticsearch.converter.ryftdsl.RyftOperator.*;
import static com.ryft.elasticsearch.converter.ryftdsl.RyftQueryComplex.RyftLogicalOperator.*;
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
            assertEquals("(RAW_TEXT " + operator.name() + " \"test\")",
                    ryftQuery.buildRyftString());
        }
        inputSpecifier = new RyftInputSpecifierRecord();
        ryftQuery = new RyftQuerySimple(inputSpecifier, EQUALS, expression);
        assertEquals("(RECORD EQUALS \"test\")",
                ryftQuery.buildRyftString());
        inputSpecifier = new RyftInputSpecifierRecord("parameter");
        ryftQuery = new RyftQuerySimple(inputSpecifier, EQUALS, expression);
        assertEquals("(RECORD.parameter EQUALS \"test\")",
                ryftQuery.buildRyftString());
    }

    @Test
    public void TestQueryComplex() {
        RyftQuery query1 = new RyftQuerySimple(new RyftInputSpecifierRawText(),
                CONTAINS, new RyftExpressionExactSearch("test1"));
        RyftQuery query2 = new RyftQuerySimple(new RyftInputSpecifierRawText(),
                NOT_CONTAINS, new RyftExpressionExactSearch("test2"));
        RyftQuery complexQuery1 = new RyftQueryComplex(query1, AND, query2);
        assertEquals("((RAW_TEXT CONTAINS \"test1\") AND (RAW_TEXT NOT_CONTAINS \"test2\"))",
                complexQuery1.buildRyftString());
        RyftQuery complexQuery2 = new RyftQueryComplex(query1, OR, complexQuery1);
        assertEquals("((RAW_TEXT CONTAINS \"test1\") OR ((RAW_TEXT CONTAINS \"test1\") AND (RAW_TEXT NOT_CONTAINS \"test2\")))",
                complexQuery2.buildRyftString());
        RyftQuery complexQuery3 = new RyftQueryComplex(complexQuery1, XOR, query2);
        assertEquals("(((RAW_TEXT CONTAINS \"test1\") AND (RAW_TEXT NOT_CONTAINS \"test2\")) XOR (RAW_TEXT NOT_CONTAINS \"test2\"))",
                complexQuery3.buildRyftString());
    }

    @Test
    public void TestQueryComplexToRawText() {
        RyftQuery query1 = new RyftQuerySimple(new RyftInputSpecifierRecord("test"),
                CONTAINS, new RyftExpressionExactSearch("test1"));
        RyftQuery query2 = new RyftQuerySimple(new RyftInputSpecifierRecord("test"),
                NOT_CONTAINS, new RyftExpressionExactSearch("test2"));
        RyftQuery complexQuery1 = new RyftQueryComplex(query1, AND, query2);
        assertEquals("((RAW_TEXT CONTAINS ES(\"test1\", LINE=true)) AND (RAW_TEXT NOT_CONTAINS ES(\"test2\", LINE=true)))",
                complexQuery1.toRawTextQuery().buildRyftString());
        RyftQuery complexQuery2 = new RyftQueryComplex(query1, OR, complexQuery1);
        assertEquals("((RAW_TEXT CONTAINS \"test1\") OR ((RAW_TEXT CONTAINS ES(\"test1\", LINE=true)) AND (RAW_TEXT NOT_CONTAINS ES(\"test2\", LINE=true))))",
                complexQuery2.toRawTextQuery().buildRyftString());
        RyftQuery complexQuery3 = new RyftQueryComplex(complexQuery1, XOR, query2);
        assertEquals("(((RAW_TEXT CONTAINS ES(\"test1\", LINE=true)) AND (RAW_TEXT NOT_CONTAINS ES(\"test2\", LINE=true))) XOR (RAW_TEXT NOT_CONTAINS \"test2\"))",
                complexQuery3.toRawTextQuery().buildRyftString());
    }
}
