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
package com.ryft.elasticsearch.converter;

import com.google.common.collect.Lists;
import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import com.ryft.elasticsearch.converter.ryftdsl.RyftFormat;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.utils.JSR250Module;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Inject;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

@RunWith(BlockJUnit4ClassRunner.class)
public class ElasticConverterTest {

    @Inject
    public ElasticConverter elasticConverter;

    @Before
    public void setUp() {
        Guice.createInjector(
                new AbstractModule() {
            @Override
            protected void configure() {
                install(new JSR250Module());
                install(new ElasticConversionModule());
            }
        }).injectMembers(this);
    }

    @Test
    public void MatchWithDefaultRequestTest() throws Exception {
        String query = "{\"query\": {\"match\": "
                + "{\"text_entry\": {\"query\": \"good mother\"}}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS \"good\") OR (RECORD.text_entry CONTAINS \"mother\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void MatchPhraseWithFuzzyRequestTest() throws Exception {
        String query = "{\"query\": {\"match_phrase\": {\"text_entry\": "
                + "{\"query\": \"good mother\", \"fuzziness\" :2, \"metric\": \"FHS\"}}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS FHS(\"good mother\", DIST=2))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void MatchWithFuzzyRequestTest() throws Exception {
        String query = "{\"query\": {\"match\": {\"text_entry\": "
                + "{\"query\": \"good mother\", \"metric\": \"feds\", \"fuzziness\": 2, \"operator\": \"and\"}}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS FEDS(\"good\", DIST=2)) AND (RECORD.text_entry CONTAINS FEDS(\"mother\", DIST=2)))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void FuzzyRequestTest() throws Exception {
        String query = "{\"query\": {\"fuzzy\": {\"text_entry\": "
                + "{\"value\": \"good mother\", \"metric\": \"fhs\", \"fuzziness\": 2}}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS FHS(\"goodmother\", DIST=2))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void MatchWithMatchPhraseTypeRequestTest() throws Exception {
        String query = "{\"query\": {\"match\": {\"text_entry\": "
                + "{\"query\": \"good mother\", \"fuzziness\" :2, \"metric\": \"FHS\", \"type\": \"phrase\"}}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS FHS(\"good mother\", DIST=2))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void FuzzyWithMatchPhraseTypeRequestTest() throws Exception {
        String query = "{\"query\": {\"fuzzy\": {\"text_entry\": "
                + "{\"value\": \"good mother\", \"metric\": \"fhs\", \"fuzziness\": 2, \"type\": \"phrase\"}}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS FHS(\"goodmother\", DIST=2))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void BoolMustRequestTest() throws Exception {
        String query = "{\"query\": {\"bool\": {\"must\": ["
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"Would nat be\", \"fuzziness\": 1, \"metric\": \"Fhs\"}}}, "
                + "{\"fuzzy\": {\"text_entry\" : {\"value\": \"knight\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}]}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS FHS(\"Would nat be\", DIST=1)) AND (RECORD.text_entry CONTAINS \"knight\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void BoolMustNotRequestTest() throws Exception {
        String query = "{\"query\": {\"bool\": {\"must_not\": ["
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"Would nat be\", \"fuzziness\": 1, \"metric\": \"Fhs\"}}}, "
                + "{\"fuzzy\": {\"text_entry\" : {\"value\": \"knight\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}]}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry NOT_CONTAINS FHS(\"Would nat be\", DIST=1)) OR (RECORD.text_entry NOT_CONTAINS \"knight\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void BoolMustAndMustNotRequestTest() throws Exception {
        String query = "{\"query\": {\"bool\": {\"must\": ["
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"Would nat be\", \"fuzziness\": 1, \"metric\": \"Fhs\"}}}, "
                + "{\"fuzzy\": {\"text_entry\" : {\"value\": \"knight\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}],"
                + "\"must_not\": ["
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"Would nat be\", \"fuzziness\": 1, \"metric\": \"Fhs\"}}}, "
                + "{\"fuzzy\": {\"text_entry\" : {\"value\": \"knight\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}]}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        String ryftString = ryftRequest.getQuery().buildRyftString();
        assertTrue(
                ryftString.equals(
                        "(((RECORD.text_entry NOT_CONTAINS FHS(\"Would nat be\", DIST=1)) OR (RECORD.text_entry NOT_CONTAINS \"knight\")) AND "
                        + "((RECORD.text_entry CONTAINS FHS(\"Would nat be\", DIST=1)) AND (RECORD.text_entry CONTAINS \"knight\")))")
                || ryftString.equals("(((RECORD.text_entry CONTAINS FHS(\"Would nat be\", DIST=1)) AND (RECORD.text_entry CONTAINS \"knight\")) AND "
                        + "((RECORD.text_entry NOT_CONTAINS FHS(\"Would nat be\", DIST=1)) OR (RECORD.text_entry NOT_CONTAINS \"knight\")))")
        );
    }

    @Test
    public void BoolShouldRequestTest() throws Exception {
        String query = "{\"query\": {\"bool\": {\"should\": ["
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"Would nat be\", \"fuzziness\": 1, \"metric\": \"Fhs\"}}}, "
                + "{\"fuzzy\": {\"text_entry\" : {\"value\": \"knight\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}]}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS FHS(\"Would nat be\", DIST=1)) OR (RECORD.text_entry CONTAINS \"knight\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void BoolShouldWithMinimumShouldMatch2RequestTest() throws Exception {
        String query = "{\"query\": {\"bool\": {\"should\": ["
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"juliet\", \"fuzziness\": 0, \"metric\": \"FEDS\"}}}, "
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"romeo\", \"fuzziness\": 0, \"metric\": \"FEDS\"}}}, "
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"knight\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}], "
                + "\"minimum_should_match\": 2}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(((RECORD.text_entry CONTAINS \"juliet\") AND (RECORD.text_entry CONTAINS \"romeo\")) OR "
                + "((RECORD.text_entry CONTAINS \"juliet\") AND (RECORD.text_entry CONTAINS \"knight\")) OR "
                + "((RECORD.text_entry CONTAINS \"romeo\") AND (RECORD.text_entry CONTAINS \"knight\")))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void BoolShouldWithMinimumShouldMatch3RequestTest() throws Exception {
        String query = "{\"query\": {\"bool\": {"
                + "\"should\": ["
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"juliet\", \"fuzziness\": 0, \"metric\": \"FEDS\"}}}, "
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"romeo\", \"fuzziness\": 0, \"metric\": \"FEDS\"}}}, "
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"knight\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}, "
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"hamlet\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}], "
                + "\"minimum_should_match\": 3}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(((RECORD.text_entry CONTAINS \"juliet\") AND (RECORD.text_entry CONTAINS \"romeo\") AND (RECORD.text_entry CONTAINS \"knight\")) OR "
                + "((RECORD.text_entry CONTAINS \"juliet\") AND (RECORD.text_entry CONTAINS \"romeo\") AND (RECORD.text_entry CONTAINS \"hamlet\")) OR "
                + "((RECORD.text_entry CONTAINS \"juliet\") AND (RECORD.text_entry CONTAINS \"knight\") AND (RECORD.text_entry CONTAINS \"hamlet\")) OR "
                + "((RECORD.text_entry CONTAINS \"romeo\") AND (RECORD.text_entry CONTAINS \"knight\") AND (RECORD.text_entry CONTAINS \"hamlet\")))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void BoolShouldWithMinimumShouldMatch1AndMustRequestTest() throws Exception {
        String query = "{\"bool\": {"
                + "\"must\": {\"match\": {\"text_entry\": {\"query\": \"Domsday\", \"fuzziness\": 1}}}, "
                + "\"should\": [{\"match\": {\"play_name\": \"Julius\"}}, {\"match\": {\"play_name\": \"Henry\"}}], "
                + "\"minimum_number_should_match\": 1}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(((RECORD.play_name CONTAINS \"Julius\") OR (RECORD.play_name CONTAINS \"Henry\")) AND (RECORD.text_entry CONTAINS FEDS(\"Domsday\", DIST=1)))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void BoolShouldWithMinimumShouldMatch2AndMustAndMustNotRequestTest() throws Exception {
        String query = "{\"query\": {\"bool\": {"
                + "\"should\": ["
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"juliet\", \"fuzziness\": 0, \"metric\": \"FEDS\"}}}, "
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"romeo\", \"fuzziness\": 0, \"metric\": \"FEDS\"}}}, "
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"knight\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}], "
                + "\"must\": ["
                + "{\"fuzzy\": {\"text_entry\" : {\"value\": \"hamlet\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}], "
                + "\"must_not\": ["
                + "{\"fuzzy\": {\"text_entry\" : {\"value\": \"love\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}], "
                + "\"minimum_should_match\": 2}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("((((RECORD.text_entry CONTAINS \"juliet\") AND "
                + "(RECORD.text_entry CONTAINS \"romeo\")) OR ((RECORD.text_entry CONTAINS \"juliet\") AND "
                + "(RECORD.text_entry CONTAINS \"knight\")) OR ((RECORD.text_entry CONTAINS \"romeo\") AND "
                + "(RECORD.text_entry CONTAINS \"knight\"))) AND (RECORD.text_entry NOT_CONTAINS \"love\") AND "
                + "(RECORD.text_entry CONTAINS \"hamlet\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void BoolShouldWithoutMinimumShouldMatchAndMustAndMustNotRequestTest() throws Exception {
        String query = "{\"query\": {\"bool\": {"
                + "\"should\": ["
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"juliet\", \"fuzziness\": 0, \"metric\": \"FEDS\"}}}, "
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"romeo\", \"fuzziness\": 0, \"metric\": \"FEDS\"}}}, "
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"knight\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}], "
                + "\"must\": ["
                + "{\"fuzzy\": {\"text_entry\" : {\"value\": \"hamlet\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}], "
                + "\"must_not\": ["
                + "{\"fuzzy\": {\"text_entry\" : {\"value\": \"love\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}]}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry NOT_CONTAINS \"love\") AND (RECORD.text_entry CONTAINS \"hamlet\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void BoolNotArrayPrimitiveRequestTest() throws Exception {
        String query = "{\"query\": {\"bool\": {\"must\": {\"match_phrase\": {\"text_entry\": \"Would not be\"}},"
                + "\"must_not\": {\"match\": {\"text_entry\": \"queen\"}},"
                + "\"should\": {\"match\": {\"text_entry\": \"friend\"}}}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry NOT_CONTAINS \"queen\") AND (RECORD.text_entry CONTAINS \"Would not be\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void ignoreUnknownPrimitives() throws Exception {
        String query = "{\"query\": {\"match_phrase\": {\"text_entry\": "
                + "{\"query\": \"good mother\", \"fuzziness\" :0}}}, \"unknown_property\": 500}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS \"good mother\")",
                ryftRequest.getQuery().buildRyftString());
        query = "{\"from\": 500, \"query\": {\"match_phrase\": {\"text_entry\": "
                + "{\"query\": \"good mother\", \"fuzziness\" :0}}}}";
        request = new SearchRequest(new String[]{""}, query.getBytes());
        ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS \"good mother\")",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void ExactSearchMatchPhraseRequestTest() throws Exception {
        String query = "{\"query\": {\"match_phrase\": {\"text_entry\": {\"query\": \"good mother\"}}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS \"good mother\")",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void ExactSearchMatchRequestTest() throws Exception {
        String query = "{\"query\": {\"match\": {\"text_entry\": {\"query\": \"good mother\"}}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS \"good\") OR (RECORD.text_entry CONTAINS \"mother\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DefaultFuzzySearchRequestTest() throws Exception {
        String query = "{\"query\": {\"fuzzy\": {\"text_entry\": {\"value\": \"mother\"}}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS FEDS(\"mother\", DIST=2))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void SimpilfiedExactSearchMatchPhraseAllRequestTest() throws Exception {
        String query = "{\"query\": {\"match_phrase\": {\"_all\": \"good mother\"}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD CONTAINS \"good mother\")",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void SimpilfiedExactSearchMatchPhraseRequestTest() throws Exception {
        String query = "{\"query\": {\"match_phrase\": {\"text_entry\": \"good mother\"}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS \"good mother\")",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void SimpilfiedExactSearchMatchRequestTest() throws Exception {
        String query = "{\"query\": {\"match\": {\"text_entry\": \"good mother\"}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS \"good\") OR (RECORD.text_entry CONTAINS \"mother\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void SimpilfiedFuzzySearchRequestTest() throws Exception {
        String query = "{\"query\": {\"fuzzy\": {\"text_entry\": \"good mother\"}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS FEDS(\"goodmother\", DIST=2))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void CustomFilesRequestTest() throws Exception {
        String query = "{\"query\": {\"match_phrase\": {\"text_entry\": \"test\"}}, "
                + "\"ryft\": {\"files\": [\"1.txt\", \"2.json\"], \"format\": \"Xml\"}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS \"test\")",
                ryftRequest.getQuery().buildRyftString());
//        assertTrue(ryftRequest.getRyftSearchUrl().contains("file=1.txt&file=2.json"));
//        assertTrue(ryftRequest.getRyftSearchUrl().contains("format=xml"));
    }

    @Test
    public void WildcardSearchRequestTest() throws Exception {
        String query = "{\"query\": {\"wildcard\": {\"text_entry\": \"m?ther\"}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS \"m\"?\"ther\")",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void WildcardSearchRequestEdgeTest() throws Exception {
        String query = "{\"query\": {\"wildcard\": {\"text_entry\": \"?other?\"}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS \"\"?\"other\"?\"\")",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void WildcardInMatchRequestTest() throws Exception {
        String query = "{\"query\": {\"match\": {\"text_entry\": \"\\\"?\\\"o\\\"?\\\"d m\\\"?\\\"the\\\"?\\\"\"}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS \"\"?\"o\"?\"d\") OR (RECORD.text_entry CONTAINS \"m\"?\"the\"?\"\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void WildcardInMatchRequestUnescapedTest() throws Exception {
        String query = "{\"query\": {\"match\": {\"text_entry\": \"go?d m?ther\"}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS \"go?d\") OR (RECORD.text_entry CONTAINS \"m?ther\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void WildcardInMatchPhraseRequestTest() throws Exception {
        String query = "{\"query\": {\"match_phrase\": {\"text_entry\": {\"query\":\"go\\\"?\\\"d m\\\"?\\\"ther\", \"fuzziness\": 1}}}}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS FEDS(\"go\"?\"d m\"?\"ther\", DIST=1))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DateTimeSearchTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"timestamp\": {\n"
                + "        \"value\": \"2014/01/01 07:00:00\",\n"
                + "        \"type\": \"datetime\",\n"
                + "        \"format\": \"yyyy/MM/dd HH:mm:ss\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.timestamp CONTAINS DATE(YYYY/MM/DD = 2014/01/01)) AND (RECORD.timestamp CONTAINS TIME(HH:MM:SS = 07:00:00)))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DateTimeRangeSearchTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"range\" : {\n"
                + "      \"timestamp\" : {\n"
                + "        \"gt\" : \"2014/01/01 07:00:00\",\n"
                + "        \"lt\" : \"2014/01/07 07:00:00\",\n"
                + "        \"type\": \"datetime\",\n"
                + "        \"format\": \"yyyy/MM/dd HH:mm:ss\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(((RECORD.timestamp CONTAINS DATE(YYYY/MM/DD = 2014/01/01)) AND (RECORD.timestamp CONTAINS TIME(HH:MM:SS > 07:00:00))) "
                + "OR (RECORD.timestamp CONTAINS DATE(2014/01/01 < YYYY/MM/DD < 2014/01/07)) "
                + "OR ((RECORD.timestamp CONTAINS DATE(YYYY/MM/DD = 2014/01/07)) AND (RECORD.timestamp CONTAINS TIME(HH:MM:SS < 07:00:00))))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DateTimeRangeSameDayTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"range\" : {\n"
                + "      \"timestamp\" : {\n"
                + "        \"gt\" : \"2014/01/01 07:00:00\",\n"
                + "        \"lt\" : \"2014/01/01 12:00:00\",\n"
                + "        \"type\": \"datetime\",\n"
                + "        \"format\": \"yyyy/MM/dd HH:mm:ss\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.timestamp CONTAINS DATE(YYYY/MM/DD = 2014/01/01)) AND (RECORD.timestamp CONTAINS TIME(07:00:00 < HH:MM:SS < 12:00:00)))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DateTimeRangeLowerBoundTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"range\" : {\n"
                + "      \"timestamp\" : {\n"
                + "        \"gte\" : \"2014-01-01 07:00\",\n"
                + "        \"type\": \"datetime\",\n"
                + "        \"format\": \"yyyy-MM-dd HH:mm\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(((RECORD.timestamp CONTAINS DATE(YYYY-MM-DD = 2014-01-01)) AND (RECORD.timestamp CONTAINS TIME(HH:MM >= 07:00))) "
                + "OR (RECORD.timestamp CONTAINS DATE(YYYY-MM-DD > 2014-01-01)))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DateTimeRangeUpperBoundTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"range\" : {\n"
                + "      \"timestamp\" : {\n"
                + "        \"lt\" : \"2014-01-01 07:00\",\n"
                + "        \"type\": \"datetime\",\n"
                + "        \"format\": \"yyyy-MM-dd HH:mm\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(((RECORD.timestamp CONTAINS DATE(YYYY-MM-DD = 2014-01-01)) AND (RECORD.timestamp CONTAINS TIME(HH:MM < 07:00))) "
                + "OR (RECORD.timestamp CONTAINS DATE(YYYY-MM-DD < 2014-01-01)))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DateTimeRangeOnlyDateTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"range\" : {\n"
                + "      \"timestamp\" : {\n"
                + "        \"gt\" : \"2014-01-01\",\n"
                + "        \"lte\" : \"2014-01-02\",\n"
                + "        \"type\": \"datetime\",\n"
                + "        \"format\": \"yyyy-MM-dd\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.timestamp CONTAINS DATE(2014-01-01 < YYYY-MM-DD <= 2014-01-02))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DateTimeRangeOnlyTimeTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"range\" : {\n"
                + "      \"timestamp\" : {\n"
                + "        \"gt\" : \"07:00\",\n"
                + "        \"lte\" : \"09:00\",\n"
                + "        \"type\": \"datetime\",\n"
                + "        \"format\": \"HH:mm\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.timestamp CONTAINS TIME(07:00 < HH:MM <= 09:00))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void NumericSearchTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"price\": {\n"
                + "        \"value\": 20,\n"
                + "        \"type\": \"number\",\n"
                + "        \"separator\":\",\",\n"
                + "        \"decimal\":\".\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.price CONTAINS NUMBER(NUM = \"20\", \",\", \".\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void NumericSearchSimplifiedTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"price\": 20\n"
                + "    }\n"
                + "  }\n"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.price CONTAINS NUMBER(NUM = \"20\", \",\", \".\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void NumericSearchArrayTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"price\": [20, 30]\n"
                + "    }\n"
                + "  }\n"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.price CONTAINS NUMBER(NUM = \"20\", \",\", \".\")) OR (RECORD.price CONTAINS NUMBER(NUM = \"30\", \",\", \".\")))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void NumericRangeTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"range\" : {\n"
                + "      \"age\" : {\n"
                + "        \"gte\" : -1.01e2,\n"
                + "        \"lte\" : \"2000.12\",\n"
                + "        \"type\":\"number\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.age CONTAINS NUMBER(\"-1.01e2\" <= NUM <= \"2000.12\", \",\", \".\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void CurrencySearchTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"price\": {\n"
                + "        \"value\": \"$100\",\n"
                + "        \"type\": \"currency\",\n"
                + "        \"currency\":\"$\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.price CONTAINS CURRENCY(CUR = \"$100\", \"$\", \",\", \".\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void CurrencySearchSimpleTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"price\": {\n"
                + "        \"value\": \"100\",\n"
                + "        \"type\": \"currency\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.price CONTAINS CURRENCY(CUR = \"$100\", \"$\", \",\", \".\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void CurrencyRangeTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"range\" : {\n"
                + "      \"price\" : {\n"
                + "        \"gte\" : 10,\n"
                + "        \"lte\" : 20,\n"
                + "        \"type\":\"currency\",\n"
                + "        \"separator\":\",\",\n"
                + "        \"decimal\":\".\",\n"
                + "        \"currency\":\"%\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.price CONTAINS CURRENCY(\"%10\" <= CUR <= \"%20\", \"%\", \",\", \".\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void Ipv4SearchTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"ip_addr\": {\n"
                + "        \"value\": \"192.168.10.11\",\n"
                + "        \"type\": \"ipv4\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.ip_addr CONTAINS IPV4(IP = \"192.168.10.11\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void Ipv4SearchMaskTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"ip_addr\": {\n"
                + "        \"value\": \"192.168.0.0/16\",\n"
                + "        \"type\": \"ipv4\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.ip_addr CONTAINS IPV4(\"192.168.0.0\" <= IP <= \"192.168.255.255\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void Ipv4RangeTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"range\": {\n"
                + "      \"ip_addr\": {\n"
                + "        \"gte\": \"192.168.1.0\",\n"
                + "        \"lt\":  \"192.168.2.0\",\n"
                + "        \"type\": \"ipv4\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.ip_addr CONTAINS IPV4(\"192.168.1.0\" <= IP < \"192.168.2.0\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void Ipv6SearchTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"ip_addr\": {\n"
                + "        \"value\": \"2001::db8\",\n"
                + "        \"type\": \"ipv6\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.ip_addr CONTAINS IPV6(IP = \"2001::db8\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void Ipv6SearchMaskTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"ip_addr\": {\n"
                + "        \"value\": \"2001::db8/32\",\n"
                + "        \"type\": \"ipv6\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.ip_addr CONTAINS IPV6(\"2001::\" <= IP <= \"2001:0:ffff:ffff:ffff:ffff:ffff:ffff\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void Ipv6RangeTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"range\": {\n"
                + "      \"ip_addr\": {\n"
                + "        \"gte\": \"2001::db8\",\n"
                + "        \"lt\":  \"2001::db9\",\n"
                + "        \"type\": \"ipv6\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.ip_addr CONTAINS IPV6(\"2001::db8\" <= IP < \"2001::db9\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DefaultDatatypeTermTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"name\": {\n"
                + "        \"value\": \"Tom\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.name CONTAINS \"Tom\")",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void FilteredQueryTest() throws Exception {
        String query = "{"
                + "\"query\": {\n"
                + "    \"filtered\": {\n"
                + "      \"query\": {\n"
                + "        \"query\": {\n"
                + "          \"term\": {\n"
                + "            \"name\": {\n"
                + "              \"value\": \"Jim\"\n"
                + "            }\n"
                + "          }\n"
                + "        },\n"
                + "        \"ryft_enabled\": true\n"
                + "      },\n"
                + "      \"filter\": {\n"
                + "        \"bool\": {\n"
                + "          \"must\": [\n"
                + "            {\n"
                + "              \"range\": {\n"
                + "                \"registered\": {\n"
                + "                  \"gte\": 1338646255122,\n"
                + "                  \"lte\": 1496412655122,\n"
                + "                  \"format\": \"epoch_millis\"\n"
                + "                }\n"
                + "              }\n"
                + "            }\n"
                + "          ],\n"
                + "          \"must_not\": []\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertEquals("((((RECORD.registered CONTAINS DATE(YYYY-MM-DD = 2012-06-02)) " +
                        "AND (RECORD.registered CONTAINS TIME(HH:MM:SS >= 14:10:55))) " +
                        "OR (RECORD.registered CONTAINS DATE(2012-06-02 < YYYY-MM-DD < 2017-06-02)) " +
                        "OR ((RECORD.registered CONTAINS DATE(YYYY-MM-DD = 2017-06-02)) " +
                        "AND (RECORD.registered CONTAINS TIME(HH:MM:SS <= 14:10:55)))) " +
                        "AND (RECORD.name CONTAINS \"Jim\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void RegexSearchTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"regexp\": {\n"
                + "      \"_all\": {\n"
                + "        \"value\": \"W[0-9].+\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft\": {\n"
                + "    \"enabled\": true,\n"
                + "    \"files\": [\"shakespear.txt\"],\n"
                + "    \"format\": \"utf8\"\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertTrue(ryftRequest.isFileSearch());
        assertEquals(RyftFormat.UTF8,
                ryftRequest.getRyftProperties().get(PropertiesProvider.RYFT_FORMAT));
        assertEquals(Lists.newArrayList("shakespear.txt"),
                ryftRequest.getRyftProperties().get(PropertiesProvider.RYFT_FILES_TO_SEARCH));
        assertEquals("(RAW_TEXT CONTAINS PCRE2(\"W[0-9].+\"))",
                ryftRequest.getQuery().buildRyftString());
    }    
}
