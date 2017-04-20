package com.ryft.elasticsearch.plugin.elastic.converter;

import com.ryft.elasticsearch.plugin.disruptor.messages.RyftRequestEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.RyftRequestEventFactory;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftExpressionDate;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftExpressionTime;
import com.ryft.elasticsearch.plugin.elastic.plugin.JSR250Module;
import com.ryft.elasticsearch.plugin.elastic.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.elastic.plugin.RyftProperties;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

@RunWith(BlockJUnit4ClassRunner.class)
public class ElasticConverterTest {

    @Inject
    public ElasticConverter elasticConverter;

    @Inject
    public ContextFactory contextFactory;

    @Before
    public void setUp() {
        Guice.createInjector(
                new AbstractModule() {
            @Override
            protected void configure() {
                install(new JSR250Module());
                install(new ElasticConversionModule());
                bind(RyftProperties.class).toProvider(PropertiesProvider.class).in(Singleton.class);
                bind(RyftRequestEventFactory.class).toProvider(
                        FactoryProvider.newFactory(RyftRequestEventFactory.class, RyftRequestEvent.class));
            }
        }).injectMembers(this);
    }

    @Test
    public void MatchWithDefaultRequestTest() throws Exception {
        String query = "{\"query\": {\"match\": "
                + "{\"text_entry\": {\"query\": \"good mother\"}}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS \"good\") OR (RECORD.text_entry CONTAINS \"mother\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void MatchPhraseWithFuzzyRequestTest() throws Exception {
        String query = "{\"query\": {\"match_phrase\": {\"text_entry\": "
                + "{\"query\": \"good mother\", \"fuzziness\" :2, \"metric\": \"FHS\"}}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS FHS(\"good mother\", DIST=2))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void MatchWithFuzzyRequestTest() throws Exception {
        String query = "{\"query\": {\"match\": {\"text_entry\": "
                + "{\"query\": \"good mother\", \"metric\": \"feds\", \"fuzziness\": 2, \"operator\": \"and\"}}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS FEDS(\"good\", DIST=2)) AND (RECORD.text_entry CONTAINS FEDS(\"mother\", DIST=2)))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void FuzzyRequestTest() throws Exception {
        String query = "{\"query\": {\"fuzzy\": {\"text_entry\": "
                + "{\"value\": \"good mother\", \"metric\": \"fhs\", \"fuzziness\": 2}}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS FHS(\"goodmother\", DIST=2))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void MatchWithMatchPhraseTypeRequestTest() throws Exception {
        String query = "{\"query\": {\"match\": {\"text_entry\": "
                + "{\"query\": \"good mother\", \"fuzziness\" :2, \"metric\": \"FHS\", \"type\": \"phrase\"}}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS FHS(\"good mother\", DIST=2))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void FuzzyWithMatchPhraseTypeRequestTest() throws Exception {
        String query = "{\"query\": {\"fuzzy\": {\"text_entry\": "
                + "{\"value\": \"good mother\", \"metric\": \"fhs\", \"fuzziness\": 2, \"type\": \"phrase\"}}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS FHS(\"goodmother\", DIST=2))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void BoolMustRequestTest() throws Exception {
        String query = "{\"query\": {\"bool\": {\"must\": ["
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"Would nat be\", \"fuzziness\": 1, \"metric\": \"Fhs\"}}}, "
                + "{\"fuzzy\": {\"text_entry\" : {\"value\": \"knight\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}]}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS FHS(\"Would nat be\", DIST=1)) AND (RECORD.text_entry CONTAINS \"knight\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void BoolMustNotRequestTest() throws Exception {
        String query = "{\"query\": {\"bool\": {\"must_not\": ["
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"Would nat be\", \"fuzziness\": 1, \"metric\": \"Fhs\"}}}, "
                + "{\"fuzzy\": {\"text_entry\" : {\"value\": \"knight\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}]}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
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
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
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
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
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
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
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
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
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
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
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
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
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
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry NOT_CONTAINS \"love\") AND (RECORD.text_entry CONTAINS \"hamlet\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void BoolNotArrayPrimitiveRequestTest() throws Exception {
        String query = "{\"query\": {\"bool\": {\"must\": {\"match_phrase\": {\"text_entry\": \"Would not be\"}},"
                + "\"must_not\": {\"match\": {\"text_entry\": \"queen\"}},"
                + "\"should\": {\"match\": {\"text_entry\": \"friend\"}}}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry NOT_CONTAINS \"queen\") AND (RECORD.text_entry CONTAINS \"Would not be\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void ignoreUnknownPrimitives() throws Exception {
        String query = "{\"query\": {\"match_phrase\": {\"text_entry\": "
                + "{\"query\": \"good mother\", \"fuzziness\" :0}}}, \"unknown_property\": 500}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS \"good mother\")",
                ryftRequest.getQuery().buildRyftString());
        query = "{\"from\": 500, \"query\": {\"match_phrase\": {\"text_entry\": "
                + "{\"query\": \"good mother\", \"fuzziness\" :0}}}}";
        bytes = new BytesArray(query);
        parser = XContentFactory.xContent(bytes).createParser(bytes);
        context = contextFactory.create(parser, query);
        ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS \"good mother\")",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void ExactSearchMatchPhraseRequestTest() throws Exception {
        String query = "{\"query\": {\"match_phrase\": {\"text_entry\": {\"query\": \"good mother\"}}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS \"good mother\")",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void ExactSearchMatchRequestTest() throws Exception {
        String query = "{\"query\": {\"match\": {\"text_entry\": {\"query\": \"good mother\"}}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS \"good\") OR (RECORD.text_entry CONTAINS \"mother\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DefaultFuzzySearchRequestTest() throws Exception {
        String query = "{\"query\": {\"fuzzy\": {\"text_entry\": {\"value\": \"mother\"}}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS FEDS(\"mother\", DIST=2))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void SimpilfiedExactSearchMatchPhraseAllRequestTest() throws Exception {
        String query = "{\"query\": {\"match_phrase\": {\"_all\": \"good mother\"}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD CONTAINS \"good mother\")",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void SimpilfiedExactSearchMatchPhraseRequestTest() throws Exception {
        String query = "{\"query\": {\"match_phrase\": {\"text_entry\": \"good mother\"}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS \"good mother\")",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void SimpilfiedExactSearchMatchRequestTest() throws Exception {
        String query = "{\"query\": {\"match\": {\"text_entry\": \"good mother\"}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS \"good\") OR (RECORD.text_entry CONTAINS \"mother\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void SimpilfiedFuzzySearchRequestTest() throws Exception {
        String query = "{\"query\": {\"fuzzy\": {\"text_entry\": \"good mother\"}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS FEDS(\"goodmother\", DIST=2))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void CustomFilesRequestTest() throws Exception {
        String query = "{\"query\": {\"match_phrase\": {\"text_entry\": \"test\"}}, "
                + "\"ryft\": {\"files\": [\"1.txt\", \"2.json\"], \"format\": \"Xml\"}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS \"test\")",
                ryftRequest.getQuery().buildRyftString());
        assertTrue(ryftRequest.getRyftSearchUrl().contains("file=1.txt&file=2.json"));
        assertTrue(ryftRequest.getRyftSearchUrl().contains("format=xml"));
    }

    @Test
    public void WildcardSearchRequestTest() throws Exception {
        String query = "{\"query\": {\"wildcard\": {\"text_entry\": \"m?ther\"}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS \"m\"?\"ther\")",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void WildcardSearchRequestEdgeTest() throws Exception {
        String query = "{\"query\": {\"wildcard\": {\"text_entry\": \"?other?\"}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS \"\"?\"other\"?\"\")",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void WildcardInMatchRequestTest() throws Exception {
        String query = "{\"query\": {\"match\": {\"text_entry\": \"\\\"?\\\"o\\\"?\\\"d m\\\"?\\\"the\\\"?\\\"\"}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS \"\"?\"o\"?\"d\") OR (RECORD.text_entry CONTAINS \"m\"?\"the\"?\"\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void WildcardInMatchRequestUnescapedTest() throws Exception {
        String query = "{\"query\": {\"match\": {\"text_entry\": \"go?d m?ther\"}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS \"go?d\") OR (RECORD.text_entry CONTAINS \"m?ther\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void WildcardInMatchPhraseRequestTest() throws Exception {
        String query = "{\"query\": {\"match_phrase\": {\"text_entry\": {\"query\":\"go\\\"?\\\"d m\\\"?\\\"ther\", \"fuzziness\": 1}}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS FEDS(\"go\"?\"d m\"?\"ther\", DIST=1))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void RawTextMatchSearchTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"match\": {\n" +
                "      \"_all\": {\n" +
                "        \"query\": \"good mother\",\n" +
                "        \"fuzziness\": 1,\n" +
                "        \"width\": 30\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"ryft\": {\n" +
                "    \"enabled\": true,\n" +
                "    \"files\": [\"shakespear.txt\"],\n" +
                "    \"format\": \"utf8\"\n" +
                "  }\n" +
                "}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("((RAW_TEXT CONTAINS FEDS(\"good\", WIDTH=30, DIST=1)) OR (RAW_TEXT CONTAINS FEDS(\"mother\", WIDTH=30, DIST=1)))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void RawTextMatchWithAndOperatorTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"match\": {\n" +
                "      \"_all\": {\n" +
                "        \"query\": \"good mother\",\n" +
                "        \"fuzziness\": 1,\n" +
                "        \"operator\": \"AND\",\n" +
                "        \"width\": 30\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"ryft\": {\n" +
                "    \"enabled\": true,\n" +
                "    \"files\": [\"shakespear.txt\"],\n" +
                "    \"format\": \"utf8\"\n" +
                "  }\n" +
                "}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("((RAW_TEXT CONTAINS FEDS(\"good\", LINE=true, DIST=1)) AND (RAW_TEXT CONTAINS FEDS(\"mother\", LINE=true, DIST=1)))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void RawTextMatchPhraseSearchTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"match_phrase\": {\n" +
                "      \"_all\": {\n" +
                "        \"query\": \"good mother\",\n" +
                "        \"fuzziness\": 1,\n" +
                "        \"width\": \"line\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"ryft\": {\n" +
                "    \"enabled\": true,\n" +
                "    \"files\": [\"shakespear.txt\"],\n" +
                "    \"format\": \"utf8\"\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RAW_TEXT CONTAINS FEDS(\"good mother\", LINE=true, DIST=1))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void RawTextTermSearchTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"wildcard\": {\n" +
                "      \"_all\": {\n" +
                "        \"value\": \"m?ther\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"ryft\": {\n" +
                "    \"enabled\": true,\n" +
                "    \"files\": [\"shakespear.txt\"],\n" +
                "    \"format\": \"utf8\"\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RAW_TEXT CONTAINS \"m\"?\"ther\")",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void RawTextComplexQueryTest() throws Exception {
        String query = "{\n" +
                "   \"query\" : {\n" +
                "      \"filtered\" : {\n" +
                "         \"query\" : {\n" +
                "            \"bool\" : {\n" +
                "               \"must\" : [\n" +
                "                  {\n" +
                "                     \"match_phrase\" : {\n" +
                "                        \"first_name\" : \"mary jane\"\n" +
                "                     }\n" +
                "                  },\n" +
                "                  {\n" +
                "                     \"match_phrase\" : {\n" +
                "                        \"last_name\" : \"smith\"\n" +
                "                     }\n" +
                "                  }\n" +
                "               ]\n" +
                "            }\n" +
                "         },\n" +
                "         \"ryft\": {\n" +
                "           \"enabled\": true,\n" +
                "           \"files\": [\"passengers.txt\"],\n" +
                "           \"format\": \"utf8\"\n" +
                "         }\n" +
                "      }\n" +
                "   }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("((RAW_TEXT CONTAINS ES(\"mary jane\", LINE=true)) AND (RAW_TEXT CONTAINS ES(\"smith\", LINE=true)))",
                ryftRequest.getQuery().buildRyftString());

        String query2 = "{\"query\": " +
                "   {\"bool\": {" +
                "       \"should\": [" +
                "           {\"match_phrase\": {\"text_entry\": {\"query\":\"juliet\", \"fuzziness\": 0, \"metric\": \"FEDS\"}}}, " +
                "           {\"match_phrase\": {\"text_entry\": {\"query\":\"romeo\", \"fuzziness\": 0, \"metric\": \"FEDS\"}}}, " +
                "           {\"match_phrase\": {\"text_entry\": {\"query\":\"knight\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}], " +
                "       \"must\": [" +
                "           {\"fuzzy\": {\"text_entry\" : {\"value\": \"hamlet\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}], " +
                "       \"must_not\": [" +
                "           {\"fuzzy\": {\"text_entry\" : {\"value\": \"love\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}], " +
                "       \"minimum_should_match\": 2}},\n" +
                "  \"ryft\": {\n" +
                "    \"enabled\": true,\n" +
                "    \"files\": [\"passengers.txt\"],\n" +
                "    \"format\": \"utf8\"\n" +
                "  }}";
        BytesArray bytes2 = new BytesArray(query2);
        XContentParser parser2 = XContentFactory.xContent(bytes).createParser(bytes2);
        ElasticConvertingContext context2 = contextFactory.create(parser2, query2);
        RyftRequestEvent ryftRequest2 = elasticConverter.convert(context2);
        assertNotNull(ryftRequest2);
        assertEquals("((((RAW_TEXT CONTAINS ES(\"juliet\", LINE=true)) " +
                        "AND (RAW_TEXT CONTAINS ES(\"romeo\", LINE=true))) OR ((RAW_TEXT CONTAINS ES(\"juliet\", LINE=true)) " +
                        "AND (RAW_TEXT CONTAINS ES(\"knight\", LINE=true))) OR ((RAW_TEXT CONTAINS ES(\"romeo\", LINE=true)) " +
                        "AND (RAW_TEXT CONTAINS ES(\"knight\", LINE=true)))) " + 
                        "AND (RAW_TEXT NOT_CONTAINS \"love\") AND (RAW_TEXT CONTAINS ES(\"hamlet\", LINE=true)))",
                ryftRequest2.getQuery().buildRyftString());
    }

    @Test
    public void DateTimeSearchTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"timestamp\": {\n" +
                "        \"value\": \"2014/01/01 07:00:00\",\n" +
                "        \"type\": \"datetime\",\n" +
                "        \"format\": \"yyyy/MM/dd HH:mm:ss\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.timestamp CONTAINS DATE(YYYY/MM/DD = 2014/01/01)) AND (RECORD.timestamp CONTAINS TIME(HH:MM:SS = 07:00:00)))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DateTimeRangeSearchTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"range\" : {\n" +
                "      \"timestamp\" : {\n" +
                "        \"gt\" : \"2014/01/01 07:00:00\",\n" +
                "        \"lt\" : \"2014/01/07 07:00:00\",\n" +
                "        \"type\": \"datetime\",\n" +
                "        \"format\": \"yyyy/MM/dd HH:mm:ss\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(((RECORD.timestamp CONTAINS DATE(YYYY/MM/DD = 2014/01/01)) AND (RECORD.timestamp CONTAINS TIME(HH:MM:SS > 07:00:00))) " +
                        "OR (RECORD.timestamp CONTAINS DATE(2014/01/01 < YYYY/MM/DD < 2014/01/07)) " +
                        "OR ((RECORD.timestamp CONTAINS DATE(YYYY/MM/DD = 2014/01/07)) AND (RECORD.timestamp CONTAINS TIME(HH:MM:SS < 07:00:00))))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DateTimeRangeSameDayTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"range\" : {\n" +
                "      \"timestamp\" : {\n" +
                "        \"gt\" : \"2014/01/01 07:00:00\",\n" +
                "        \"lt\" : \"2014/01/01 12:00:00\",\n" +
                "        \"type\": \"datetime\",\n" +
                "        \"format\": \"yyyy/MM/dd HH:mm:ss\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(((RECORD.timestamp CONTAINS DATE(YYYY/MM/DD = 2014/01/01)) AND (RECORD.timestamp CONTAINS TIME(HH:MM:SS > 07:00:00))) " +
                        "OR ((RECORD.timestamp CONTAINS DATE(YYYY/MM/DD = 2014/01/01)) AND (RECORD.timestamp CONTAINS TIME(HH:MM:SS < 12:00:00))))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DateTimeRangeLowerBoundTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"range\" : {\n" +
                "      \"timestamp\" : {\n" +
                "        \"gte\" : \"2014-01-01 07:00\",\n" +
                "        \"type\": \"datetime\",\n" +
                "        \"format\": \"yyyy-MM-dd HH:mm\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(((RECORD.timestamp CONTAINS DATE(YYYY-MM-DD = 2014-01-01)) AND (RECORD.timestamp CONTAINS TIME(HH:MM >= 07:00))) " +
                        "OR (RECORD.timestamp CONTAINS DATE(YYYY-MM-DD > 2014-01-01)))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DateTimeRangeUpperBoundTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"range\" : {\n" +
                "      \"timestamp\" : {\n" +
                "        \"lt\" : \"2014-01-01 07:00\",\n" +
                "        \"type\": \"datetime\",\n" +
                "        \"format\": \"yyyy-MM-dd HH:mm\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(((RECORD.timestamp CONTAINS DATE(YYYY-MM-DD = 2014-01-01)) AND (RECORD.timestamp CONTAINS TIME(HH:MM < 07:00))) " +
                        "OR (RECORD.timestamp CONTAINS DATE(YYYY-MM-DD < 2014-01-01)))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DateTimeRangeOnlyDateTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"range\" : {\n" +
                "      \"timestamp\" : {\n" +
                "        \"gt\" : \"2014-01-01\",\n" +
                "        \"lte\" : \"2014-01-02\",\n" +
                "        \"type\": \"datetime\",\n" +
                "        \"format\": \"yyyy-MM-dd\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.timestamp CONTAINS DATE(2014-01-01 < YYYY-MM-DD <= 2014-01-02))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DateTimeRangeOnlyTimeTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"range\" : {\n" +
                "      \"timestamp\" : {\n" +
                "        \"gt\" : \"07:00\",\n" +
                "        \"lte\" : \"09:00\",\n" +
                "        \"type\": \"datetime\",\n" +
                "        \"format\": \"HH:mm\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.timestamp CONTAINS TIME(07:00 < HH:MM <= 09:00))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void NumericSearchTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"price\": {\n" +
                "        \"value\": 20,\n" +
                "        \"type\": \"number\",\n" +
                "        \"separator\":\",\",\n" +
                "        \"decimal\":\".\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.price CONTAINS NUMBER(NUM = \"20\", \",\", \".\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void NumericSearchSimplifiedTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"price\": 20\n" +
                "    }\n" +
                "  }\n" +
                "}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.price CONTAINS NUMBER(NUM = \"20\", \",\", \".\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void NumericSearchArrayTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"price\": [20, 30]\n" +
                "    }\n" +
                "  }\n" +
                "}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.price CONTAINS NUMBER(NUM = \"20\", \",\", \".\")) OR (RECORD.price CONTAINS NUMBER(NUM = \"30\", \",\", \".\")))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void NumericRangeTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"range\" : {\n" +
                "      \"age\" : {\n" +
                "        \"gte\" : -1.01e2,\n" +
                "        \"lte\" : \"2000.12\",\n" +
                "        \"type\":\"number\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.age CONTAINS NUMBER(\"-1.01e2\" <= NUM <= \"2000.12\", \",\", \".\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void CurrencySearchTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"price\": {\n" +
                "        \"value\": \"$100\",\n" +
                "        \"type\": \"currency\",\n" +
                "        \"currency\":\"$\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.price CONTAINS CURRENCY(CUR = \"$100\", \"$\", \",\", \".\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void CurrencySearchSimpleTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"price\": {\n" +
                "        \"value\": \"100\",\n" +
                "        \"type\": \"currency\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.price CONTAINS CURRENCY(CUR = \"$100\", \"$\", \",\", \".\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void CurrencyRangeTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"range\" : {\n" +
                "      \"price\" : {\n" +
                "        \"gte\" : 10,\n" +
                "        \"lte\" : 20,\n" +
                "        \"type\":\"currency\",\n" +
                "        \"separator\":\",\",\n" +
                "        \"decimal\":\".\",\n" +
                "        \"currency\":\"%\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.price CONTAINS CURRENCY(\"%10\" <= CUR <= \"%20\", \"%\", \",\", \".\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void Ipv4SearchTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"ip_addr\": {\n" +
                "        \"value\": \"192.168.10.11\",\n" +
                "        \"type\": \"ipv4\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.ip_addr CONTAINS IPV4(IP = \"192.168.10.11\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void Ipv4SearchMaskTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"ip_addr\": {\n" +
                "        \"value\": \"192.168.0.0/16\",\n" +
                "        \"type\": \"ipv4\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.ip_addr CONTAINS IPV4(\"192.168.0.0\" <= IP <= \"192.168.255.255\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void Ipv4RangeTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"range\": {\n" +
                "      \"ip_addr\": {\n" +
                "        \"gte\": \"192.168.1.0\",\n" +
                "        \"lt\":  \"192.168.2.0\",\n" +
                "        \"type\": \"ipv4\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.ip_addr CONTAINS IPV4(\"192.168.1.0\" <= IP < \"192.168.2.0\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void Ipv6SearchTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"ip_addr\": {\n" +
                "        \"value\": \"2001::db8\",\n" +
                "        \"type\": \"ipv6\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.ip_addr CONTAINS IPV6(IP = \"2001::db8\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void Ipv6SearchMaskTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"ip_addr\": {\n" +
                "        \"value\": \"2001::db8/32\",\n" +
                "        \"type\": \"ipv6\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.ip_addr CONTAINS IPV6(\"2001::\" <= IP <= \"2001:0:ffff:ffff:ffff:ffff:ffff:ffff\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void Ipv6RangeTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"range\": {\n" +
                "      \"ip_addr\": {\n" +
                "        \"gte\": \"2001::db8\",\n" +
                "        \"lt\":  \"2001::db9\",\n" +
                "        \"type\": \"ipv6\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.ip_addr CONTAINS IPV6(\"2001::db8\" <= IP < \"2001::db9\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void DefaultDatatypeTermTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"name\": {\n" +
                "        \"value\": \"Tom\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.name CONTAINS \"Tom\")",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void RawTextNumericSearchTest() throws Exception {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"term\": {\n" +
                "      \"_all\": {\n" +
                "        \"query\": \"64\",\n" +
                "        \"type\": \"number\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"ryft\": {\n" +
                "    \"enabled\": true,\n" +
                "    \"files\": [\"shakespear.txt\"],\n" +
                "    \"format\": \"utf8\"\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);
        assertEquals("(RAW_TEXT CONTAINS NUMBER(NUM = \"64\", \",\", \".\"))",
                ryftRequest.getQuery().buildRyftString());
    }


    @Test
    public void DateTimeRangeMillisTest() throws Exception {
        long now = Instant.now().toEpochMilli();
        long earlier = now - 1000000000;

        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"range\" : {\n" +
                "      \"timestamp\" : {\n" +
                "        \"gt\" : \"" + earlier + "\",\n" +
                "        \"lt\" : \"" + now + "\",\n" +
                "        \"type\": \"datetime\",\n" +
                "        \"format\": \"epoch_millis\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context);
        assertNotNull(ryftRequest);

        Date nowDate = new Date(now);
        Date earlierDate = new Date(earlier);

        SimpleDateFormat dayFormat = new SimpleDateFormat(RyftExpressionDate.DEFAULT_FORMAT);
        SimpleDateFormat timeFormat = new SimpleDateFormat(RyftExpressionTime.DEFAULT_FORMAT);

        String nowDay = dayFormat.format(nowDate);
        String nowTime = timeFormat.format(nowDate);

        String earlierDay = dayFormat.format(earlierDate);
        String earlierTime = timeFormat.format(earlierDate);

        assertEquals("(((RECORD.timestamp CONTAINS DATE(YYYY-MM-DD = " + earlierDay + ")) AND (RECORD.timestamp CONTAINS TIME(HH:MM:SS > " + earlierTime + "))) " +
                        "OR (RECORD.timestamp CONTAINS DATE(" + earlierDay + " < YYYY-MM-DD < " + nowDay + ")) " +
                        "OR ((RECORD.timestamp CONTAINS DATE(YYYY-MM-DD = " + nowDay + ")) AND (RECORD.timestamp CONTAINS TIME(HH:MM:SS < " + nowTime + "))))",
                ryftRequest.getQuery().buildRyftString());
    }
}
