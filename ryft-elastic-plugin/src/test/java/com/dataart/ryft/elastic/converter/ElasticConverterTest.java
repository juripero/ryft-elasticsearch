package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.disruptor.messages.RyftRequestEventFactory;
import com.dataart.ryft.elastic.plugin.JSR250Module;
import com.dataart.ryft.elastic.plugin.PropertiesProvider;
import com.dataart.ryft.elastic.plugin.RyftProperties;
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
        assertNotNull(ryftRequest);
        assertEquals("(((RECORD.text_entry CONTAINS \"juliet\") AND (RECORD.text_entry CONTAINS \"romeo\") AND (RECORD.text_entry CONTAINS \"knight\")) OR "
                + "((RECORD.text_entry CONTAINS \"juliet\") AND (RECORD.text_entry CONTAINS \"romeo\") AND (RECORD.text_entry CONTAINS \"hamlet\")) OR "
                + "((RECORD.text_entry CONTAINS \"juliet\") AND (RECORD.text_entry CONTAINS \"knight\") AND (RECORD.text_entry CONTAINS \"hamlet\")) OR "
                + "((RECORD.text_entry CONTAINS \"romeo\") AND (RECORD.text_entry CONTAINS \"knight\") AND (RECORD.text_entry CONTAINS \"hamlet\")))",
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS \"good mother\")",
                ryftRequest.getQuery().buildRyftString());
        query = "{\"from\": 500, \"query\": {\"match_phrase\": {\"text_entry\": "
                + "{\"query\": \"good mother\", \"fuzziness\" :0}}}}";
        bytes = new BytesArray(query);
        parser = XContentFactory.xContent(bytes).createParser(bytes);
        context = contextFactory.create(parser, query);
        ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS FEDS(\"mother\", DIST=2))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void SimpilfiedExactSearchMatchPhraseRequestTest() throws Exception {
        String query = "{\"query\": {\"match_phrase\": {\"text_entry\": \"good mother\"}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
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
        RyftRequestEvent ryftRequest = elasticConverter.convert(context).getResultOrException();
        assertNotNull(ryftRequest);
        assertEquals("(RECORD.text_entry CONTAINS \"test\")",
                ryftRequest.getQuery().buildRyftString());
        assertTrue(ryftRequest.getRyftSearchUrl().contains("file=1.txt&file=2.json"));
        assertTrue(ryftRequest.getRyftSearchUrl().contains("format=xml"));
    }
}
