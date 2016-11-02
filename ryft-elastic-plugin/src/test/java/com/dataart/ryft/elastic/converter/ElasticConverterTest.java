package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.disruptor.messages.RyftRequestEventFactory;
import com.dataart.ryft.elastic.plugin.JSR250Module;
import com.dataart.ryft.elastic.plugin.PropertiesProvider;
import com.dataart.ryft.elastic.plugin.RyftProperties;
import com.dataart.ryft.utils.Try;
import java.io.IOException;
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
    public void SimpleMatchPhraseRequestTest() throws IOException {
        String query = "{\"query\": {\"match_phrase\": {\"text_entry\": "
                + "{\"query\": \"good mother\", \"fuzziness\" :2, \"metric\": \"FHS\"}}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        Try<RyftRequestEvent> tryRyftRequest = elasticConverter.convert(context);
        assertNotNull(tryRyftRequest);
        assertFalse(tryRyftRequest.hasError());
        assertEquals(tryRyftRequest.getResult().getQuery().buildRyftString(), "(RECORD.doc.text_entry CONTAINS FHS(\"good mother\", DIST=2))");
    }

    @Test
    public void SimpleMatchRequestTest() throws IOException {
        String query = "{\"query\": {\"match\": {\"text_entry\": "
                + "{\"query\": \"good mother\", \"metric\": \"feds\", \"fuzziness\": 2, \"operator\": \"or\"}}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        Try<RyftRequestEvent> tryRyftRequest = elasticConverter.convert(context);
        assertNotNull(tryRyftRequest);
        assertFalse(tryRyftRequest.hasError());
        assertEquals(tryRyftRequest.getResult().getQuery().buildRyftString(),
                "((RECORD.doc.text_entry CONTAINS FEDS(\"good\", DIST=2)) OR (RECORD.doc.text_entry CONTAINS FEDS(\"mother\", DIST=2)))");
    }

    @Test
    public void SimpleFuzzyRequestTest() throws IOException {
        String query = "{\"query\": {\"fuzzy\": {\"text_entry\": "
                + "{\"value\": \"good mother\", \"metric\": \"fhs\", \"fuzziness\": 2}}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        Try<RyftRequestEvent> tryRyftRequest = elasticConverter.convert(context);
        assertNotNull(tryRyftRequest);
        assertFalse(tryRyftRequest.hasError());
        assertEquals(tryRyftRequest.getResult().getQuery().buildRyftString(),
                "(RECORD.doc.text_entry CONTAINS FHS(\"goodmother\", DIST=2))");
    }

    @Test
    public void ComplexFuzzyRequestTest1() throws IOException {
        String query = "{\"query\": {\"bool\": {\"must\": ["
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"Would nat be\", \"fuzziness\": 1, \"metric\": \"Fhs\"}}}, "
                + "{\"fuzzy\": {\"text_entry\" : {\"value\": \"knight\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}]}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        Try<RyftRequestEvent> tryRyftRequest = elasticConverter.convert(context);
        assertNotNull(tryRyftRequest);
        assertFalse(tryRyftRequest.hasError());
        assertEquals(tryRyftRequest.getResult().getQuery().buildRyftString(),
                "((RECORD.doc.text_entry CONTAINS FHS(\"Would nat be\", DIST=1)) AND (RECORD.doc.text_entry CONTAINS \"knight\"))");
    }

}
