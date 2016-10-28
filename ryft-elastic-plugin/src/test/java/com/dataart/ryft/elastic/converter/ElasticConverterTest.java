package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.utils.Try;
import java.io.IOException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Inject;
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
    public ElasticConverter elasticParser;

    @Inject
    public ContextFactory contextFactory;

    @Before
    public void setUp() {
        Guice.createInjector(new ElasticConversionModule()).injectMembers(this);
    }

    @Test
    public void SimpleFuzzyRequestTest1() throws IOException {
        String query = "{\"query\": {\"match_phrase\": {\"text_entry\": "
                + "{\"query\": \"knight\", \"fuzziness\" :2, \"metric\": \"FHS\"}}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        Try<RyftQuery> maybeRyftQuery = elasticParser.convert(context);
        assertTrue(!maybeRyftQuery.hasError());
        assertEquals(maybeRyftQuery.getResult().buildRyftString(), "(RECORD.text_entry CONTAINS FHS(\"knight\", DIST=2))");
    }

    @Test
    public void SimpleFuzzyRequestTest2() throws IOException {
        String query = "{\"query\": {\"fuzzy\": {\"text_entry\" : "
                + "{\"value\": \"knight\", \"fuzziness\" :1, \"metric\": \"feds\"}}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        Try<RyftQuery> maybeRyftQuery = elasticParser.convert(context);
        assertTrue(!maybeRyftQuery.hasError());
        assertEquals(maybeRyftQuery.getResult().buildRyftString(), "(RECORD.text_entry CONTAINS FEDS(\"knight\", DIST=1))");
    }

    @Test
    public void ComplexFuzzyRequestTest1() throws IOException {
        String query = "{\"query\": {\"bool\": {\"must\": ["
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"Would nat be\", \"fuzziness\": 1, \"metric\": \"Fhs\"}}}, "
                + "{\"fuzzy\": {\"text_entry\" : {\"value\": \"knight\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}]}}}";
        BytesArray bytes = new BytesArray(query);
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        ElasticConvertingContext context = contextFactory.create(parser, query);
        Try<RyftQuery> maybeRyftQuery = elasticParser.convert(context);
        assertTrue(!maybeRyftQuery.hasError());
        assertEquals(maybeRyftQuery.getResult().buildRyftString(),
                "((RECORD.text_entry CONTAINS FHS(\"Would nat be\", DIST=1)) AND (RECORD.text_entry CONTAINS \"knight\"))");
    }

}
