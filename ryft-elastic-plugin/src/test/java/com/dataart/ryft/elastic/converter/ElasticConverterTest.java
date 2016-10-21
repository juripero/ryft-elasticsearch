package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import java.io.IOException;
import java.util.Optional;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class ElasticConverterTest {

    @Inject
    public ElasticConverter elasticParser;

    public ElasticConverterTest() {
    }

    @Before
    public void setUp() {
        Guice.createInjector(new ElasticConversionModule()).injectMembers(this);
    }

    @Test
    public void SimpleFuzzyRequestTest1() throws IOException {
        BytesArray bytes = new BytesArray("{\"query\": {\"match_phrase\": {\"text_entry\": "
                + "{\"value\": \"knight\", \"fuzziness\" :2, \"metric\": \"FHS\"}}}}");
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        Optional<RyftQuery> maybeRyftQuery = elasticParser.parse(parser);
        assertTrue(maybeRyftQuery.isPresent());
        assertEquals(maybeRyftQuery.get().buildRyftString(), "(RECORD.text_entry CONTAINS FHS(\"knight\", DIST=2))");
    }

    @Test
    public void SimpleFuzzyRequestTest2() throws IOException {
        BytesArray bytes = new BytesArray("{\"query\": {\"fuzzy\": {\"text_entry\" : "
                + "{\"value\": \"knight\", \"fuzziness\" :1, \"metric\": \"feds\"}}}}");
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        Optional<RyftQuery> maybeRyftQuery = elasticParser.parse(parser);
        assertTrue(maybeRyftQuery.isPresent());
        assertEquals(maybeRyftQuery.get().buildRyftString(), "(RECORD.text_entry CONTAINS FEDS(\"knight\", DIST=1))");
    }

    @Test
    public void ComplexFuzzyRequestTest1() throws IOException {
        BytesArray bytes = new BytesArray("{\"query\": {\"bool\": {\"must\": ["
                + "{\"match_phrase\": {\"text_entry\": {\"query\":\"Would nat be\", \"fuzziness\": 1, \"metric\": \"Fhs\"}}}, "
                + "{\"fuzzy\": {\"text_entry\" : {\"value\": \"knight\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}]}}}");
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        Optional<RyftQuery> maybeRyftQuery = elasticParser.parse(parser);
        assertTrue(maybeRyftQuery.isPresent());
        assertEquals(maybeRyftQuery.get().buildRyftString(), 
                "((RECORD.text_entry CONTAINS FHS(\"Would nat be\", DIST=1)) AND (RECORD.text_entry CONTAINS \"knight\"))");
    }
}
