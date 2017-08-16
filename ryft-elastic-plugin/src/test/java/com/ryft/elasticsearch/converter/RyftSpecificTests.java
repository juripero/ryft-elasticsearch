package com.ryft.elasticsearch.converter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import com.ryft.elasticsearch.converter.ryftdsl.RyftFormat;
import static com.ryft.elasticsearch.plugin.PropertiesProvider.*;
import com.ryft.elasticsearch.utils.JSR250Module;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Inject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

public class RyftSpecificTests {

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
    public void RawTextNumericSearchTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"_all\": {\n"
                + "        \"query\": \"64\",\n"
                + "        \"type\": \"number\"\n"
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
        assertEquals(RyftFormat.UTF8, ryftRequest.getRyftProperties().get(RYFT_FORMAT));
        assertEquals(Lists.newArrayList("shakespear.txt"),
                ryftRequest.getRyftProperties().get(RYFT_FILES_TO_SEARCH));
        assertEquals("(RAW_TEXT CONTAINS NUMBER(NUM = \"64\", \",\", \".\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void RawTextMatchSearchTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"match\": {\n"
                + "      \"_all\": {\n"
                + "        \"query\": \"good mother\",\n"
                + "        \"fuzziness\": 1,\n"
                + "        \"width\": 30\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft\": {\n"
                + "    \"enabled\": true,\n"
                + "    \"files\": [\"shakespear.txt\"],\n"
                + "    \"format\": \"utf8\"\n"
                + "  }\n"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertTrue(ryftRequest.isFileSearch());
        assertEquals(RyftFormat.UTF8, ryftRequest.getRyftProperties().get(RYFT_FORMAT));
        assertEquals(Lists.newArrayList("shakespear.txt"),
                ryftRequest.getRyftProperties().get(RYFT_FILES_TO_SEARCH));
        assertEquals("((RAW_TEXT CONTAINS FEDS(\"good\", WIDTH=30, DIST=1)) OR (RAW_TEXT CONTAINS FEDS(\"mother\", WIDTH=30, DIST=1)))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void RawTextMatchWithAndOperatorTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"match\": {\n"
                + "      \"_all\": {\n"
                + "        \"query\": \"good mother\",\n"
                + "        \"fuzziness\": 1,\n"
                + "        \"operator\": \"AND\",\n"
                + "        \"width\": 30\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft\": {\n"
                + "    \"enabled\": true,\n"
                + "    \"files\": [\"shakespear.txt\"],\n"
                + "    \"format\": \"utf8\"\n"
                + "  }\n"
                + "}";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertTrue(ryftRequest.isFileSearch());
        assertEquals(RyftFormat.UTF8, ryftRequest.getRyftProperties().get(RYFT_FORMAT));
        assertEquals(Lists.newArrayList("shakespear.txt"),
                ryftRequest.getRyftProperties().get(RYFT_FILES_TO_SEARCH));
        assertEquals("((RAW_TEXT CONTAINS FEDS(\"good\", LINE=true, DIST=1)) AND (RAW_TEXT CONTAINS FEDS(\"mother\", LINE=true, DIST=1)))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void RawTextMatchPhraseSearchTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"match_phrase\": {\n"
                + "      \"_all\": {\n"
                + "        \"query\": \"good mother\",\n"
                + "        \"fuzziness\": 1,\n"
                + "        \"width\": \"line\"\n"
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
        assertEquals(RyftFormat.UTF8, ryftRequest.getRyftProperties().get(RYFT_FORMAT));
        assertEquals(Lists.newArrayList("shakespear.txt"),
                ryftRequest.getRyftProperties().get(RYFT_FILES_TO_SEARCH));
        assertEquals("(RAW_TEXT CONTAINS FEDS(\"good mother\", LINE=true, DIST=1))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void RawTextTermSearchTest() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"wildcard\": {\n"
                + "      \"_all\": {\n"
                + "        \"value\": \"m?ther\"\n"
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
        assertEquals(RyftFormat.UTF8, ryftRequest.getRyftProperties().get(RYFT_FORMAT));
        assertEquals(Lists.newArrayList("shakespear.txt"),
                ryftRequest.getRyftProperties().get(RYFT_FILES_TO_SEARCH));
        assertEquals("(RAW_TEXT CONTAINS \"m\"?\"ther\")",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void RawTextComplexQueryTest() throws Exception {
        String query = "{\n"
                + "   \"query\" : {\n"
                + "      \"filtered\" : {\n"
                + "         \"query\" : {\n"
                + "            \"bool\" : {\n"
                + "               \"must\" : [\n"
                + "                  {\n"
                + "                     \"match_phrase\" : {\n"
                + "                        \"first_name\" : \"mary jane\"\n"
                + "                     }\n"
                + "                  },\n"
                + "                  {\n"
                + "                     \"match_phrase\" : {\n"
                + "                        \"last_name\" : \"smith\"\n"
                + "                     }\n"
                + "                  }\n"
                + "               ]\n"
                + "            }\n"
                + "         },\n"
                + "         \"ryft\": {\n"
                + "           \"enabled\": true,\n"
                + "           \"files\": [\"passengers.txt\"],\n"
                + "           \"format\": \"utf8\"\n"
                + "         }\n"
                + "      }\n"
                + "   }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertTrue(ryftRequest.isFileSearch());
        assertEquals(RyftFormat.UTF8, ryftRequest.getRyftProperties().get(RYFT_FORMAT));
        assertEquals(Lists.newArrayList("passengers.txt"),
                ryftRequest.getRyftProperties().get(RYFT_FILES_TO_SEARCH));
        assertEquals("((RAW_TEXT CONTAINS ES(\"mary jane\", LINE=true)) AND (RAW_TEXT CONTAINS ES(\"smith\", LINE=true)))",
                ryftRequest.getQuery().buildRyftString());

        String query2 = "{\"query\": "
                + "   {\"bool\": {"
                + "       \"should\": ["
                + "           {\"match_phrase\": {\"text_entry\": {\"query\":\"juliet\", \"fuzziness\": 0, \"metric\": \"FEDS\"}}}, "
                + "           {\"match_phrase\": {\"text_entry\": {\"query\":\"romeo\", \"fuzziness\": 0, \"metric\": \"FEDS\"}}}, "
                + "           {\"match_phrase\": {\"text_entry\": {\"query\":\"knight\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}], "
                + "       \"must\": ["
                + "           {\"fuzzy\": {\"text_entry\" : {\"value\": \"hamlet\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}], "
                + "       \"must_not\": ["
                + "           {\"fuzzy\": {\"text_entry\" : {\"value\": \"love\", \"fuzziness\" :0, \"metric\": \"FEDS\"}}}], "
                + "       \"minimum_should_match\": 2}},\n"
                + "  \"ryft\": {\n"
                + "    \"enabled\": true,\n"
                + "    \"files\": [\"passengers.txt\"],\n"
                + "    \"format\": \"utf8\"\n"
                + "  }}";
        request = new SearchRequest(new String[]{""}, query2.getBytes());
        RyftRequestParameters ryftRequest2 = elasticConverter.convert(request);
        assertNotNull(ryftRequest2);
        assertTrue(ryftRequest2.isFileSearch());
        assertEquals(RyftFormat.UTF8, ryftRequest.getRyftProperties().get(RYFT_FORMAT));
        assertEquals(Lists.newArrayList("passengers.txt"),
                ryftRequest.getRyftProperties().get(RYFT_FILES_TO_SEARCH));
        assertEquals("((((RAW_TEXT CONTAINS ES(\"juliet\", LINE=true)) "
                + "AND (RAW_TEXT CONTAINS ES(\"romeo\", LINE=true))) OR ((RAW_TEXT CONTAINS ES(\"juliet\", LINE=true)) "
                + "AND (RAW_TEXT CONTAINS ES(\"knight\", LINE=true))) OR ((RAW_TEXT CONTAINS ES(\"romeo\", LINE=true)) "
                + "AND (RAW_TEXT CONTAINS ES(\"knight\", LINE=true)))) "
                + "AND (RAW_TEXT NOT_CONTAINS \"love\") AND (RAW_TEXT CONTAINS ES(\"hamlet\", LINE=true)))",
                ryftRequest2.getQuery().buildRyftString());
    }

    @Test
    public void LimitedSearchTest() throws Exception {
        String query = "{\"query\": {\"match\": {\"text_entry\": \"good mother\"}}, \"size\": 5,"
                + "\"aggs\": {\"1\":{\"terms\": {\"field\": \"xx\", \"size\": 20}}}}";
        SearchRequest request = new SearchRequest(new String[]{"test"}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertEquals(new Integer(5), ryftRequest.getRyftProperties().getInt(SEARCH_QUERY_LIMIT));
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS \"good\") OR (RECORD.text_entry CONTAINS \"mother\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void RyftEnabledTest() throws Exception {
        String query = "{\"query\": {\"match\": {\"text_entry\": \"good mother\"}}, "
                + "\"ryft_enabled\": false}";
        SearchRequest request = new SearchRequest(new String[]{"test"}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertEquals(false, ryftRequest.getRyftProperties().getBool(RYFT_INTEGRATION_ENABLED));
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS \"good\") OR (RECORD.text_entry CONTAINS \"mother\"))",
                ryftRequest.getQuery().buildRyftString());
    }

    @Test
    public void MappingTest1() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"match_phrase\": {\n"
                + "      \"_all\": {\n"
                + "        \"query\": \"good mother\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft\": {\n"
                + "    \"enabled\": true,\n"
                + "    \"files\": [\"shakespear.json\"],\n"
                + "    \"mapping\": {\n"
                + "      \"registered\": \"type=date,format=yyyy-MM-dd HH:mm:ss\",\n"
                + "      \"location\": \"type=geo_point\""
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertTrue(ryftRequest.isFileSearch());
        assertEquals(Lists.newArrayList("shakespear.json"),
                ryftRequest.getRyftProperties().get(RYFT_FILES_TO_SEARCH));
        assertEquals("(RECORD CONTAINS \"good mother\")",
                ryftRequest.getQuery().buildRyftString());
        assertEquals(
                ImmutableMap.of(
                        "registered", "type=date,format=yyyy-MM-dd HH:mm:ss",
                        "location", "type=geo_point"),
                ryftRequest.getRyftProperties().get(RYFT_MAPPING));
    }

    @Test
    public void MappingTest2() throws Exception {
        String query = "{\n"
                + "  \"query\": {\n"
                + "    \"match_phrase\": {\n"
                + "      \"_all\": {\n"
                + "        \"query\": \"good mother\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"ryft\": {\n"
                + "    \"enabled\": true,\n"
                + "    \"files\": [\"shakespear.json\"],\n"
                + "    \"mapping\": {\n"
                + "      \"registered\": {\n"
                + "        \"type\": \"date\",\n"
                + "        \"format\": \"yyyy-MM-dd HH:mm:ss\"\n"
                + "      }\n,"
                + "      \"location\": {\n"
                + "        \"type\": \"geo_point\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n";
        SearchRequest request = new SearchRequest(new String[]{""}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertNotNull(ryftRequest);
        assertTrue(ryftRequest.isFileSearch());
        assertEquals(Lists.newArrayList("shakespear.json"),
                ryftRequest.getRyftProperties().get(RYFT_FILES_TO_SEARCH));
        assertEquals("(RECORD CONTAINS \"good mother\")",
                ryftRequest.getQuery().buildRyftString());
        assertEquals(
                ImmutableMap.of(
                        "registered", ImmutableMap.of("type", "date", "format", "yyyy-MM-dd HH:mm:ss"),
                        "location", ImmutableMap.of("type", "geo_point")),
                ryftRequest.getRyftProperties().get(RYFT_MAPPING));
    }

    @Test
    public void CaseSensitiveSearchTest() throws Exception {
        String query = "{\"query\": {\"match\": {\"text_entry\": \"good mother\"}}, "
                + "\"ryft\":{\"case_sensitive\": true}}";
        SearchRequest request = new SearchRequest(new String[]{"test"}, query.getBytes());
        RyftRequestParameters ryftRequest = elasticConverter.convert(request);
        assertEquals(true, ryftRequest.getRyftProperties().getBool(RYFT_CASE_SENSITIVE));
        assertNotNull(ryftRequest);
        assertEquals("((RECORD.text_entry CONTAINS \"good\") OR (RECORD.text_entry CONTAINS \"mother\"))",
                ryftRequest.getQuery().buildRyftString());
    }
}
