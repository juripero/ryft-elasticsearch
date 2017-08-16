package com.ryft.elasticsearch.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.ryft.elasticsearch.converter.ryftdsl.RyftFormat;
import static com.ryft.elasticsearch.plugin.PropertiesProvider.RYFT_CASE_SENSITIVE;
import static com.ryft.elasticsearch.plugin.PropertiesProvider.RYFT_FILES_TO_SEARCH;
import static com.ryft.elasticsearch.plugin.PropertiesProvider.RYFT_FORMAT;
import static com.ryft.elasticsearch.plugin.PropertiesProvider.RYFT_INTEGRATION_ENABLED;
import static com.ryft.elasticsearch.plugin.PropertiesProvider.RYFT_MAPPING;
import static com.ryft.elasticsearch.plugin.PropertiesProvider.SEARCH_QUERY_LIMIT;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryConverterHelper {

    static final String SIZE_PROPERTY = "size";
    static final String RYFT_PROPERTY = "ryft";
    static final String RYFT_ENABLED_PROPERTY = "ryft_enabled";
    static final String ENABLED_PROPERTY = "enabled";
    static final String FILES_PROPERTY = "files";
    static final String FORMAT_PROPERTY = "format";
    static final String CASE_SENSITIVE_PROPERTY = "case_sensitive";
    static final String MAPPING_PROPERTY = "mapping";

    static Map<String, Object> getQueryProperties(ElasticConvertingContext convertingContext) {
        Map<String, Object> result = new HashMap<>();
        if (convertingContext.getQueryJsonNode().has(SIZE_PROPERTY)) {
            result.put(SEARCH_QUERY_LIMIT, convertingContext.getQueryJsonNode().get(SIZE_PROPERTY).asInt(0));
        }
        JsonNode ryftJsonNode = convertingContext.getQueryJsonNode().findValue(RYFT_PROPERTY);
        if (ryftJsonNode != null) {
            if (ryftJsonNode.has(ENABLED_PROPERTY)) {
                result.put(RYFT_INTEGRATION_ENABLED, ryftJsonNode.get(ENABLED_PROPERTY).asBoolean());
            }
            if (ryftJsonNode.has(FILES_PROPERTY)) {
                List<String> files = new ArrayList<>();
                ryftJsonNode.get(FILES_PROPERTY).elements().forEachRemaining(jsonNode -> files.add(jsonNode.asText()));
                result.put(RYFT_FILES_TO_SEARCH, files);
            }
            if (ryftJsonNode.has(FORMAT_PROPERTY)) {
                result.put(RYFT_FORMAT, RyftFormat.get(ryftJsonNode.get(FORMAT_PROPERTY).asText()));
            }
            if (ryftJsonNode.has(CASE_SENSITIVE_PROPERTY)) {
                result.put(RYFT_CASE_SENSITIVE, ryftJsonNode.get(CASE_SENSITIVE_PROPERTY).asBoolean());
            }
            if (ryftJsonNode.has(MAPPING_PROPERTY)) {
                Map<String, Object> mapping = convertingContext.getObjectMapper().convertValue(ryftJsonNode.get(MAPPING_PROPERTY), Map.class);
                result.put(RYFT_MAPPING, mapping);
            }
        }
        JsonNode ryftEnabledJsonNode = convertingContext.getQueryJsonNode().findValue(RYFT_ENABLED_PROPERTY);
        if (ryftEnabledJsonNode != null) {
            result.put(RYFT_INTEGRATION_ENABLED, ryftEnabledJsonNode.asBoolean());
        }
        return result;
    }
}
