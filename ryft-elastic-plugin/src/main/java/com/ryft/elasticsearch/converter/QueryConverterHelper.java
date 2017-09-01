package com.ryft.elasticsearch.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ryft.elasticsearch.converter.ryftdsl.RyftFormat;
import static com.ryft.elasticsearch.plugin.PropertiesProvider.*;
import com.ryft.elasticsearch.plugin.RyftProperties;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryConverterHelper {

    public static final String SIZE_PROPERTY = "size";
    public static final String RYFT_PROPERTY = "ryft";
    public static final String RYFT_ENABLED_PROPERTY = "ryft_enabled";
    public static final String ENABLED_PROPERTY = "enabled";
    public static final String FILES_PROPERTY = "files";
    public static final String FORMAT_PROPERTY = "format";
    public static final String LIMIT_PROPERTY = "limit";
    public static final String CASE_SENSITIVE_PROPERTY = "case_sensitive";
    public static final String MAPPING_PROPERTY = "mapping";

    static Map<String, Object> getQueryProperties(ElasticConvertingContext convertingContext) {
        Map<String, Object> result = new HashMap<>();
        ObjectNode objectNode = convertingContext.getQueryObjectNode();
        if (objectNode.has(SIZE_PROPERTY)) {
            result.put(ES_RESULT_SIZE, objectNode.get(SIZE_PROPERTY).asInt());
        }
        JsonNode ryftJsonNode = objectNode.findValue(RYFT_PROPERTY);
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
                result.put(RYFT_MAPPING, new RyftProperties(mapping));
            }
            if (ryftJsonNode.has(LIMIT_PROPERTY)) {
                result.put(RYFT_QUERY_LIMIT, ryftJsonNode.get(LIMIT_PROPERTY).asInt(-1));
            }
        }
        JsonNode ryftEnabledJsonNode = objectNode.findValue(RYFT_ENABLED_PROPERTY);
        if (ryftEnabledJsonNode != null) {
            result.put(RYFT_INTEGRATION_ENABLED, ryftEnabledJsonNode.asBoolean());
        }
        return result;
    }
}
