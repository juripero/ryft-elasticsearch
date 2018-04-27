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
        }
        JsonNode ryftEnabledJsonNode = objectNode.findValue(RYFT_ENABLED_PROPERTY);
        if (ryftEnabledJsonNode != null) {
            result.put(RYFT_INTEGRATION_ENABLED, ryftEnabledJsonNode.asBoolean());
        }
        return result;
    }
}
