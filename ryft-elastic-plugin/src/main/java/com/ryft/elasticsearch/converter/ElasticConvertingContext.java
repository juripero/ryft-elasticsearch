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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ryft.elasticsearch.converter.ryftdsl.RyftOperator;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQueryFactory;
import com.google.common.collect.ImmutableMap;
import com.ryft.elasticsearch.plugin.ObjectMapperFactory;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

public class ElasticConvertingContext {

    public static enum ElasticSearchType {
        MATCH, MATCH_PHRASE, FUZZY, WILDCARD, REGEX, TERM, RANGE
    }

    public static enum ElasticDataType {
        STRING, DATETIME, NUMBER, NUMBER_ARRAY, CURRENCY, IPV4, IPV6
    }

    public static enum ElasticBoolSearchType {
        MUST, MUST_NOT, SHOULD
    }

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConvertingContext.class);

    private XContentParser contentParser;
    private final Map<String, ElasticConvertingElement> elasticConverters;
    private String originalQuery;
    private final RyftQueryFactory ryftQueryFactory;
    private ElasticSearchType searchType;
    private RyftOperator ryftOperator = RyftOperator.CONTAINS;
    private Integer minimumShouldMatch = 1;
    private Boolean minimumShouldMatchDefined = false;
    private Boolean line;
    private Integer width;
    private ElasticDataType dataType = ElasticDataType.STRING;
    private List<String> searchArray; //FIXME - workaround for timeseries
    private String[] indices;
    private final ObjectMapperFactory objectMapperFactory;
    private ObjectMapper objectMapper;
    private JsonNode queryJsonNode;
    private Boolean filtered = false;

    @Inject
    public ElasticConvertingContext(Map<String, ElasticConvertingElement> injectedConverters,
            RyftQueryFactory ryftQueryFactory, ObjectMapperFactory objectMapperFactory) {
        this.elasticConverters = ImmutableMap.copyOf(injectedConverters);
        this.ryftQueryFactory = ryftQueryFactory;
        this.objectMapperFactory = objectMapperFactory;
    }

    public void setSearchRequest(SearchRequest searchRequest) throws ElasticConversionException, IOException {
        BytesReference searchContent = searchRequest.source();
        if ((searchContent == null) || !(searchContent.hasArray())) {
            throw new ElasticConversionException("Can not get search query");
        } else {
            this.originalQuery = searchContent.toUtf8();
            this.contentParser = XContentFactory.xContent(searchContent).createParser(searchContent);
            this.objectMapper = objectMapperFactory.get(contentParser.contentType());
            this.queryJsonNode = objectMapper.readTree(searchContent.toBytes());
        }
        this.indices = searchRequest.indices();
    }

//    private static ObjectMapper createObjectMapper(XContentType xContentType) {
//        JsonFactory jsonFactory = null;
//        switch (xContentType) {
//            case JSON:
//                jsonFactory = new JsonFactory();
//                jsonFactory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
//                jsonFactory.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
//                jsonFactory.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
//                jsonFactory.configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false);
//                break;
//            case CBOR:
//                jsonFactory = new CBORFactory();
//                jsonFactory.configure(CBORFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false);
//                break;
//            case YAML:
//                jsonFactory = new YAMLFactory();
//                break;
//            case SMILE:
//                jsonFactory = new SmileFactory();
//                ((SmileFactory) jsonFactory).configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false);
//                jsonFactory.configure(SmileFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false);
//        }
//        ObjectMapper result = new ObjectMapper(jsonFactory);
//        result.configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, false);
//        return result;
//    }

    public XContentParser getContentParser() {
        return contentParser;
    }

    public ElasticConvertingElement getElasticConverter(String name) {
        ElasticConvertingElement result = elasticConverters.get(name);
        if (result == null) {
            LOGGER.debug("Failed to find appropriate converter for token: '{}'", name);
            result = elasticConverters.get(ElasticConverterUnknown.NAME);
        }
        LOGGER.debug("Return converter {}", result.getClass().getSimpleName());
        return result;
    }

    public String getOriginalQuery() {
        return originalQuery;
    }

    public RyftOperator getRyftOperator() {
        return ryftOperator;
    }

    public void setRyftOperator(RyftOperator ryftOperator) {
        this.ryftOperator = ryftOperator;
    }

    public ElasticSearchType getSearchType() {
        return searchType;
    }

    public void setSearchType(ElasticSearchType searchType) {
        this.searchType = searchType;
    }

    public Integer getMinimumShouldMatch() {
        return minimumShouldMatch;
    }

    public void setMinimumShouldMatch(Integer minimumShouldMatch) {
        if ((minimumShouldMatch != null) && (minimumShouldMatch >= 1)) {
            minimumShouldMatchDefined = true;
            this.minimumShouldMatch = minimumShouldMatch;
        }
    }

    public Boolean isMinimumShouldMatchDefined() {
        return minimumShouldMatchDefined;
    }

    public Boolean getLine() {
        return line;
    }

    public void setLine(Boolean line) {
        this.line = line;
    }

    public Integer getWidth() {
        return width;
    }

    public void setWidth(Integer width) {
        this.width = width;
    }

    public ElasticDataType getDataType() {
        return dataType;
    }

    public void setDataType(ElasticDataType dataType) {
        this.dataType = dataType;
    }

    public RyftQueryFactory getQueryFactory() {
        return ryftQueryFactory;
    }

    public List<String> getSearchArray() {
        return searchArray;
    }

    public void setSearchArray(List<String> searchArray) {
        this.searchArray = searchArray;
    }

    public String[] getIndices() {
        return indices;
    }

    public void setIndices(String[] indices) {
        this.indices = indices;
    }

    public Boolean getFiltered() {
        return filtered;
    }

    public void setFiltered(Boolean filtered) {
        this.filtered = filtered;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public ObjectNode getQueryObjectNode() {
        return (ObjectNode) queryJsonNode;
    }

    public Map<String, Object> getQueryMap() {
        return objectMapper.convertValue(queryJsonNode, Map.class);
    }
}
