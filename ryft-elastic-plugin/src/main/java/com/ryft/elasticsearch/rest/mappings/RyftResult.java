package com.ryft.elasticsearch.rest.mappings;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class RyftResult {

    @JsonProperty("_index")
    private RyftIndex index;
    private final ObjectNode record = JsonNodeFactory.instance.objectNode();

    public RyftResult() {
    }

    @JsonAnyGetter
    public ObjectNode record() {
        return record;
    }

    @JsonAnySetter
    public void setRecordProperty(String name, JsonNode value) {
        record.set(name, value);
    }

    public RyftIndex getIndex() {
        return index;
    }

    public void setIndex(RyftIndex index) {
        this.index = index;
    }
}
