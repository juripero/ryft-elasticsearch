package com.ryft.elasticsearch.rest.mappings;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RyftExtra {

    @JsonProperty("aggregations")
    private ObjectNode aggregations;

    public RyftExtra(ObjectNode aggregations) {
        this.aggregations = aggregations;
    }

    public RyftExtra() {
    }

    public ObjectNode getAggregations() {
        return aggregations;
    }

    public void setAggregations(ObjectNode aggregations) {
        this.aggregations = aggregations;
    }
}
