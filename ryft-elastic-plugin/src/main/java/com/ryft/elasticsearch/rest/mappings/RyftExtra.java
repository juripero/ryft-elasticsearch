package com.ryft.elasticsearch.rest.mappings;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RyftExtra {

    @JsonProperty("aggregations")
    private ObjectNode aggregations;
    private String session;
    private String backend;

    public RyftExtra() {
    }

    public ObjectNode getAggregations() {
        return aggregations;
    }

    public void setAggregations(ObjectNode aggregations) {
        this.aggregations = aggregations;
    }

    public String getSession() {
        return session;
    }

    public void setSession(String session) {
        this.session = session;
    }

    public String getBackend() {
        return backend;
    }

    public void setBackend(String backend) {
        this.backend = backend;
    }
    
}
