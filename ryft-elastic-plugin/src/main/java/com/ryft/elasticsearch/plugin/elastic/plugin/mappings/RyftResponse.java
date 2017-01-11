package com.ryft.elasticsearch.plugin.elastic.plugin.mappings;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RyftResponse {

    private ArrayList<ObjectNode> results = new ArrayList<ObjectNode>();

    private RyftStats stats;

    private String[] errors;

    private String message;

    public RyftResponse() {
        // TODO Auto-generated constructor stub
    }

    @JsonCreator
    public RyftResponse(@JsonProperty("results") ArrayList<ObjectNode> results,//
                        @JsonProperty("stats") RyftStats stats, //
                        @JsonProperty("errors") String[] errors,
                        @JsonProperty("message") String message) {
        super();
        this.results = results;
        this.stats = stats;
        this.errors = errors;
        this.message = message;
    }

    @JsonProperty("results")
    public ArrayList<ObjectNode> getResults() {
        return results;
    }

    @JsonProperty("errors")
    public String[] getErrors() {
        return errors;
    }

    public void setResults(ArrayList<ObjectNode> results) {
        this.results = results;
    }

    public RyftStats getStats() {
        return stats;
    }

    public void setStats(RyftStats stats) {
        this.stats = stats;
    }

    @JsonProperty("message")
    public String getMessage() {
        return message;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((results == null) ? 0 : results.hashCode());
        result = prime * result + ((stats == null) ? 0 : stats.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RyftResponse other = (RyftResponse) obj;
        if (results == null) {
            if (other.results != null)
                return false;
        } else if (!results.equals(other.results))
            return false;
        if (stats == null) {
            if (other.stats != null)
                return false;
        } else if (!stats.equals(other.stats))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "RyftResponse [results=" + results + ", stats=" + stats + "]";
    }

}
