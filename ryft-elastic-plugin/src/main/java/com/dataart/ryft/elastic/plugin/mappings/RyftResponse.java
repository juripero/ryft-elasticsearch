package com.dataart.ryft.elastic.plugin.mappings;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RyftResponse {

    private ArrayList<RyftHit> results = new ArrayList<RyftHit>();

    private RyftStats stats;

    public RyftResponse() {
        // TODO Auto-generated constructor stub
    }
    
    @JsonCreator
    public RyftResponse(@JsonProperty("results") ArrayList<RyftHit> results, @JsonProperty("stats") RyftStats stats) {
        super();
        this.results = results;
        this.stats = stats;
    }
    
    @JsonProperty("results")
    public ArrayList<RyftHit> getResults() {
        return results;
    }

    public void setResults(ArrayList<RyftHit> results) {
        this.results = results;
    }

    public RyftStats getStats() {
        return stats;
    }

    public void setStats(RyftStats stats) {
        this.stats = stats;
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
