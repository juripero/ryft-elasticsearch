package com.ryft.elasticsearch.rest.mappings;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RyftResponse {

    private ArrayList<RyftResult> results = new ArrayList<>();

    private RyftStats stats;

    private String[] errors;

    private String message;

    public RyftResponse() {
    }

    @JsonCreator
    public RyftResponse(@JsonProperty("results") ArrayList<RyftResult> results,
            @JsonProperty("stats") RyftStats stats,
            @JsonProperty("errors") String[] errors,
            @JsonProperty("message") String message) {
        super();
        this.results = results;
        this.stats = stats;
        this.errors = errors;
        this.message = message;
    }

    @JsonProperty("errors")
    public String[] getErrors() {
        return errors;
    }

    public void setErrors(String[] errors) {
        this.errors = errors;
    }

    @JsonProperty("message")
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @JsonProperty("results")
    public ArrayList<RyftResult> getResults() {
        return results;
    }

    public void setResults(ArrayList<RyftResult> results) {
        this.results = results;
    }

    public RyftStats getStats() {
        return stats;
    }

    public void setStats(RyftStats stats) {
        this.stats = stats;
    }

    public Boolean hasErrors() {
        return !((message == null) || (message.isEmpty())) || !((errors == null) || (errors.length == 0));
    }

    public Boolean hasResults() {
        return !((results == null) || (results.isEmpty()));
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
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RyftResponse other = (RyftResponse) obj;
        if (results == null) {
            if (other.results != null) {
                return false;
            }
        } else if (!results.equals(other.results)) {
            return false;
        }
        if (stats == null) {
            if (other.stats != null) {
                return false;
            }
        } else if (!stats.equals(other.stats)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("RyftResponse{ ");
        if ((results != null) && (!results.isEmpty())) {
            sb.append(" results=").append(results.size());
        }
        if (stats != null) {
            sb.append(" stats=").append(stats);
        }
        if ((errors != null) && (errors.length > 0)) {
            sb.append(" errors=").append(errors.length);
        }
        if ((message != null) && (!message.isEmpty())) {
            sb.append(" message=").append(message);
        }
        sb.append(" }");
        return sb.toString();
    }

}
