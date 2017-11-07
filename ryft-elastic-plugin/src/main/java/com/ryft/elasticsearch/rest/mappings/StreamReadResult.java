package com.ryft.elasticsearch.rest.mappings;

import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.internal.InternalSearchHit;

public class StreamReadResult {

    private final List<InternalSearchHit> searchHits = new ArrayList<>();
    private final List<ShardSearchFailure> failures = new ArrayList<>();
    private RyftStats ryftStats;

    public StreamReadResult() {
    }

    public void addHit(InternalSearchHit hit) {
        searchHits.add(hit);
    }

    public void addFailure(ShardSearchFailure failure) {
        failures.add(failure);
    }

    public List<InternalSearchHit> getSearchHits() {
        return searchHits;
    }

    public List<ShardSearchFailure> getFailures() {
        return failures;
    }

    public RyftStats getStats() {
        return ryftStats;
    }

    public void setStats(RyftStats ryftStats) {
        this.ryftStats = ryftStats;
    }

}
