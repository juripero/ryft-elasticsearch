package com.ryft.elasticsearch.converter.entities;

public class AggregationParameters {

    public enum AggregationType {
        NONE, DATE_HISTOGRAM
    }

    private AggregationType aggregationType;
    private String field;
    private String interval;
    private String timeZone;
    private int minDocCount;
    private long minBound;
    private long maxBound;


    public AggregationParameters(AggregationType aggregationType) {
        this.aggregationType = aggregationType;
    }

    public AggregationType getAggregationType() {
        return aggregationType;
    }

    public void setAggregationType(AggregationType aggregationType) {
        this.aggregationType = aggregationType;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getInterval() {
        return interval;
    }

    public void setInterval(String interval) {
        this.interval = interval;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public int getMinDocCount() {
        return minDocCount;
    }

    public void setMinDocCount(int minDocCount) {
        this.minDocCount = minDocCount;
    }

    public long getMinBound() {
        return minBound;
    }

    public void setMinBound(long minBound) {
        this.minBound = minBound;
    }

    public long getMaxBound() {
        return maxBound;
    }

    public void setMaxBound(long maxBound) {
        this.maxBound = maxBound;
    }
}
