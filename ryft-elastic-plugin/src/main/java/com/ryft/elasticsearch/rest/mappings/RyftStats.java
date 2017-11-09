package com.ryft.elasticsearch.rest.mappings;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RyftStats {

    @JsonProperty("matches")
    private Long matches;
    @JsonProperty("totalBytes")
    private Long totalBytes;
    @JsonProperty("duration")
    private Long duration;
    @JsonProperty("dataRate")
    private Double dataRate;
    @JsonProperty("fabricDuration")
    private Long fabricDuration;
    @JsonProperty("fabricDataRate")
    private Double fabricDataRate;
    @JsonProperty("host")
    private String host;
    @JsonProperty("extra")
    private RyftExtra extra;
    @JsonProperty("details")
    private List<RyftStats> details;

    public RyftStats() {
        // TODO Auto-generated constructor stub
    }

    public Long getMatches() {
        return matches;
    }

    public void setMatches(Long matches) {
        this.matches = matches;
    }

    public Long getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(Long totalBytes) {
        this.totalBytes = totalBytes;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public Double getDataRate() {
        return dataRate;
    }

    public void setDataRate(Double dataRate) {
        this.dataRate = dataRate;
    }

    public Long getFabricDuration() {
        return fabricDuration;
    }

    public void setFabricDuration(Long fabricDuration) {
        this.fabricDuration = fabricDuration;
    }

    public Double getFabricDataRate() {
        return fabricDataRate;
    }

    public void setFabricDataRate(Double fabricDataRate) {
        this.fabricDataRate = fabricDataRate;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public RyftExtra getExtra() {
        return extra;
    }

    public void setExtra(RyftExtra extra) {
        this.extra = extra;
    }

    public List<RyftStats> getDetails() {
        return details;
    }

    public void setDetails(List<RyftStats> details) {
        this.details = details;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RyftStats ryftStats = (RyftStats) o;

        if (matches != null ? !matches.equals(ryftStats.matches) : ryftStats.matches != null) {
            return false;
        }
        if (totalBytes != null ? !totalBytes.equals(ryftStats.totalBytes) : ryftStats.totalBytes != null) {
            return false;
        }
        if (duration != null ? !duration.equals(ryftStats.duration) : ryftStats.duration != null) {
            return false;
        }
        if (dataRate != null ? !dataRate.equals(ryftStats.dataRate) : ryftStats.dataRate != null) {
            return false;
        }
        if (fabricDuration != null ? !fabricDuration.equals(ryftStats.fabricDuration) : ryftStats.fabricDuration != null) {
            return false;
        }
        if (fabricDataRate != null ? !fabricDataRate.equals(ryftStats.fabricDataRate) : ryftStats.fabricDataRate != null) {
            return false;
        }
        if (host != null ? !host.equals(ryftStats.host) : ryftStats.host != null) {
            return false;
        }
        return extra != null ? extra.equals(ryftStats.extra) : ryftStats.extra == null;
    }

    @Override
    public int hashCode() {
        int result = matches != null ? matches.hashCode() : 0;
        result = 31 * result + (totalBytes != null ? totalBytes.hashCode() : 0);
        result = 31 * result + (duration != null ? duration.hashCode() : 0);
        result = 31 * result + (dataRate != null ? dataRate.hashCode() : 0);
        result = 31 * result + (fabricDuration != null ? fabricDuration.hashCode() : 0);
        result = 31 * result + (fabricDataRate != null ? fabricDataRate.hashCode() : 0);
        result = 31 * result + (host != null ? host.hashCode() : 0);
        result = 31 * result + (extra != null ? extra.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "RyftStats{"
                + "matches=" + matches
                + ", totalBytes=" + totalBytes
                + ", duration=" + duration
                + ", dataRate=" + dataRate
                + ", fabricDuration=" + fabricDuration
                + ", fabricDataRate=" + fabricDataRate
                + ", host='" + host + '\''
                + ", extra=" + extra
                + '}';
    }
}
