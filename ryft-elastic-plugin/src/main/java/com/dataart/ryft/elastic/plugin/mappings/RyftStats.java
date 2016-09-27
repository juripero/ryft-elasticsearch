package com.dataart.ryft.elastic.plugin.mappings;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

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
    
    public RyftStats() {
        // TODO Auto-generated constructor stub
    }

    public RyftStats(Long matches, Long totalBytes, Long duration, Double dataRate, Long fabricDuration,
            Double fabricDataRate, String host) {
        super();
        this.matches = matches;
        this.totalBytes = totalBytes;
        this.duration = duration;
        this.dataRate = dataRate;
        this.fabricDuration = fabricDuration;
        this.fabricDataRate = fabricDataRate;
        this.host = host;
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dataRate == null) ? 0 : dataRate.hashCode());
        result = prime * result + ((duration == null) ? 0 : duration.hashCode());
        result = prime * result + ((fabricDataRate == null) ? 0 : fabricDataRate.hashCode());
        result = prime * result + ((fabricDuration == null) ? 0 : fabricDuration.hashCode());
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + ((matches == null) ? 0 : matches.hashCode());
        result = prime * result + ((totalBytes == null) ? 0 : totalBytes.hashCode());
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
        RyftStats other = (RyftStats) obj;
        if (dataRate == null) {
            if (other.dataRate != null)
                return false;
        } else if (!dataRate.equals(other.dataRate))
            return false;
        if (duration == null) {
            if (other.duration != null)
                return false;
        } else if (!duration.equals(other.duration))
            return false;
        if (fabricDataRate == null) {
            if (other.fabricDataRate != null)
                return false;
        } else if (!fabricDataRate.equals(other.fabricDataRate))
            return false;
        if (fabricDuration == null) {
            if (other.fabricDuration != null)
                return false;
        } else if (!fabricDuration.equals(other.fabricDuration))
            return false;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        if (matches == null) {
            if (other.matches != null)
                return false;
        } else if (!matches.equals(other.matches))
            return false;
        if (totalBytes == null) {
            if (other.totalBytes != null)
                return false;
        } else if (!totalBytes.equals(other.totalBytes))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "RyftStats [matches=" + matches + ", totalBytes=" + totalBytes + ", duration=" + duration
                + ", dataRate=" + dataRate + ", fabricDuration=" + fabricDuration + ", fabricDataRate="
                + fabricDataRate + ", host=" + host + "]";
    }

}
