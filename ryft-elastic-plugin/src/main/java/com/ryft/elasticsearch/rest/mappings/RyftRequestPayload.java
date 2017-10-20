package com.ryft.elasticsearch.rest.mappings;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RyftRequestPayload {

    public static class Tweaks {

        public static class ClusterRoute {

            private String location;
            private List<String> files;

            public ClusterRoute() {
            }

            public String getLocation() {
                return location;
            }

            public void setLocation(String location) {
                this.location = location;
            }

            public List<String> getFiles() {
                return files;
            }

            public void setFiles(List<String> files) {
                this.files = files;
            }

            @Override
            public String toString() {
                return "ClusterRoute{" + "location=" + location + ", files=" + files + '}';
            }

        }
        @JsonProperty("cluster")
        private List<ClusterRoute> clusterRoutes;

        public Tweaks() {
        }

        public List<ClusterRoute> getClusterRoutes() {
            return clusterRoutes;
        }

        public void setClusterRoutes(List<ClusterRoute> clusterRoutes) {
            this.clusterRoutes = clusterRoutes;
        }
    }

    private Tweaks tweaks;

    private JsonNode aggs;

    public RyftRequestPayload() {
    }

    public Tweaks getTweaks() {
        return tweaks;
    }

    public void setTweaks(Tweaks tweaks) {
        this.tweaks = tweaks;
    }

    public JsonNode getAggs() {
        return aggs;
    }

    public void setAggs(JsonNode aggs) {
        this.aggs = aggs;
    }

}
