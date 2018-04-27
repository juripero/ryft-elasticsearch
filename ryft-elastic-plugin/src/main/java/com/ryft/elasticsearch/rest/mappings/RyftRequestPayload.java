/**
 * ##License
 * Ryft-Customized BSD License
 * Copyright (c) 2018, Ryft Systems, Inc.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
 *   This product includes software developed by Ryft Systems, Inc.
 * 4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
 *   to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
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
