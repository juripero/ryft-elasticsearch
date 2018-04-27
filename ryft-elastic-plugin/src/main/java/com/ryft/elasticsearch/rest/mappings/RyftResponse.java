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

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.List;

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

    public List<String> getErrorsAndMessage() {
        List<String> result = new ArrayList<>();
        if ((errors != null) && (errors.length > 0)) {
            result.addAll(Arrays.asList(errors));
        }
        if ((message != null) && (!message.isEmpty())) {
            result.add(message);
        }
        return result;
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
