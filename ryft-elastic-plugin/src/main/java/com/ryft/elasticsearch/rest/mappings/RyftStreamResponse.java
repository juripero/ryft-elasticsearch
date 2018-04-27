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
import java.util.List;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.internal.InternalSearchHit;

public class RyftStreamResponse {

    private final List<InternalSearchHit> searchHits = new ArrayList<>();
    private final List<ShardSearchFailure> failures = new ArrayList<>();
    private RyftStats ryftStats;

    public RyftStreamResponse() {
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

    @Override
    public String toString() {
        return "RyftStreamResponse{" + "searchHits=" + searchHits.size() + ", failures=" + failures + ", ryftStats=" + ryftStats + '}';
    }

}
