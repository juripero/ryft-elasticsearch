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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.ryft.elasticsearch.plugin.disruptor.messages.FileSearchRequestEvent;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.internal.InternalSearchHit;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RyftResult {

    @JsonProperty("_index")
    private RyftIndex index;
    private final ObjectNode record = JsonNodeFactory.instance.objectNode();

    public RyftResult() {
    }

    @JsonAnyGetter
    public ObjectNode record() {
        return record;
    }

    @JsonAnySetter
    public void setRecordProperty(String name, JsonNode value) {
        record.set(name, value);
    }

    public RyftIndex getIndex() {
        return index;
    }

    public void setIndex(RyftIndex index) {
        this.index = index;
    }

    public InternalSearchHit getInternalSearchHit() {
        InternalSearchHit searchHit;
        try {
            String uid = record.has("_uid") ? record.get("_uid").asText() : String.valueOf(hashCode());
            String type = record.has("type") ? record.get("type").asText() : FileSearchRequestEvent.NON_INDEXED_TYPE;

            searchHit = new InternalSearchHit(0, uid, new Text(type),
                    ImmutableMap.of());
            searchHit.shardTarget(index.getSearchShardTarget());

            String error = record.has("error") ? record.get("error").asText() : "";
            if (!error.isEmpty()) {
                searchHit.sourceRef(new BytesArray("{\"error\": \"" + error + "\"}"));
            } else {
                record.remove("_index");
                record.remove("_uid");
                record.remove("type");
                searchHit.sourceRef(new BytesArray(record.toString()));
            }
            return searchHit;
        } catch (Exception ex) {
            searchHit = new InternalSearchHit(0, "", new Text(""), ImmutableMap.of());
            searchHit.sourceRef(new BytesArray("{\"error\": \"" + ex.toString() + "\"}"));
        }
        return searchHit;
    }
}
