package com.ryft.elasticsearch.rest.mappings;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.ryft.elasticsearch.plugin.disruptor.messages.FileSearchRequestEvent;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.internal.InternalSearchHit;

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
            if(record.has("type")) {
                searchHit.shardTarget(index.getSearchShardTarget());
            } else {
                searchHit.shardTarget(null);
            }

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
