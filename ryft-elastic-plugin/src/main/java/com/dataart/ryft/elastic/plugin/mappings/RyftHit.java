package com.dataart.ryft.elastic.plugin.mappings;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
@JsonIgnoreProperties(ignoreUnknown = true)
public class RyftHit {
    @JsonProperty("_index")
    private RyftIndex index;
    @JsonProperty("_uid")
    private String uid;
    @JsonProperty("doc")
    private ObjectNode doc;
    @JsonProperty("type")
    private String type;
    @JsonProperty("_error")
    private String error;

    public RyftHit() {
        // TODO Auto-generated constructor stub
    }

    public RyftHit(RyftIndex index, String uid, ObjectNode doc, String type, String error) {
        super();
        this.index = index;
        this.uid = uid;
        this.doc = doc;
        this.type = type;
        this.error = error;
    }

    public RyftIndex getIndex() {
        return index;
    }

    public void setIndex(RyftIndex index) {
        this.index = index;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public ObjectNode getDoc() {
        return doc;
    }

    public void setDoc(ObjectNode doc) {
        this.doc = doc;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((doc == null) ? 0 : doc.hashCode());
        result = prime * result + ((index == null) ? 0 : index.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + ((uid == null) ? 0 : uid.hashCode());
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
        RyftHit other = (RyftHit) obj;
        if (doc == null) {
            if (other.doc != null)
                return false;
        } else if (!doc.equals(other.doc))
            return false;
        if (index == null) {
            if (other.index != null)
                return false;
        } else if (!index.equals(other.index))
            return false;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        if (uid == null) {
            if (other.uid != null)
                return false;
        } else if (!uid.equals(other.uid))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "RyftHit [index=" + index + ", uid=" + uid + ", doc=" + doc + ", type=" + type + ", error=" + error
                + "]";
    }

}
