package com.dataart.ryft.disruptor.messages;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;

public class RyftRequestEvent extends InternalEvent {

    private ActionListener<SearchResponse> callback;
    private Integer fuzziness;
    private String[] index;
    private RyftQuery query;
    private List<String> fields;// Needed for multi matching

    public RyftRequestEvent(RyftQuery ryftQuery) {
        super();
        this.query = ryftQuery;
    }
    //(RECORD.type%20CONTAINS%20%22act%22)&files=elasticsearch/elasticsearch/nodes/0/indices/shakespeare/0/index/_0.shakespearejsonfld&mode=es&format=json&local=true&stats=true
    public String getRyftSearchUrl(){
        StringBuilder sb = new StringBuilder("/search?query=");
        sb.append(java.net.URLEncoder.encode(query.buildRyftString()).replaceAll("\\+", "%20"));
        sb.append("&files=");
        sb.append("elasticsearch/elasticsearch/nodes/0/indices/shakespeare/0/index/_0.");
        sb.append(index[0]).append("jsonfld");
        sb.append("&mode=es&format=json&local=true&stats=true");
        return sb.toString();
    }

    public Integer getFuzziness() {
        return fuzziness;
    }

    public void setFuzziness(Integer fuzziness) {
        this.fuzziness = fuzziness;
    }

    public String[] getIndex() {
        return index;
    }

    public void setIndex(String[] index) {
        this.index = index;
    }

    public RyftQuery getQuery() {
        return query;
    }

    public void setQuery(RyftQuery query) {
        this.query = query;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }
    
    public ActionListener<SearchResponse> getCallback() {
        return callback;
    }
    
    public void setCallback(ActionListener<SearchResponse> callback) {
        this.callback = callback;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fields == null) ? 0 : fields.hashCode());
        result = prime * result + ((fuzziness == null) ? 0 : fuzziness.hashCode());
        result = prime * result + Arrays.hashCode(index);
        result = prime * result + ((query == null) ? 0 : query.hashCode());
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
        RyftRequestEvent other = (RyftRequestEvent) obj;
        if (fields == null) {
            if (other.fields != null)
                return false;
        } else if (!fields.equals(other.fields))
            return false;
        if (fuzziness == null) {
            if (other.fuzziness != null)
                return false;
        } else if (!fuzziness.equals(other.fuzziness))
            return false;
        if (!Arrays.equals(index, other.index))
            return false;
        if (query == null) {
            if (other.query != null)
                return false;
        } else if (!query.equals(other.query))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "RyftFuzzyRequest [fuzziness=" + fuzziness + ", index=" + Arrays.toString(index) + ", query=" + query + ", fields=" + fields + "]";
    }

    @Override
    public EventType getEventType() {
        return EventType.ES_REQUEST;
    }

}