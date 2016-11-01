package com.dataart.ryft.disruptor.messages;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;

public class RyftRequestEvent extends InternalEvent {

    private ActionListener<SearchResponse> callback;
    private Integer fuzziness;
    private String[] index;
    private RyftQuery query;
    private int limit = 1000;

    public RyftRequestEvent(RyftQuery ryftQuery) {
        super();
        this.query = ryftQuery;
    }

    public String getRyftSearchUrl() {
        StringBuilder sb = new StringBuilder("/search?query=");
        try {
            sb.append(URLEncoder.encode(query.buildRyftString(), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < index.length; i++) {
            sb.append("&file=");
            sb.append("elasticsearch/elasticsearch/nodes/0/indices/");
            sb.append(index[i]);
            sb.append("/0/index/*.");
            sb.append(index[i]).append("jsonfld");
        }
        sb.append("&mode=es&format=json&local=true&stats=true");
        sb.append("&limit=");
        sb.append(limit);
        return sb.toString();
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
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
        result = prime * result + ((fuzziness == null) ? 0 : fuzziness.hashCode());
        result = prime * result + Arrays.hashCode(index);
        result = prime * result + ((query == null) ? 0 : query.hashCode());
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
        RyftRequestEvent other = (RyftRequestEvent) obj;
        if (fuzziness == null) {
            if (other.fuzziness != null) {
                return false;
            }
        } else if (!fuzziness.equals(other.fuzziness)) {
            return false;
        }
        if (!Arrays.equals(index, other.index)) {
            return false;
        }
        if (query == null) {
            if (other.query != null) {
                return false;
            }
        } else if (!query.equals(other.query)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "RyftFuzzyRequest [fuzziness=" + fuzziness + ", index=" + Arrays.toString(index) + ", query=" + query + "]";
    }

    @Override
    public EventType getEventType() {
        return EventType.ES_REQUEST;
    }

}
