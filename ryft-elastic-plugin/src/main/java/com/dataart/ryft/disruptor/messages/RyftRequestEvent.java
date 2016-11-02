package com.dataart.ryft.disruptor.messages;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.elastic.plugin.PropertiesProvider;
import com.dataart.ryft.elastic.plugin.RyftProperties;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;

public class RyftRequestEvent extends InternalEvent {

    private final RyftProperties ryftProperties;
    private ActionListener<SearchResponse> callback;
    private String[] index;
    private RyftQuery query;

    @Inject
    public RyftRequestEvent(RyftProperties ryftProperties, @Assisted RyftQuery ryftQuery) {
        super();
        this.query = ryftQuery;
        this.ryftProperties = new RyftProperties();
        this.ryftProperties.putAll(ryftProperties);
    }

    public String getRyftSearchUrl() {
        StringBuilder sb = new StringBuilder("/search?query=");
        try {
            sb.append(URLEncoder.encode(query.buildRyftString(), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        for (String indexName : index) {
            sb.append("&file=");
            sb.append("elasticsearch/elasticsearch/nodes/0/indices/");
            sb.append(indexName);
            sb.append("/0/index/*.");
            sb.append(indexName).append("jsonfld");
        }
        sb.append("&mode=es&format=json&local=true&stats=true");
        sb.append("&limit=");
        sb.append(getLimit());
        return sb.toString();
    }

    public int getLimit() {
        return ryftProperties.getInt(PropertiesProvider.SEARCH_QUERY_SIZE);
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

    public RyftProperties getRyftProperties() {
        return ryftProperties;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
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
        return "RyftFuzzyRequest [index=" + Arrays.toString(index) + ", query=" + query + "]";
    }

    @Override
    public EventType getEventType() {
        return EventType.ES_REQUEST;
    }

}
