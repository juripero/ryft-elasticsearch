package com.dataart.ryft.disruptor.messages;

import com.dataart.ryft.elastic.converter.ElasticConversionCriticalException;
import com.dataart.ryft.elastic.converter.ElasticConverterRyft.ElasticConverterFormat.RyftFormat;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.elastic.plugin.PropertiesProvider;
import com.dataart.ryft.elastic.plugin.RyftProperties;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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

    public String getRyftSearchUrl() throws UnsupportedEncodingException, ElasticConversionCriticalException {
        StringBuilder sb = new StringBuilder("http://");
        sb.append(ryftProperties.getStr(PropertiesProvider.HOST)).append(":");
        sb.append(ryftProperties.getStr(PropertiesProvider.PORT));
        sb.append("/search?query=");
        sb.append(buildRyftQuery());
        sb.append(URLEncoder.encode(query.buildRyftString(), "UTF-8"));
        getFilenames().stream().forEach((filename) -> {
            sb.append("&file=");
            sb.append(filename);
        });
        sb.append("&mode=es&local=true&stats=true");
        sb.append("&format=").append(getFormat().name().toLowerCase());
        sb.append("&limit=").append(getLimit());
        return sb.toString();
    }

    private String buildRyftQuery() throws ElasticConversionCriticalException {
        if (!isIndexedSearch()) {
            if ((getFilenames() == null) || (getFilenames().isEmpty()))  {
                throw new ElasticConversionCriticalException("Filenames should be defined for non indexed search.");
            }
        }
        return query.buildRyftString();
    }

    public int getLimit() {
        return ryftProperties.getInt(PropertiesProvider.SEARCH_QUERY_SIZE);
    }

    public RyftFormat getFormat() {
        return (RyftFormat)ryftProperties.get(PropertiesProvider.RYFT_FORMAT);
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

    private List<String> getFilenames() {
        if (!isIndexedSearch()) {
            return (List) ryftProperties.get(PropertiesProvider.RYFT_FILES_TO_SEARCH);
        }
        return Arrays.stream(index)
                .map(indexName -> String.format("elasticsearch/elasticsearch/nodes/0/indices/%1$s/0/index/*.%1$sjsonfld", indexName))
                .collect(Collectors.toList());
    }

    private Boolean isIndexedSearch() {
        return !(ryftProperties.containsKey(PropertiesProvider.RYFT_FILES_TO_SEARCH)
                && (ryftProperties.get(PropertiesProvider.RYFT_FILES_TO_SEARCH) instanceof List));
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
