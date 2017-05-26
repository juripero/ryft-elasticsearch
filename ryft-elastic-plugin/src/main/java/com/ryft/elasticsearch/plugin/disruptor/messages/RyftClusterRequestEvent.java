package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConversionCriticalException;
import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConverterRyft;
import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConverterRyft.ElasticConverterFormat.RyftFormat;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.plugin.elastic.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.elastic.plugin.RyftProperties;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;

/**
 *
 * @author denis
 */
public class RyftClusterRequestEvent extends InternalEvent {

    private final ClusterState clusterState;

    private final RyftProperties ryftProperties;

    private final Settings settings;

    private final String query;

    private final List<ShardRouting> shards;

    private ActionListener<SearchResponse> callback;

    @Override
    public EventType getEventType() {
        return EventType.ES_REQUEST;
    }

    @Inject
    public RyftClusterRequestEvent(ClusterService clusterService,
            Settings settings, @Assisted RyftProperties ryftProperties,
            @Assisted RyftQuery query, @Assisted List<ShardRouting> shards) throws ElasticConversionCriticalException {
        super();
        this.clusterState = clusterService.state();
        this.settings = settings;
        this.ryftProperties = new RyftProperties();
        this.ryftProperties.putAll(ryftProperties);
        try {
            this.query = URLEncoder.encode(query.buildRyftString(), "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new ElasticConversionCriticalException(ex);
        }
        this.shards = shards;
    }
    
    public URI getRyftSearchURL() throws ElasticConversionCriticalException {
       return getRyftSearchURL(null);
    }

    public URI getRyftSearchURL(ShardRouting shardRouting) throws ElasticConversionCriticalException {
        try {
            validateRequest(shardRouting);
            URI result = new URI("http://"
                    + getHost(shardRouting) + ":" + ryftProperties.getStr(PropertiesProvider.PORT)
                    + "/search?query=" + query
                    + "&file=" + getFilenames(shardRouting).stream().collect(Collectors.joining("&file="))
                    + "&local=" + !isNonIndexedSearch()
                    + "&stats=true"
                    + "&cs=" + getCaseSensitive()
                    + "&format=" + getFormat().name().toLowerCase()
                    + "&limit=" + getLimit());
            return result;
        } catch (URISyntaxException ex) {
            throw new ElasticConversionCriticalException("Ryft search URL composition exceptoion", ex);
        }
    }

    private void validateRequest(ShardRouting shardRouting) throws ElasticConversionCriticalException {
        if (ryftProperties.containsKey(PropertiesProvider.RYFT_FORMAT)
                && ryftProperties.get(PropertiesProvider.RYFT_FORMAT).equals(RyftFormat.UNKNOWN_FORMAT)) {
            throw new ElasticConversionCriticalException("Unknown format. Please use one of the following formats: json, xml, utf8, raw");
        }

        if (isNonIndexedSearch()) {
            if ((getFilenames(shardRouting) == null) || (getFilenames(shardRouting).isEmpty())) {
                throw new ElasticConversionCriticalException("File names should be defined for non indexed search.");
            }
        }
    }

    public List<String> getFilenames(ShardRouting shardRouting) {
        if (isNonIndexedSearch()) {
            return (List) ryftProperties.get(PropertiesProvider.RYFT_FILES_TO_SEARCH);
        } else {
            String dataPath = settings.get("path.data");
            StringBuilder result = new StringBuilder();
            if ((dataPath != null) && (!dataPath.isEmpty())) {
                result.append(dataPath.replaceFirst("^\\/ryftone\\/", ""));
            }
            ///{clustername}/nodes/{nodenumber}/indices/{indexname}/{shardid}/index/*.{indexname}jsonfld
            String s = String.format("/%1$s/nodes/0/indices/%2$s/%3$d/index/*.%2$sjsonfld",
                    clusterState.getClusterName().value(),
                    shardRouting.getIndex(),
                    shardRouting.getId());
            return Stream.of(result.append(s).toString()).collect(Collectors.toList());
        }
    }

    private String getHost(ShardRouting shardRouting) {
        if (isNonIndexedSearch()) {
            return clusterState.getNodes().getLocalNode().getHostAddress();
        } else {
            return clusterState.getNodes().get(shardRouting.currentNodeId()).getHostAddress();
        }
    }

    private int getLimit() {
        return ryftProperties.getInt(PropertiesProvider.SEARCH_QUERY_SIZE);
    }

    private ElasticConverterRyft.ElasticConverterFormat.RyftFormat getFormat() {
        return (ElasticConverterRyft.ElasticConverterFormat.RyftFormat) ryftProperties.get(PropertiesProvider.RYFT_FORMAT);
    }

    private boolean getCaseSensitive() {
        return ryftProperties.getBool(PropertiesProvider.RYFT_CASE_SENSITIVE);
    }

    public ActionListener<SearchResponse> getCallback() {
        return callback;
    }

    public void setCallback(ActionListener<SearchResponse> callback) {
        this.callback = callback;
    }

    public List<ShardRouting> getShards() {
        return shards;
    }

    public Boolean isNonIndexedSearch() {
        if (ryftProperties.containsKey(PropertiesProvider.RYFT_FILES_TO_SEARCH)
                && (ryftProperties.get(PropertiesProvider.RYFT_FILES_TO_SEARCH) instanceof List)) {
            return true;
        } else if (ryftProperties.containsKey(PropertiesProvider.RYFT_FORMAT)
                && (ryftProperties.get(PropertiesProvider.RYFT_FORMAT).equals(RyftFormat.RAW)
                || ryftProperties.get(PropertiesProvider.RYFT_FORMAT).equals(RyftFormat.UTF8))) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "RyftClusterRequestEvent{query=" + query + ", shards=" + shards + '}';
    }

}
