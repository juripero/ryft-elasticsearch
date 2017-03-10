package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConversionCriticalException;
import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConverterRyft;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.plugin.elastic.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.elastic.plugin.RyftProperties;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.List;
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
            RyftProperties ryftProperties, Settings settings,
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

    public URI getRyftSearchURL(ShardRouting shardRouting) throws URISyntaxException {
        String host = clusterState.getNodes().get(shardRouting.currentNodeId()).getHostAddress();
        //String host = ryftProperties.getStr(PropertiesProvider.HOST);
        URI result = new URI("http://"
                + host + ":" + ryftProperties.getStr(PropertiesProvider.PORT)
                + "/search?query=" + query
                + "&file=" + getFilename(shardRouting)
                + "&mode=es&local=true&stats=true"
                + "&cs=" + getCaseSensitive()
                + "&format=" + getFormat().name().toLowerCase()
                + "&limit=" + getLimit());

        return result;
    }

    private String getFilename(ShardRouting shardRouting) {
        String dataPath = settings.get("path.data");
        StringBuilder result = new StringBuilder();
        if ((dataPath != null) && (!dataPath.isEmpty())) {
            result.append(dataPath.replaceFirst("^\\/ryftone\\/", ""));
        }
        //elasticsearch/elasticsearch/nodes/0/indices/shakespeare/0/index/*.shakespearejsonfld
        return result.append(String.format("/%s/nodes/0/indices/%s/%d/index/*.%sjsonfld",
                clusterState.getClusterName().value(),
                shardRouting.getIndex(),
                shardRouting.getId(),
                shardRouting.getIndex())).toString();
    }

    public int getLimit() {
        return ryftProperties.getInt(PropertiesProvider.SEARCH_QUERY_SIZE);
    }

    public ElasticConverterRyft.ElasticConverterFormat.RyftFormat getFormat() {
        return (ElasticConverterRyft.ElasticConverterFormat.RyftFormat) ryftProperties.get(PropertiesProvider.RYFT_FORMAT);
    }

    public boolean getCaseSensitive() {
        return ryftProperties.getBool(PropertiesProvider.RYFT_CASE_SENSITIVE);
    }

    @Override
    public String toString() {
        return "RyftClusterRequestEvent{query=" + query + ", shards=" + shards + '}';
    }

    public RyftProperties getRyftProperties() {
        return ryftProperties;
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

}
