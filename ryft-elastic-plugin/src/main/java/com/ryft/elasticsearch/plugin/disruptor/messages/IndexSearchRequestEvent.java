package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;

public class IndexSearchRequestEvent extends SearchRequestEvent {

    private final Settings settings;

    private final List<ShardRouting> shards;

    @Override
    public EventType getEventType() {
        return EventType.INDEX_SEARCH_REQUEST;
    }

    @Inject
    public IndexSearchRequestEvent(ClusterService clusterService,
            Settings settings, @Assisted RyftRequestParameters requestParameters,
            @Assisted List<ShardRouting> shards) throws RyftSearchException {
        super(clusterService, requestParameters);
        this.settings = settings;
        this.shards = shards;
    }

    public URI getRyftSearchURL(ShardRouting shardRouting) throws RyftSearchException {
        try {
            validateRequest();
            String local;
            if (aggregationQuery != null) {
                local = "false";
            } else {
                local = "true";
            }

            URI result = new URI("http://"
                    + getHost(shardRouting) + ":" + ryftProperties.getStr(PropertiesProvider.PORT)
                    + "/search?query=" + encodedQuery
                    + "&file=" + getFilenames(shardRouting).stream().collect(Collectors.joining("&file="))
                    + "&local=" + local
                    + "&stats=true"
                    + "&cs=" + getCaseSensitive()
                    + "&format=" + getFormat().name().toLowerCase()
                    + "&limit=" + getLimit());
            return result;
        } catch (URISyntaxException ex) {
            throw new RyftSearchException("Ryft search URL composition exceptoion", ex);
        }
    }

    public List<String> getFilenames(ShardRouting shardRouting) {
        String dataPath = settings.get("path.data");
        StringBuilder result = new StringBuilder();
        if ((dataPath != null) && (!dataPath.isEmpty())) {
            result.append(dataPath.replaceFirst("^\\/ryftone\\/", ""));
        }
        ///{clustername}/nodes/{nodenumber}/indices/{indexname}/{shardid}/index/*.{indexname}jsonfld
        String s = String.format("/%1$s/nodes/0/indices/%2$s/%3$s/index/*.%2$sjsonfld",
                clusterState.getClusterName().value(),
                shardRouting.getIndex(),
                aggregationQuery == null ? String.valueOf(shardRouting.getId()) : "*");
        return Stream.of(result.append(s).toString()).collect(Collectors.toList());
    }

    private String getHost(ShardRouting shardRouting) {
        return clusterState.getNodes().get(shardRouting.currentNodeId()).getHostAddress();
    }

    public List<ShardRouting> getShards() {
        return shards;
    }

    @Override
    public String toString() {
        return "IndexSearchRequestEvent{query=" + query + ", index=" + shards.get(0).getIndex() + ", shards=" + shards.size() + '}';
    }

}
