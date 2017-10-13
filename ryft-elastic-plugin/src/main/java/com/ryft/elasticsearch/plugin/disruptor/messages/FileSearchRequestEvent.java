package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import static com.ryft.elasticsearch.plugin.disruptor.messages.EventType.FILE_SEARCH_REQUEST;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;

public class FileSearchRequestEvent extends SearchRequestEvent {
    
    public static final String NON_INDEXED_TYPE = "nonindexed";
    
    @Override
    public EventType getEventType() {
        return FILE_SEARCH_REQUEST;
    }

    @Inject
    public FileSearchRequestEvent(ClusterService clusterService,
            @Assisted RyftRequestParameters requestParameters) throws RyftSearchException {
        super(clusterService, requestParameters);
    }


    public URI getRyftSearchURL() throws RyftSearchException {
        int clusterSize = clusterState.getNodes().dataNodes().size();

        String local;
        if (clusterSize > 1) {
            local = "false";
        } else {
            local = "true";
        }

        validateRequest();
        try {
            URI result = new URI("http://"
                    + getHost() + ":" + ryftProperties.getStr(PropertiesProvider.PORT)
                    + "/search?query=" + encodedQuery
                    + "&file=" + getFilenames().stream().collect(Collectors.joining("&file="))
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

    @Override
    protected void validateRequest() throws RyftSearchException {
        super.validateRequest();
        if ((getFilenames() == null) || (getFilenames().isEmpty())) {
            throw new RyftSearchException("File names should be defined for non indexed search.");
        }
    }

    public List<String> getFilenames() {
        if (ryftProperties.containsKey(PropertiesProvider.RYFT_FILES_TO_SEARCH)) {
            return (List) ryftProperties.get(PropertiesProvider.RYFT_FILES_TO_SEARCH);
        } else {
            return Collections.emptyList();
        }
    }

    private String getHost() {
        return clusterState.getNodes().getLocalNode().getHostAddress();
    }

    @Override
    public String toString() {
        return "FileSearchRequestEvent{query=" + query + "files=" + getFilenames() +'}';
    }
    
    
}
