package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import com.ryft.elasticsearch.plugin.ObjectMapperFactory;
import static com.ryft.elasticsearch.plugin.disruptor.messages.EventType.FILE_SEARCH_REQUEST;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.rest.mappings.RyftRequestPayload;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class FileSearchRequestEvent extends SearchRequestEvent {

    private static final ESLogger LOGGER = Loggers.getLogger(FileSearchRequestEvent.class);

    public static final String NON_INDEXED_TYPE = "nonindexed";

    @Override
    public EventType getEventType() {
        return FILE_SEARCH_REQUEST;
    }

    @Inject
    public FileSearchRequestEvent(ClusterService clusterService,
            ObjectMapperFactory objectMapperFactory,
            @Assisted RyftRequestParameters requestParameters) throws RyftSearchException {
        super(clusterService, objectMapperFactory, requestParameters);
    }

    @Override
    public RyftRequestPayload getRyftRequestPayload() throws RyftSearchException {
        validateRequest();
        RyftRequestPayload payload = new RyftRequestPayload();
        if (canBeAggregatedByRYFT()) {
            LOGGER.info("Ryft Server selected as aggregation backend");
            payload.setAggs(getAggregations());
        }
        return payload;
    }

    @Override
    public URI getRyftSearchURL() throws RyftSearchException {
        validateRequest();
        try {
            if (!nodesToSearch.isEmpty()) {
                URI result = new URI("http://"
                        + nodesToSearch.get(0) + ":" + getPort()
                        + "/search?query=" + getEncodedQuery()
                        + "&file=" + getFilenames().stream().collect(Collectors.joining("&file="))
                        + "&local=" + (clusterState.getNodes().dataNodes().size() == 1)
                        + "&stats=true&ignore-missing-files=true"
                        + "&cs=" + getCaseSensitive()
                        + "&format=" + getFormat().name().toLowerCase()
                        + "&limit=" + getLimit());
                return result;
            } else {
                throw new RyftSearchException("No RYFT nodes to search left");
            }
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

    private List<String> getFilenames() {
        if (requestParameters.getRyftProperties().containsKey(PropertiesProvider.RYFT_FILES_TO_SEARCH)) {
            return (List) requestParameters.getRyftProperties().get(PropertiesProvider.RYFT_FILES_TO_SEARCH);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public String toString() {
        return "FileSearchRequestEvent{query=" + requestParameters.getQuery() + "files=" + getFilenames() + '}';
    }

}
