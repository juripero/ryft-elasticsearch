/**
 * ##License
 * Ryft-Customized BSD License
 * Copyright (c) 2018, Ryft Systems, Inc.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
 *   This product includes software developed by Ryft Systems, Inc.
 * 4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
 *   to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
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
            @Assisted RyftRequestParameters requestParameters) {
        super(clusterService, objectMapperFactory, requestParameters);
    }

    @Override
    public RyftRequestPayload getRyftRequestPayload() throws RyftSearchException {
        validateRequest();
        RyftRequestPayload payload = new RyftRequestPayload();
        if (canBeAggregatedByRyft()) {
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
                        + getHost() + ":" + getPort()
                        + "/search?query=" + getEncodedQuery()
                        + "&file=" + getFilenames().stream().collect(Collectors.joining("&file="))
                        + "&local=" + (clusterService.state().getNodes().dataNodes().size() == 1)
                        + "&stats=true&ignore-missing-files=true"
                        + "&cs=" + getCaseSensitive()
                        + "&format=" + getFormat().name().toLowerCase()
                        + "&stream=true&limit=" + getSize());
                return result;
            } else {
                throw new RyftSearchException("No RYFT nodes to search left");
            }
        } catch (URISyntaxException ex) {
            throw new RyftSearchException("Ryft search URL composition exceptoion", ex);
        }
    }

    @Override
    public void validateRequest() throws RyftSearchException {
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