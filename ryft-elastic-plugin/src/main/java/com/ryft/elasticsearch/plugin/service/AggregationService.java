package com.ryft.elasticsearch.plugin.service;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryft.elasticsearch.plugin.RyftProperties;
import com.ryft.elasticsearch.plugin.disruptor.messages.SearchRequestEvent;
import com.ryft.elasticsearch.rest.client.RyftSearchException;
import java.io.IOException;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;

import java.util.concurrent.ExecutionException;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;

public class AggregationService {

    private static final ESLogger LOGGER = Loggers.getLogger(AggregationService.class);
    private static final String TEMPINDEX_PREFIX = ".tmpagg";

    private final Client client;
    private final ObjectMapper mapper = new ObjectMapper();

    @Inject
    public AggregationService(TransportClient client) {
        this.client = client;
    }

    public InternalAggregations applyAggregation(InternalSearchHits internalSearchHits,
            SearchRequestEvent requestEvent) throws RyftSearchException {
        RyftProperties query = new RyftProperties();
        query.putAll(requestEvent.getParsedQuery());
        if ((internalSearchHits.getTotalHits() > 0)
                && (query.containsKey("aggs") || query.containsKey("aggregations"))) {
            String tempIndexName = getTempIndexName(requestEvent);
            try {
                prepareTempIndex(internalSearchHits, tempIndexName);
                query.put("size", 0);
                query.put("ryft_enabled", false);
                SearchResponse searchResponse = client.execute(SearchAction.INSTANCE,
                        new SearchRequest(new String[]{tempIndexName},
                        mapper.writeValueAsBytes(query.toMap()))).get();
                return (InternalAggregations) searchResponse.getAggregations();
            } catch (JsonProcessingException | InterruptedException | ExecutionException ex) {
                throw new RyftSearchException(ex);
            } finally {
                client.admin().indices().prepareDelete(tempIndexName).get();
            }
        } else {
            LOGGER.debug("No aggregation");
            return InternalAggregations.EMPTY;
        }
    }

    private void prepareTempIndex(InternalSearchHits internalSearchHits, String tempIndexName)
            throws RyftSearchException {
        LOGGER.debug("Creating temp index {}.", tempIndexName);

        Settings indexSettings = Settings.builder().put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build();
        client.admin().indices().prepareCreate(tempIndexName).setSettings(indexSettings).get();

        String index = internalSearchHits.getAt(0).getIndex();
        GetMappingsResponse mappingsResponse = client.admin().indices().prepareGetMappings(index).get();

        LOGGER.debug("Creating mappings in temp index.");
        ImmutableOpenMap<String, MappingMetaData> mappings = mappingsResponse
                .getMappings()
                .get(index);
        try {
            for (ObjectObjectCursor<String, MappingMetaData> mappingCursor : mappings) {
                MappingMetaData mappingMetaData = mappingCursor.value;
                String type = mappingCursor.key;
                client.admin().indices().preparePutMapping(tempIndexName)
                        .setType(type).setSource(mappingMetaData.sourceAsMap()).get();
            }
        } catch (IOException ex) {
            String errorMessage = String.format("Cannot get mappings of index %s.", index);
            LOGGER.error(errorMessage, ex);
            throw new RyftSearchException(errorMessage, ex);
        }

        BulkRequestBuilder bulkRequest = client.prepareBulk();
        InternalSearchHit[] hits = internalSearchHits.internalHits();

        for (InternalSearchHit hit : hits) {
            bulkRequest.add(client.prepareIndex(tempIndexName, hit.getType(), hit.getId()).setSource(hit.getSourceAsString()));
        }

        BulkResponse bulkResponse = bulkRequest.setRefresh(true).get();
        if (bulkResponse.hasFailures()) {
            String errorMessage = String.format("Cannot upload data to temp index %s: %s", tempIndexName, bulkResponse.buildFailureMessage());
            LOGGER.error(errorMessage);
            throw new RyftSearchException(errorMessage);
        } else {
            LOGGER.info("Data uploaded to temp index {} succesfully.", tempIndexName);
        }

        client.admin().cluster().prepareHealth(tempIndexName)
                .setWaitForYellowStatus()
                .get();
    }

    private String getTempIndexName(SearchRequestEvent requestEvent) {
        return String.format("%s%d", TEMPINDEX_PREFIX, requestEvent.hashCode());
    }
}
