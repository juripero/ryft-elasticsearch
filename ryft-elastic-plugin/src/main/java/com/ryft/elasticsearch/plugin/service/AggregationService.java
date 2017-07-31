package com.ryft.elasticsearch.plugin.service;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.ryft.elasticsearch.converter.ElasticConversionCriticalException;
import com.ryft.elasticsearch.plugin.disruptor.messages.SearchRequestEvent;
import java.io.IOException;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;

import java.net.InetAddress;
import java.util.List;
import org.elasticsearch.common.collect.ImmutableOpenMap;

public class AggregationService {

    private static final ESLogger LOGGER = Loggers.getLogger(AggregationService.class);
    private static final String TEMPINDEX_PREFIX = ".tmpagg";

    public static InternalAggregations applyAggregation(InternalSearchHits internalSearchHits,
            SearchRequestEvent requestEvent) throws ElasticConversionCriticalException {
        List<AggregationBuilder> aggregations = requestEvent.getAggregations();
        if ((internalSearchHits.getTotalHits() == 0)
                || (aggregations == null)
                || (aggregations.isEmpty())) {
            return InternalAggregations.EMPTY;
        } else {
            String tempIndexName = getTempIndexName(requestEvent);
            Client client = getClient();
            try {
                prepareTempIndex(internalSearchHits, tempIndexName, client);
                SearchResponse searchResponse = client
                        .prepareSearch(tempIndexName)
                        .setQuery(QueryBuilders.matchAllQuery())
                        .addAggregation(aggregations.get(0))
                        .get();
                return (InternalAggregations) searchResponse.getAggregations();
            } finally {
                client.admin().indices().prepareDelete(tempIndexName).get();
                client.close();
            }
        }
    }

    private static void prepareTempIndex(InternalSearchHits internalSearchHits, String tempIndexName, Client client)
            throws ElasticConversionCriticalException {
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
            throw new ElasticConversionCriticalException(errorMessage, ex);
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
            throw new ElasticConversionCriticalException(errorMessage);
        } else {
            LOGGER.info("Data uploaded to temp index {} succesfully.", tempIndexName);
        }

        client.admin().cluster().prepareHealth(tempIndexName)
                .setWaitForYellowStatus()
                .get();
    }

    private static Client getClient() {
        Settings clientSettings = Settings.settingsBuilder()
                .put("client.transport.ignore_cluster_name", true).build();
        return TransportClient.builder()
                .settings(clientSettings)
                .build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), 9300));
    }

    private static String getTempIndexName(SearchRequestEvent requestEvent) {
        return String.format("%s%d", TEMPINDEX_PREFIX, requestEvent.hashCode());
    }
}
