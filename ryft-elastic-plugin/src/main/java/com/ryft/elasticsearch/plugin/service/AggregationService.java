package com.ryft.elasticsearch.plugin.service;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.ryft.elasticsearch.converter.QueryConverterHelper;
import com.ryft.elasticsearch.converter.aggregation.AggregationConverter;
import com.ryft.elasticsearch.plugin.ObjectMapperFactory;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.RyftProperties;
import com.ryft.elasticsearch.plugin.disruptor.messages.FileSearchRequestEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.IndexSearchRequestEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.SearchRequestEvent;
import com.ryft.elasticsearch.rest.client.RyftRestClient;
import com.ryft.elasticsearch.rest.client.RyftSearchException;

import java.io.IOException;
import java.util.*;

import com.ryft.elasticsearch.rest.mappings.RyftResponse;
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

import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.SearchShardTarget;

public class AggregationService {

    private static final ESLogger LOGGER = Loggers.getLogger(AggregationService.class);
    private static final String TEMPINDEX_PREFIX = "tmpagg";

    private final Client client;
    private final ObjectMapper mapper;
    private final RyftRestClient channelProvider;
    private final PropertiesProvider props;

    private final List<String> supportedAggregations;

    @Inject
    public AggregationService(TransportClient client, ObjectMapperFactory objectMapperFactory,
            RyftRestClient channelProvider, PropertiesProvider props) {
        this.client = client;
        this.mapper = objectMapperFactory.get();
        this.channelProvider = channelProvider;
        this.props = props;

        supportedAggregations = Arrays.asList(props.get().getStr(PropertiesProvider.AGGREGATIONS_ON_RYFT_SERVER).split(","));
    }

    public InternalAggregations applyAggregationElastic(List<InternalSearchHit> searchHitList,
            SearchRequestEvent requestEvent) throws RyftSearchException {
        RyftProperties query = new RyftProperties();
        query.putAll(mapper.convertValue(requestEvent.getParsedQuery(), Map.class));
        if (!searchHitList.isEmpty()
                && (query.containsKey("aggs") || query.containsKey("aggregations"))) {
            String tempIndexName = getTempIndexName(requestEvent);
            try {
                prepareTempIndex(requestEvent, searchHitList, tempIndexName);
                query.put(QueryConverterHelper.SIZE_PROPERTY, 0);
                query.remove(QueryConverterHelper.RYFT_PROPERTY);
                query.put(QueryConverterHelper.RYFT_ENABLED_PROPERTY, false);
                query.put("query", ImmutableMap.of("match_all", ImmutableMap.of()));
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

    public InternalAggregations applyAggregationRyft(SearchRequestEvent requestEvent, Map<SearchShardTarget, RyftResponse> resultResponses) throws RyftSearchException {
        RyftResponse ryftResponse = resultResponses.values().stream().findFirst().get();
        ObjectNode ryftAggregationResults = ryftResponse.getStats().getExtra().getAggregations();
        return getFromRyftAggregations(requestEvent, ryftAggregationResults);
    }

    public Map<String, Map> getAggregationsFromEvent(SearchRequestEvent requestEvent) {
        RyftProperties query = new RyftProperties();
        query.putAll(mapper.convertValue(requestEvent.getParsedQuery(), Map.class));

        //TODO - when optional parameters are not set by user, set them explicitly here
        return getAggregationsFromProperties(query);
    }

    public Map<String, Map> getAggregationsFromProperties(RyftProperties query) {
        Map aggs = (Map) query.get("aggs");
        if (aggs == null) {
            aggs = (Map) query.get("aggregations");
        }

        return aggs;
    }

    public InternalAggregations getFromRyftAggregations(SearchRequestEvent requestEvent, ObjectNode ryftAggregations) {
        Map<String, Map> aggregationQuery = getAggregationsFromEvent(requestEvent);
        List<InternalAggregation> internalAggregationList = new ArrayList<>();

        //Aggregation results return without an aggregation type, so we have to map that back to the original request based on the aggregation name
        for (Map.Entry<String, Map> entry : aggregationQuery.entrySet()) {
            Map<String, Map> value = entry.getValue();

            for (Map.Entry<String, Map> innerEntry : value.entrySet()) {
                InternalAggregation internalAggregation = AggregationConverter.convertJsonToAggregation(innerEntry, entry.getKey(), ryftAggregations);

                if (internalAggregation != null) {
                    internalAggregationList.add(internalAggregation);
                }
            }
        }

        return new InternalAggregations(internalAggregationList);
    }

    private void prepareTempIndex(SearchRequestEvent requestEvent, List<InternalSearchHit> searchHitList, String tempIndexName)
            throws RyftSearchException {
        LOGGER.debug("Creating temp index {}.", tempIndexName);

        Settings indexSettings = Settings.builder().put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build();
        client.admin().indices().prepareCreate(tempIndexName).setSettings(indexSettings).get();
        createMapping(requestEvent, tempIndexName);

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        for (InternalSearchHit hit : searchHitList) {
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

    private void createMapping(FileSearchRequestEvent requestEvent, String tempIndexName) {
        PutMappingRequestBuilder putMappingRequestBuilder = client.admin()
                .indices().preparePutMapping(tempIndexName)
                .setType(FileSearchRequestEvent.NON_INDEXED_TYPE);
        RyftProperties mapping = requestEvent.getMapping();
        if (mapping != null) {
            if (mapping.values().stream().allMatch(o -> (o instanceof String))) {
                Object[] mappingObjects = new Object[mapping.size() * 2];
                Integer index = 0;
                for (Map.Entry<Object, Object> entry : mapping.entrySet()) {
                    mappingObjects[index++] = entry.getKey();
                    mappingObjects[index++] = entry.getValue();
                }
                putMappingRequestBuilder.setSource(mappingObjects).get();
            } else {
                if (mapping.containsKey("properties")) {
                    putMappingRequestBuilder.setSource(mapping).get();
                } else {
                    putMappingRequestBuilder.setSource(ImmutableMap.of("properties", mapping)).get();
                }
            }
        }
    }

    private void createMapping(IndexSearchRequestEvent requestEvent, String tempIndexName) throws RyftSearchException {
        if (requestEvent.getAllShards().size() > 0) {
            String index = requestEvent.getAllShards().get(0).getIndex();
            try {
                GetMappingsResponse mappingsResponse = client.admin().indices().prepareGetMappings(index).get();
                LOGGER.debug("Creating mappings in temp index.");
                ImmutableOpenMap<String, MappingMetaData> mappings = mappingsResponse
                        .getMappings()
                        .get(index);
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
        }
    }

    private void createMapping(SearchRequestEvent requestEvent, String tempIndexName) throws RyftSearchException {
        if (requestEvent instanceof FileSearchRequestEvent) {
            createMapping((FileSearchRequestEvent) requestEvent, tempIndexName);
            return;
        }
        if (requestEvent instanceof IndexSearchRequestEvent) {
            createMapping((IndexSearchRequestEvent) requestEvent, tempIndexName);
        }
    }

    private String getTempIndexName(SearchRequestEvent requestEvent) {
        return String.format("%s%d", TEMPINDEX_PREFIX, requestEvent.hashCode());
    }

}
