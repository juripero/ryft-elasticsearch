package com.ryft.elasticsearch.plugin.service;

import com.ryft.elasticsearch.converter.ElasticConversionCriticalException;
import com.ryft.elasticsearch.converter.entities.AggregationParameters;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class AggregationService {

    private static final ESLogger LOGGER = Loggers.getLogger(AggregationService.class);
    private static final String INDEX_NAME = "aggregationtemp";

    private static Client client;

    public static InternalAggregations applyAggregation(InternalSearchHits internalSearchHits,
                                                        AggregationParameters aggregationParameters)
            throws UnknownHostException, ElasticConversionCriticalException {
        if (internalSearchHits.getTotalHits() == 0) {
            return InternalAggregations.EMPTY;
        }
        prepareTempIndex(internalSearchHits, aggregationParameters);

        // Only date_histogram aggregation is currently supported
        AggregationBuilder aggregation = AggregationBuilders
                .dateHistogram("2")
                .field(aggregationParameters.getField())
                .interval(new DateHistogramInterval(aggregationParameters.getInterval()))
                .timeZone(aggregationParameters.getTimeZone())
                .minDocCount(aggregationParameters.getMinDocCount())
                .extendedBounds(aggregationParameters.getMinBound(), aggregationParameters.getMaxBound());

        SearchResponse searchResponse = client
                .prepareSearch(INDEX_NAME)
                .setTypes("agg")
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(aggregation)
                .get();

        cleanUp();

        return (InternalAggregations) searchResponse.getAggregations();
    }

    private static void prepareTempIndex(InternalSearchHits internalSearchHits, AggregationParameters aggregationParameters)
            throws UnknownHostException, ElasticConversionCriticalException {
        Settings clientSettings = Settings.settingsBuilder()
                .put("client.transport.ignore_cluster_name", true).build();

        client = TransportClient.builder()
                .settings(clientSettings)
                .build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        Settings settings = Settings.builder().put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build();
        client.admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).get();

        client.admin().indices().preparePutMapping(INDEX_NAME).setType("agg").setSource("{\n" +
                "    \"agg\" : {\n" +
                "        \"properties\" : {\n" +
                "            \"" + aggregationParameters.getField() + "\" : {\"type\" : \"date\", \"format\" : \"yyyy-MM-dd HH:mm:ss\"}\n" +
                "        }\n" +
                "    }\n" +
                "}").get();

        BulkRequestBuilder bulkRequest = client.prepareBulk();
        InternalSearchHit[] hits = internalSearchHits.internalHits();

        for (InternalSearchHit hit : hits) {
            bulkRequest.add(client.prepareIndex(INDEX_NAME, "agg", hit.getId()).setSource(hit.getSourceAsString()));
        }

        BulkResponse bulkResponse = bulkRequest.setRefresh(true).get();
        if (bulkResponse.hasFailures()) {
            LOGGER.error(bulkResponse.buildFailureMessage());
            throw new ElasticConversionCriticalException("Cannot apply aggregation");
        } else {
            LOGGER.info("Bulk indexing succeeded.");
        }

        client.admin().cluster().prepareHealth(INDEX_NAME)
                .setWaitForYellowStatus()
                .get();
    }

    private static void cleanUp() {
        client.admin().indices().prepareDelete(INDEX_NAME).get();
        client.close();
    }
}
