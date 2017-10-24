/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.ryft.elasticsearch.integration.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.ryft.elasticsearch.integration.test.entity.TestData;
import com.ryft.elasticsearch.integration.test.util.TestDataGenerator;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertNotNull;

/**
 * {@link RyftElasticTestCase} is an abstract base class to run integration
 * tests against an external Elasticsearch Cluster.
 * <p>
 * You can define a list of transport addresses from where you can reach your
 * cluster by setting "tests.cluster" system property. It defaults to
 * "localhost:9300".
 * <p>
 * All tests can be run from maven using mvn install as maven will start an
 * external cluster first.
 * <p>
 * If you want to debug this module from your IDE, then start an external
 * cluster by yourself then run JUnit. If you changed the default port, set
 * "tests.cluster=localhost:PORT" when running your test.
 */
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public abstract class RyftElasticTestCase extends LuceneTestCase {

    /**
     * Key used to eventually switch to using an external cluster and provide
     * its transport addresses
     */
    public static final String TESTS_CLUSTER_PROPERTY = "test.cluster";

    /**
     * Defaults to localhost:9300
     */
    public static final String TESTS_CLUSTER_DEFAULT = "localhost:9300";

    public static final String INDEX_NAME_PARAM = "test.index";
    protected static String indexName;

    public static final String RECORDS_NUM_INDEX_PARAM = "test.records";
    protected static Integer recordsNum;

    public static final String DELETE_INDEX_PARAM = "test.delete-index";
    protected static Boolean deleteIndex;

    public static final String ENABLE_SSL_PARAM = "test.ssl-enabled";
    protected static Boolean enableSsl;

    public static final String TRUSTSTORE_FILEPATH_PARAM = "test.truststore-filepath";
    public static final String TRUSTSTORE_FILEPATH_DEFAULT = "/etc/elasticsearch/truststore.jks";
    protected static String truststoreFilepath;

    public static final String TRUSTSTORE_PASSWORD_PARAM = "test.truststore-password";
    public static final String TRUSTSTORE_PASSWORD_DEFAULT = "password";
    protected static String truststorePassword;

    public static final String KEYSTORE_FILEPATH_PARAM = "test.keystore-filepath";
    public static final String KEYSTORE_FILEPATH_DEFAULT = "/etc/elasticsearch/admin-keystore.jks";
    protected static String keystoreFilepath;

    public static final String KEYSTORE_PASSWORD_PARAM = "test.keystore-password";
    public static final String KEYSTORE_PASSWORD_DEFAULT = "admin123";
    protected static String keystorePassword;

    protected static final ESLogger LOGGER = ESLoggerFactory.getLogger(RyftElasticTestCase.class.getName());

    private static final AtomicInteger COUNTER = new AtomicInteger();
    private static Client client;
    private static String clusterAddresses;
    protected static TransportAddress[] transportAddresses;

    protected static List<String> testDataStringsList;
    protected static List<TestData> testDataList;

    private static Client startClient(Path tempDir) {
        Settings.Builder settingsBuilder = Settings.settingsBuilder()
                .put("name", "qa_smoke_client_" + COUNTER.getAndIncrement())
                .put(InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING, true) // prevents any settings to be replaced by system properties.
                .put("client.transport.ignore_cluster_name", true)
                .put("path.home", tempDir)
                .put("node.mode", "network");
        TransportClient.Builder transportClientBuilder = TransportClient.builder();

        if (enableSsl) {
            LOGGER.debug(enableSsl + " " + truststoreFilepath + " " + truststorePassword + " " + keystoreFilepath + " " + keystorePassword);
            settingsBuilder
                    .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED, enableSsl)
                    .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH, truststoreFilepath)
                    .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD, truststorePassword)
                    .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH, keystoreFilepath)
                    .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD, keystorePassword);
            transportClientBuilder.addPlugin(SearchGuardSSLPlugin.class);
        }

        Settings clientSettings = settingsBuilder.build();

        client = transportClientBuilder.settings(clientSettings)
                .build().addTransportAddresses(transportAddresses);

        LOGGER.info("--> Elasticsearch Java TransportClient started");

        Exception clientException = null;
        try {
            ClusterHealthResponse health = client.admin().cluster().prepareHealth().get();
            LOGGER.info("--> connected to [{}] cluster which is running [{}] node(s).",
                    health.getClusterName(), health.getNumberOfNodes());
        } catch (Exception e) {
            clientException = e;
        }

        assumeNoException("Sounds like your cluster is not running at " + clusterAddresses, clientException);

        return client;
    }

    protected static Client getClient() {
        if (client == null) {
            startClient(createTempDir());
            assertThat(client, notNullValue());
        }
        return client;
    }

    protected static <T> void createIndex(String index, String type, List<String> objects, String... mapping) {
        boolean exists = getClient().admin().indices().prepareExists(index).execute().actionGet().isExists();
        if (exists) {
            deleteIndex(index);
        }
        LOGGER.info("Creating index {}", index);
        Settings settings = Settings.settingsBuilder()
                .put("number_of_replicas", "1")
               // .put("number_of_shards", "3")
                .build();
        getClient().admin().indices().prepareCreate(index).setSettings(settings).get();

        getClient().admin().indices().preparePutMapping(index).setType(type)
                .setSource((Object[]) mapping).get();

        BulkRequestBuilder bulkRequest = getClient().prepareBulk();
        int id = 0;
        for (String data : objects) {
            bulkRequest.add(getClient().prepareIndex(index, type, String.valueOf(id++))
                    .setSource(data));
        }
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            LOGGER.error(bulkResponse.buildFailureMessage());
        } else {
            LOGGER.info("Bulk indexing succeeded.");
        }
        getClient().admin().indices().prepareRefresh(index).get();
    }

    @BeforeClass
    static void initializeSettings() throws UnknownHostException, JsonProcessingException {
        Properties properties = System.getProperties();
        deleteIndex = Boolean.parseBoolean(properties.getOrDefault(DELETE_INDEX_PARAM, true).toString());
        recordsNum = Integer.valueOf(properties.getOrDefault(RECORDS_NUM_INDEX_PARAM, 100).toString());
        indexName = properties.getProperty(INDEX_NAME_PARAM, "integration-test");
        clusterAddresses = properties.getProperty(TESTS_CLUSTER_PROPERTY, TESTS_CLUSTER_DEFAULT);

        enableSsl = Boolean.parseBoolean(properties.getOrDefault(ENABLE_SSL_PARAM, false).toString());
        truststoreFilepath = properties.getProperty(TRUSTSTORE_FILEPATH_PARAM, TRUSTSTORE_FILEPATH_DEFAULT);
        truststorePassword = properties.getProperty(TRUSTSTORE_PASSWORD_PARAM, TRUSTSTORE_PASSWORD_DEFAULT);
        keystoreFilepath = properties.getProperty(KEYSTORE_FILEPATH_PARAM, KEYSTORE_FILEPATH_DEFAULT);
        keystorePassword = properties.getProperty(KEYSTORE_PASSWORD_PARAM, KEYSTORE_PASSWORD_DEFAULT);

        getTransportAddresses();
        LOGGER.info("Cluster addresses: {}\nIndex name: {}\nRecords: {}\nDelete test index: {}",
                clusterAddresses, indexName, recordsNum, deleteIndex);
        prepareData();
    }

    static private void prepareData() throws JsonProcessingException {
        TestDataGenerator dataGenerator = new TestDataGenerator(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        testDataList = IntStream.range(0, recordsNum).mapToObj(dataGenerator::getDataSample)
                .collect(Collectors.toList());
        testDataStringsList = new ArrayList<>();
        for (TestData data : testDataList) {
            testDataStringsList.add(data.toJson());
        }
    }

    private static void getTransportAddresses() throws UnknownHostException {
        String[] stringAddresses = clusterAddresses.split(",");
        transportAddresses = new TransportAddress[stringAddresses.length];
        int i = 0;
        for (String stringAddress : stringAddresses) {
            String[] split = stringAddress.split(":");
            if (split.length < 2) {
                throw new IllegalArgumentException("address [" + clusterAddresses + "] not valid");
            }
            try {
                transportAddresses[i++] = new InetSocketTransportAddress(InetAddress.getByName(split[0]), Integer.valueOf(split[1]));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("port is not valid, expected number but was [" + split[1] + "]");
            }
        }
    }

    @AfterClass
    public static void stopTransportClient() {
        testDataStringsList = null;
        testDataList = null;
        if (client != null) {
            client.close();
            client = null;
        }
    }

    protected static void deleteIndex(String index) {
        client.admin().indices().prepareDelete(index).get();
    }

    protected static void cleanUp(String index) {
        if ((client != null) && (deleteIndex)) {
            try {
                deleteIndex(index);
            } catch (Exception e) {
                // We ignore this cleanup exception
            }
        }
    }

    protected void assertResponse(SearchResponse searchResponse) {
        assertNotNull(searchResponse);
        assertNotNull(searchResponse.getHits());
        assertNotNull(searchResponse.getHits().getHits());
    }
}
