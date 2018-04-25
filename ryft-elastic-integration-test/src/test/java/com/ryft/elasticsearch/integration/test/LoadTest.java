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
package com.ryft.elasticsearch.integration.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import static com.ryft.elasticsearch.integration.test.RyftElasticTestCase.deleteIndex;
import com.ryft.elasticsearch.integration.test.client.handler.FilesApi;
import com.ryft.elasticsearch.integration.test.client.handler.SearchApi;
import com.ryft.elasticsearch.integration.test.client.invoker.ApiClient;
import com.ryft.elasticsearch.integration.test.client.invoker.ApiException;
import com.ryft.elasticsearch.integration.test.entity.TestData;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class LoadTest extends RyftElasticTestCase {

    private final static Integer CHUNK_NUMBER = 10;
    private final static Integer CHUNK_DATA_NUMBER = 100000;
    private final static String FILENAME = "loadtest.json";

    private static ApiClient apiClient;
    private static FilesApi filesApi;
    private static SearchApi searchApi;

    @BeforeClass
    static public void prepareData() throws IOException {
        filesApi = getFilesApi();
        searchApi = getSearchApi();
        LOGGER.info("Start data uploading into {}", FILENAME);
        for (int i = 0; i < CHUNK_NUMBER; i++) {
            prepareData(CHUNK_DATA_NUMBER).stream()
                    .collect(Collectors.groupingBy(data -> data.getIndex() / 10000)).values()
                    .stream().forEach(dataList -> {
                        try {
                            List<String> strings = new ArrayList<>();
                            for (TestData data : dataList) {
                                strings.add(data.toJson());
                            }
                            byte[] bytes = strings.stream().collect(Collectors.joining("\n", "", "\n")).getBytes();
                            filesApi.postRawFile(bytes, FILENAME, null, null, null, Long.valueOf(bytes.length), null, "wait-10s", true);
                        } catch (JsonProcessingException | ApiException ex) {
                        }
                    });
            LOGGER.info("Uploaded {} records into {}", (i + 1) * CHUNK_DATA_NUMBER, FILENAME);
        }
    }

    @AfterClass
    static void cleanUp() throws ApiException {
        if (deleteIndex) {
            filesApi.deleteFiles(null, Lists.newArrayList(FILENAME), null, true);
        }
    }

    static ApiClient getApiClient() {
        if (apiClient == null) {
            apiClient = new ApiClient();
            apiClient.setApiKeyPrefix("Bearer");
            apiClient.setBasePath(String.format("http://%s:%d", transportAddresses[0].getAddress(), 8765));
        }
        return apiClient;
    }

    static FilesApi getFilesApi() {
        if (filesApi == null) {
            filesApi = new FilesApi(getApiClient());
        }
        return filesApi;
    }

    static SearchApi getSearchApi() {
        if (searchApi == null) {
            searchApi = new SearchApi(getApiClient());
        }
        return searchApi;
    }

    @Test
    public void test1() {
        try {
            String ryftQuery = "{\n"
                    + "  \"query\": {\n"
                    + "    \"match\": {\n"
                    + "      \"eyeColor\": {\n"
                    + "        \"query\": \"green\"\n"
                    + "      }\n"
                    + "    }\n"
                    + "  },\n"
                    + "  \"aggs\": {\n"
                    + "    \"1\": {\n"
                    + "      \"sum\": {\n"
                    + "        \"field\": \"balance\"\n"
                    + "      }\n"
                    + "    }\n"
                    + "  },\n"
                    + "  \"ryft\": {\n"
                    + "    \"enabled\": true,\n"
                    + "    \"files\": [\"" + FILENAME + "\"],\n"
                    + "    \"format\": \"json\"\n"
                    + "  }\n"
                    + "}\n";
            Long elasticStartTime = System.currentTimeMillis();
            SearchResponse ryftResponse = getClient().execute(SearchAction.INSTANCE,
                    new SearchRequest(new String[]{FILENAME}, ryftQuery.getBytes())).get();
            Long elasticFinishTime = System.currentTimeMillis();
            assertNotNull(ryftResponse);
            LOGGER.info("Elasticsearch execution {}", elasticFinishTime - elasticStartTime);
            assertNotNull(ryftResponse.getHits());
            LOGGER.info("Elasticsearch total hits {}", ryftResponse.getHits().getTotalHits());
            LOGGER.info("Elasticsearch returned hits {}", ryftResponse.getHits().hits().length);
            Sum elasticAggregation = (Sum) ryftResponse.getAggregations().get("1");
            assertNotNull(elasticAggregation);
            LOGGER.info("Elasticsearch aggregation {}", elasticAggregation.getValue());
        } catch (InterruptedException | ExecutionException | RuntimeException e) {
            LOGGER.error("Test error", e);
        }
    }
}
