package com.dataart.ryft.integration.test;

import static org.hamcrest.Matchers.greaterThan;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class RyftElasticPluginSmokeTest extends ESSmokeClientTestCase {
    protected static final ESLogger logger = ESLoggerFactory.getLogger(ESSmokeClientTestCase.class.getName());
    
    private Client client;

    @Before
    public void before() {
        client = getClient();
    }

    /**
     * Check that we are connected to a cluster named "elasticsearch".
     */
    // @Ignore
    @Test
    public void testSimpleFuzzyMatch() {

        // START SNIPPET: java-doc-admin-cluster-health
        ClusterHealthResponse health = client.admin().cluster().prepareHealth().setWaitForYellowStatus().get();
        String clusterName = health.getClusterName();
        int numberOfNodes = health.getNumberOfNodes();
        // END SNIPPET: java-doc-admin-cluster-health
        assertThat("cluster [" + clusterName + "] should have at least 1 node", numberOfNodes, greaterThan(0));
        index = "shakespeare";
        MatchQueryBuilder builder = QueryBuilders.matchPhraseQuery("text_entry", "To be or not to be")//
                .fuzziness(Fuzziness.TWO)//
                .operator(Operator.AND);

        logger.info("Testing query: {}", builder.toString());
        SearchResponse searchResponse = client.prepareSearch("shakespeare").setQuery(builder).get();//

        assertNotNull(searchResponse);
        assertEquals(1l, searchResponse.getHits().getTotalHits());

    }
}
