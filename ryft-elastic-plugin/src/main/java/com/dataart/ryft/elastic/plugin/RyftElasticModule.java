package com.dataart.ryft.elastic.plugin;

import org.elasticsearch.common.inject.AbstractModule;

import com.dataart.ryft.elastic.plugin.rest.client.RyftRestClient;

/**
 * 
 * @author imasternoy
 */
public class RyftElasticModule extends AbstractModule {

    @Override
    protected void configure() {
        // TODO: [imasternoy] Think about provider for this
//        bind(RyftRestClient.class);
//        bind(RestSearchActionFilter.class);

    }

}
