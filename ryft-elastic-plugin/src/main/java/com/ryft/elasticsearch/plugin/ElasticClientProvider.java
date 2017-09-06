package com.ryft.elasticsearch.plugin;

import java.net.InetAddress;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ElasticClientProvider implements Provider<TransportClient> {

    @Override
    public TransportClient get() {
        Settings clientSettings = Settings.settingsBuilder()
                .put("client.transport.ignore_cluster_name", true).build();
        return TransportClient.builder()
                .settings(clientSettings)
                .build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), 9300));
    }

}
