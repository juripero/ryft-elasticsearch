package com.ryft.elasticsearch.plugin;

import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.plugins.PluginsService;

public class ElasticClientProvider implements Provider<TransportClient> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticClientProvider.class);

    private final Settings settings;
    private final PluginsService pluginsService;

    @Inject
    public ElasticClientProvider(Settings settings, PluginsService pluginsService) {
        this.settings = settings;
        this.pluginsService = pluginsService;
    }

    @Override
    public TransportClient get() {
        Boolean isEnryptedTransport = pluginsService.info().getPluginInfos().stream()
                .filter(pluginInfo -> pluginInfo.getName().equals("search-guard-ssl"))
                .findFirst().isPresent();
        Settings.Builder clientSettingsBuilder = Settings.settingsBuilder()
                .put("client.transport.ignore_cluster_name", true);
        TransportClient.Builder clientBuilder = TransportClient.builder();
        if (isEnryptedTransport) {
            clientSettingsBuilder
                    .put(SEARCHGUARD_SSL_TRANSPORT_ENABLED, true)
                    .put(SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH, settings.get(SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH))
                    .put(SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD, settings.get(SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD))
                    .put(SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH, settings.get(SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH))
                    .put(SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD, settings.get(SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD));
            clientBuilder.addPlugin(SearchGuardSSLPlugin.class);
        }
        TransportClient client = clientBuilder
                .settings(clientSettingsBuilder.build())
                .build();
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getLoopbackAddress(), 9300));
        return client;
    }

}
