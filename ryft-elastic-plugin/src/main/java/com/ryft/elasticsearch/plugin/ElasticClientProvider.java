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
