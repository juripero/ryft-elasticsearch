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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.ryft.elasticsearch.utils.PostConstruct;

@Singleton
public class RyftPluginGlobalSettingsProvider implements PostConstruct {

    private final static ESLogger LOGGER = Loggers.getLogger(RyftPluginGlobalSettingsProvider.class);
    // Enable/disable ryft-elastic-plugin integration globally

    Client client;
    PropertiesProvider provider;
    ClusterService clusterService;
    Optional<Map<String, Object>> globalSettingsOptional = Optional.empty();
    String settingsIndex;

    @Inject
    public RyftPluginGlobalSettingsProvider(ClusterService clusterService, PropertiesProvider provider, Client client) {
        this.clusterService = clusterService;
        this.provider = provider;
        this.client = client;
        clusterService.add(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                RoutingTable routingTable = event.state().routingTable();
                IndexRoutingTable indexRouting = routingTable.index(settingsIndex);
                if (indexRouting == null) {
                    return;
                }
                List<ShardRouting> startedShards = indexRouting.shardsWithState(ShardRoutingState.STARTED);
                if (startedShards.isEmpty()) {
                    return;
                }
                HealthListener healthListener = new HealthListener();
                // Just to be sure that we didn't miss 'Started' status (Yellow
                // OR Green)
                client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute(healthListener);
                // client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute(healthListener);
            }
        });
    }

    @Override
    public void onPostConstruct() {
        settingsIndex = provider.get().getStr(PropertiesProvider.PLUGIN_SETTINGS_INDEX);
    }

    public void rereadGlobalSettings() {
        client.admin()
                .cluster()
                //
                .execute(GetAction.INSTANCE, new GetRequest(settingsIndex, "_all", "1"),
                        new ActionListener<GetResponse>() {

                    @Override
                    public void onFailure(Throwable e) {
                        LOGGER.error("Failed to get the response", e);
                    }

                    @Override
                    public void onResponse(GetResponse response) {
                        globalSettingsOptional = Optional.ofNullable(response.getSource());
                        if (globalSettingsOptional.isPresent()) {
                            provider.get().putAll(globalSettingsOptional.get());
                        }
                        LOGGER.info("Received global settings: {}", globalSettingsOptional);
                    }
                });
    }

    public Optional<String> getString(String key) {
        return globalSettingsOptional.map(globalSettings
                -> globalSettings.get(key).toString());
    }

    public Optional<Boolean> getBool(String key) {
        return globalSettingsOptional.map(globalSettings
                -> Boolean.parseBoolean(globalSettings.get(key).toString()));
    }

    public Optional<Integer> getInt(String key) {
        return globalSettingsOptional.map(globalSettings
                -> Integer.parseInt(globalSettings.get(key).toString()));
    }

    class HealthListener implements ActionListener<ClusterHealthResponse> {

        @Override
        public void onResponse(ClusterHealthResponse response) {
            rereadGlobalSettings();
        }

        @Override
        public void onFailure(Throwable e) {
            LOGGER.error("Failed to get green health response", e);
        }
    }
}
