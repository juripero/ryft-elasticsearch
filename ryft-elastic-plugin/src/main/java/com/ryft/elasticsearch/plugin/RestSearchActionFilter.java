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

import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.tasks.Task;

import com.ryft.elasticsearch.plugin.interceptors.ActionInterceptor;

@Singleton
public class RestSearchActionFilter implements ActionFilter {
    // private final ESLogger logger = Loggers.getLogger(getClass());
    private final PropertiesProvider provider;
    private final Map<String, ActionInterceptor> interceptors;
    private final RyftPluginGlobalSettingsProvider globalSettings;
    private boolean rereadProperties = false;

    @Inject
    public RestSearchActionFilter(PropertiesProvider provider, Map<String, ActionInterceptor> interceptors,
            RyftPluginGlobalSettingsProvider globalSettings) {
        this.interceptors = interceptors;
        this.provider = provider;
        this.globalSettings = globalSettings;
    }

    @Override
    public int order() {
        return 0; // We are the first here!
    }

    @Override
    public void apply(Task task, String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {
        ActionInterceptor interceptor = interceptors.get(task.getAction());
        if (request instanceof IndexRequest) {
            // We are updating global settings
            String settingsIndex = provider.get().getStr(PropertiesProvider.PLUGIN_SETTINGS_INDEX);
            IndexRequest req = (IndexRequest) request;
            String index = req.index();
            rereadProperties = settingsIndex.equals(index);
        } else if (interceptor != null && interceptor.intercept(task, action, request, listener, chain)) {
            return; // Our Search interceptor intercepted request
        }
        chain.proceed(task, action, request, listener);
    }

    @Override
    public void apply(String action, ActionResponse response, ActionListener listener, ActionFilterChain chain) {
        chain.proceed(action, response, listener);
        if (rereadProperties && response instanceof IndexResponse) {
            //ES saved new GlobalSettings we should reread them now
            String settingsIndex = provider.get().getStr(PropertiesProvider.PLUGIN_SETTINGS_INDEX);
            if (((IndexResponse) response).getIndex().equals(settingsIndex)) {
                globalSettings.rereadGlobalSettings();
                rereadProperties = false;
            }
        }
    }
}
