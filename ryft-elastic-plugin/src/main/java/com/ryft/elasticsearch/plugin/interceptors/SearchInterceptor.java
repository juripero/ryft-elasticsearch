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
package com.ryft.elasticsearch.plugin.interceptors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.tasks.Task;

import com.ryft.elasticsearch.plugin.disruptor.EventProducer;
import com.ryft.elasticsearch.converter.ElasticConversionCriticalException;
import com.ryft.elasticsearch.converter.ElasticConversionException;
import com.ryft.elasticsearch.converter.ElasticConverter;
import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.RyftProperties;
import com.ryft.elasticsearch.plugin.service.RyftSearchService;
import com.ryft.elasticsearch.plugin.disruptor.messages.RequestEvent;

public class SearchInterceptor implements ActionInterceptor {

    private static final ESLogger LOGGER = Loggers.getLogger(SearchInterceptor.class);
    private final EventProducer<RequestEvent> producer;
    private final ElasticConverter elasticConverter;
    private final RyftProperties properties;
    private final RyftSearchService ryftSearchService;

    @Inject
    public SearchInterceptor(RyftProperties ryftProperties,
            EventProducer<RequestEvent> producer, ElasticConverter elasticConverter,
            RyftSearchService ryftSearchService) {
        this.properties = ryftProperties;
        this.producer = producer;
        this.elasticConverter = elasticConverter;
        this.ryftSearchService = ryftSearchService;
    }

    @Override
    public boolean intercept(Task task, String action, ActionRequest request, ActionListener listener, ActionFilterChain chain) {
        try {
            RyftRequestParameters ryftRequestParameters = elasticConverter.convert(request);
            if (ryftRequestParameters == null) {
                return false;
            } else {
                RequestEvent requestEvent = ryftSearchService.getClusterRequestEvent(ryftRequestParameters);
                Boolean isRyftIntegrationElabled;
                if (requestEvent != null) {
                    isRyftIntegrationElabled = ryftRequestParameters.getRyftProperties().getBool(PropertiesProvider.RYFT_INTEGRATION_ENABLED);
                } else {
                    isRyftIntegrationElabled = properties.getBool(PropertiesProvider.RYFT_INTEGRATION_ENABLED);
                }
                if (isRyftIntegrationElabled && (requestEvent != null)) {
                    requestEvent.setCallback(listener);
                    producer.send(requestEvent);
                    return true;
                }
                return false;
            }
        } catch (ElasticConversionException ex) {
            if (ex != null) {
                LOGGER.error("Convertion exception.", ex);
                return ex instanceof ElasticConversionCriticalException;
            } else {
                return false;
            }
        }
    }

}
