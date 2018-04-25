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
package com.ryft.elasticsearch.plugin.disruptor;

import com.ryft.elasticsearch.utils.PostConstruct;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;

import com.ryft.elasticsearch.plugin.disruptor.messages.DisruptorEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.Factory;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.plugin.RyftProperties;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

@Singleton
public class RingBufferProvider<T> implements Provider<RingBuffer<DisruptorEvent<T>>>, PostConstruct {
    Disruptor<DisruptorEvent<T>> disruptor;
    Set<EventHandler<DisruptorEvent<T>>> consumers;
    RyftProperties props;

    @Inject
    public RingBufferProvider(RyftProperties props, Set<EventHandler<DisruptorEvent<T>>> consumers) {
        this.props = props;
        this.consumers = consumers;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void onPostConstruct() {
        Executor executor = Executors.newCachedThreadPool();
        Factory<T> factory = new Factory<>();
        disruptor = AccessController.doPrivileged((PrivilegedAction<Disruptor>) () -> {
            return new Disruptor<>(factory, props.getInt(PropertiesProvider.DISRUPTOR_CAPACITY), executor);
        });
        disruptor.handleEventsWith(consumers.toArray(new EventHandler[consumers.size()]));
        disruptor.start();
    }

    @Override
    public RingBuffer<DisruptorEvent<T>> get() {
        return disruptor.getRingBuffer();
    }

}
