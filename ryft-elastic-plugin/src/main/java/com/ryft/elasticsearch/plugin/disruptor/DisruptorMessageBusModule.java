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

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.multibindings.Multibinder;

import com.ryft.elasticsearch.plugin.disruptor.messages.DisruptorEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.RequestEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.ryft.elasticsearch.plugin.disruptor.messages.FileSearchRequestEvent;
import com.ryft.elasticsearch.plugin.disruptor.messages.FileSearchRequestEventFactory;
import com.ryft.elasticsearch.plugin.disruptor.messages.IndexSearchRequestEvent;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import com.ryft.elasticsearch.plugin.disruptor.messages.IndexSearchRequestEventFactory;

public class DisruptorMessageBusModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(new TypeLiteral<EventProducer<RequestEvent>>() {
        }).to(new TypeLiteral<InternalEventProducer<RequestEvent>>() {
        });

        Multibinder<EventHandler<DisruptorEvent<RequestEvent>>> binder = Multibinder.newSetBinder(binder(),
                new TypeLiteral<EventHandler<DisruptorEvent<RequestEvent>>>() {
        });

        binder.addBinding().to(new TypeLiteral<RyftRequestEventConsumer>() {
        }).asEagerSingleton();

        bind(new TypeLiteral<RingBuffer<DisruptorEvent<RequestEvent>>>() {
        }).toProvider(Key.get(new TypeLiteral<RingBufferProvider<RequestEvent>>() {
        })).in(Singleton.class);

        bind(IndexSearchRequestEventFactory.class).toProvider(
                FactoryProvider.newFactory(IndexSearchRequestEventFactory.class, IndexSearchRequestEvent.class)).in(Singleton.class);

        bind(FileSearchRequestEventFactory.class).toProvider(
                FactoryProvider.newFactory(FileSearchRequestEventFactory.class, FileSearchRequestEvent.class)).in(Singleton.class);
    }

}
