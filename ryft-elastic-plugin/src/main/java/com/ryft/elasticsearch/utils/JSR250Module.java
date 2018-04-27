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
package com.ryft.elasticsearch.utils;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.matcher.AbstractMatcher;
import org.elasticsearch.common.inject.matcher.Matcher;
import org.elasticsearch.common.inject.spi.InjectionListener;
import org.elasticsearch.common.inject.spi.TypeEncounter;
import org.elasticsearch.common.inject.spi.TypeListener;


/**
 * Module responsible for binding postConstruct listeners;
 * Bind as first module in the module chain
 * 
 * @author imasternoy
 *
 */
public class JSR250Module extends AbstractModule {

    @Override
    protected void configure() {
        bindListener(POST_CONSTRUCT_MATCHER, new TypeListener() {
            @Override
            public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter) {
                try {
                    @SuppressWarnings("unchecked")
                    TypeEncounter<PostConstruct> g = (TypeEncounter<PostConstruct>) encounter;
                    g.register(InvokePostConstructMethod.INSTANCE);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    /**
     * PostConstruct developed according to
     * http://developer.vz.net/2012/02/08/extending-guice-2/
     */
    private static final Matcher<TypeLiteral<?>> POST_CONSTRUCT_MATCHER = new TypeMatcher(PostConstruct.class);

    private static final class TypeMatcher extends AbstractMatcher<TypeLiteral<?>> {
        private final Class<?> type;

        public TypeMatcher(Class<?> type) {
            this.type = type;
        }

        public boolean matches(TypeLiteral<?> type) {
            try {
                return this.type.isAssignableFrom(type.getRawType());
            } catch (Exception e) {
                return false;
            }
        }
    }

    static enum InvokePostConstructMethod implements InjectionListener<PostConstruct> {
        INSTANCE;

        public void afterInjection(PostConstruct injectee) {
            try {
                PostConstruct g = (PostConstruct) injectee;
                g.onPostConstruct();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

}
