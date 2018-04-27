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
package com.ryft.elasticsearch.converter;

import com.ryft.elasticsearch.converter.ElasticConverterBool.*;
import com.ryft.elasticsearch.converter.ElasticConverterField.*;
import com.ryft.elasticsearch.converter.ElasticConverterShared.*;
import com.ryft.elasticsearch.converter.ElasticConverterRangeField.*;
import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import com.ryft.elasticsearch.converter.entities.RyftRequestParametersFactory;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQueryFactory;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class ElasticConversionModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ContextFactory.class).toProvider(
                FactoryProvider.newFactory(ContextFactory.class, ElasticConvertingContext.class));

        MapBinder<String, ElasticConvertingElement> convertersBinder
                = MapBinder.newMapBinder(binder(), String.class, ElasticConvertingElement.class);
        convertersBinder.addBinding(ElasticConverterQuery.NAME).to(ElasticConverterQuery.class);
        convertersBinder.addBinding(ElasticConverterFuzzy.NAME).to(ElasticConverterFuzzy.class);
        convertersBinder.addBinding(ElasticConverterMatch.NAME).to(ElasticConverterMatch.class);
        convertersBinder.addBinding(ElasticConverterMatchPhrase.NAME).to(ElasticConverterMatchPhrase.class);
        convertersBinder.addBinding(ElasticConverterWildcard.NAME).to(ElasticConverterWildcard.class);
        convertersBinder.addBinding(ElasticConverterRegex.NAME).to(ElasticConverterRegex.class);
        convertersBinder.addBinding(ElasticConverterTerm.NAME).to(ElasticConverterTerm.class);
        convertersBinder.addBinding(ElasticConverterRangeField.NAME).to(ElasticConverterRangeField.class);
        convertersBinder.addBinding(ElasticConverterField.NAME).to(ElasticConverterField.class);
        convertersBinder.addBinding(ElasticConverterRange.NAME).to(ElasticConverterRange.class);
        convertersBinder.addBinding(ElasticConverterBool.NAME).to(ElasticConverterBool.class);
        convertersBinder.addBinding(ElasticConverterMust.NAME).to(ElasticConverterMust.class);
        convertersBinder.addBinding(ElasticConverterMustNot.NAME).to(ElasticConverterMustNot.class);
        convertersBinder.addBinding(ElasticConverterShould.NAME).to(ElasticConverterShould.class);
        convertersBinder.addBinding(ElasticConverterMinimumShouldMatch.NAME).to(ElasticConverterMinimumShouldMatch.class);
        convertersBinder.addBinding(ElasticConverterMinimumShouldMatch.NAME_ALTERNATIVE).to(ElasticConverterMinimumShouldMatch.class);
        convertersBinder.addBinding(ElasticConverterMetric.NAME).to(ElasticConverterMetric.class);
        convertersBinder.addBinding(ElasticConverterFuzziness.NAME).to(ElasticConverterFuzziness.class);
        convertersBinder.addBinding(ElasticConverterOperator.NAME).to(ElasticConverterOperator.class);
        convertersBinder.addBinding(ElasticConverterWidth.NAME).to(ElasticConverterWidth.class);

        convertersBinder.addBinding(ElasticConverterGreaterThanEquals.NAME).to(ElasticConverterGreaterThanEquals.class);
        convertersBinder.addBinding(ElasticConverterGreaterThan.NAME).to(ElasticConverterGreaterThan.class);
        convertersBinder.addBinding(ElasticConverterLessThanEquals.NAME).to(ElasticConverterLessThanEquals.class);
        convertersBinder.addBinding(ElasticConverterLessThan.NAME).to(ElasticConverterLessThan.class);

        convertersBinder.addBinding(ElasticConverterDateFormat.NAME).to(ElasticConverterShared.ElasticConverterDateFormat.class);
        convertersBinder.addBinding(ElasticConverterType.NAME).to(ElasticConverterShared.ElasticConverterType.class);
        convertersBinder.addBinding(ElasticConverterValue.NAME).to(ElasticConverterShared.ElasticConverterValue.class);
        convertersBinder.addBinding(ElasticConverterSeparator.NAME).to(ElasticConverterShared.ElasticConverterSeparator.class);
        convertersBinder.addBinding(ElasticConverterDecimal.NAME).to(ElasticConverterShared.ElasticConverterDecimal.class);
        convertersBinder.addBinding(ElasticConverterCurrency.NAME).to(ElasticConverterShared.ElasticConverterCurrency.class);
        
        convertersBinder.addBinding(ElasticConverterFiltered.NAME).to(ElasticConverterFiltered.class);

        convertersBinder.addBinding(ElasticConverterUnknown.NAME).to(ElasticConverterUnknown.class);

        bind(RyftRequestParametersFactory.class).toProvider(
                FactoryProvider.newFactory(RyftRequestParametersFactory.class, RyftRequestParameters.class)).in(Singleton.class);

        bind(RyftQueryFactory.class).in(Singleton.class);
    }

}
