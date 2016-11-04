package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ElasticConverterBool.*;
import com.dataart.ryft.elastic.converter.ElasticConverterField.*;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryFactory;
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
        convertersBinder.addBinding(ElasticConverterField.NAME).to(ElasticConverterField.class);
        convertersBinder.addBinding(ElasticConverterBool.NAME).to(ElasticConverterBool.class);
        convertersBinder.addBinding(ElasticConverterMust.NAME).to(ElasticConverterMust.class);
        convertersBinder.addBinding(ElasticConverterMustNot.NAME).to(ElasticConverterMustNot.class);
        convertersBinder.addBinding(ElasticConverterShould.NAME).to(ElasticConverterShould.class);
        convertersBinder.addBinding(ElasticConverterMinimumShouldMatch.NAME).to(ElasticConverterMinimumShouldMatch.class);
        convertersBinder.addBinding(ElasticConverterValue.NAME).to(ElasticConverterValue.class);
        convertersBinder.addBinding(ElasticConverterMetric.NAME).to(ElasticConverterMetric.class);
        convertersBinder.addBinding(ElasticConverterFuzziness.NAME).to(ElasticConverterFuzziness.class);
        convertersBinder.addBinding(ElasticConverterOperator.NAME).to(ElasticConverterOperator.class);

        convertersBinder.addBinding(ElasticConverterRyftEnabled.NAME).to(ElasticConverterRyftEnabled.class);
        convertersBinder.addBinding(ElasticConverterRyftLimit.NAME).to(ElasticConverterRyftLimit.class);

        convertersBinder.addBinding(ElasticConverterUnknown.NAME).to(ElasticConverterUnknown.class);

        bind(RyftQueryFactory.class).in(Singleton.class);
    }

}
