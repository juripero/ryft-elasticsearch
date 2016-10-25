package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ElasticConverterBool.ElasticConverterMust;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryFactory;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class ElasticConversionModule extends AbstractModule {

    @Override
    protected void configure() {
        MapBinder<String, ElasticConvertingElement> convertersBinder
                = MapBinder.newMapBinder(binder(), String.class, ElasticConvertingElement.class);
        convertersBinder.addBinding(ElasticConverterQuery.NAME).to(ElasticConverterQuery.class);
        convertersBinder.addBinding(ElasticConverterFuzzy.NAME1).to(ElasticConverterFuzzy.class);
        convertersBinder.addBinding(ElasticConverterFuzzy.NAME2).to(ElasticConverterFuzzy.class);
        convertersBinder.addBinding(ElasticConverterField.NAME).to(ElasticConverterField.class);
        convertersBinder.addBinding(ElasticConverterBool.NAME).to(ElasticConverterBool.class);
        convertersBinder.addBinding(ElasticConverterMust.NAME).to(ElasticConverterMust.class);

        bind(RyftQueryFactory.class).in(Singleton.class);
    }

}
