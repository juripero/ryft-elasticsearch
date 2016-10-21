package com.dataart.ryft.elastic.converter;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;

public class ElasticConversionModule extends AbstractModule {

    @Override
    protected void configure() {
        Multibinder<ElasticConvertingElement> convertersBinder = 
                Multibinder.newSetBinder(binder(), ElasticConvertingElement.class);
        convertersBinder.addBinding().to(ElasticConverterQuery.class);
        convertersBinder.addBinding().to(ElasticConverterFuzzy.class);
        convertersBinder.addBinding().to(ElasticConverterField.class);
        convertersBinder.addBinding().to(ElasticConverterBool.class);
        convertersBinder.addBinding().to(ElasticConverterBool.ElasticConverterMust.class);
    }

}
