package com.ryft.elasticsearch.converter;

import com.ryft.elasticsearch.converter.ElasticConverterBool.*;
import com.ryft.elasticsearch.converter.ElasticConverterField.*;
import com.ryft.elasticsearch.converter.ElasticConverterRyft.*;
import com.ryft.elasticsearch.converter.ElasticConverterShared.*;
import com.ryft.elasticsearch.converter.ElasticConverterRangeField.*;
import com.ryft.elasticsearch.converter.aggregations.*;
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

        convertersBinder.addBinding(ElasticConverterRyftEnabled.NAME).to(ElasticConverterRyftEnabled.class);
        convertersBinder.addBinding(ElasticConverterSize.NAME).to(ElasticConverterSize.class);
        convertersBinder.addBinding(ElasticConverterRyft.NAME).to(ElasticConverterRyft.class);
        convertersBinder.addBinding(ElasticConverterEnabled.NAME).to(ElasticConverterEnabled.class);
        convertersBinder.addBinding(ElasticConverterFiles.NAME).to(ElasticConverterFiles.class);
        convertersBinder.addBinding(ElasticConverterFormat.NAME).to(ElasticConverterFormat.class);
        convertersBinder.addBinding(ElasticConverterCaseSensitive.NAME).to(ElasticConverterCaseSensitive.class);

        convertersBinder.addBinding(ElasticConverterAggs.NAME).to(ElasticConverterAggs.class);
        convertersBinder.addBinding(ElasticConverterAggs.NAME_ALTERNATIVE).to(ElasticConverterAggs.class);
        
        convertersBinder.addBinding(ElasticConverterFiltered.NAME).to(ElasticConverterFiltered.class);

        convertersBinder.addBinding(ElasticConverterUnknown.NAME).to(ElasticConverterUnknown.class);

        bind(RyftRequestParametersFactory.class).toProvider(
                FactoryProvider.newFactory(RyftRequestParametersFactory.class, RyftRequestParameters.class)).in(Singleton.class);

        bind(RyftQueryFactory.class).in(Singleton.class);
        bind(AggregationFactory.class).in(Singleton.class);
    }

}
