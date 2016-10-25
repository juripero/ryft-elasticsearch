package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpression;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionExactSearch;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionFuzzySearch;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftInputSpecifierRecord;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftOperator;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuerySimple;
import java.util.Map;
import org.elasticsearch.common.xcontent.XContentParser;

public class ElasticConvertingContext {

    private final XContentParser contentParser;
    private final Map<String, ElasticConvertingElement> elasticConverters;

    @Inject
    public ElasticConvertingContext(@Assisted XContentParser parser, Set<ElasticConvertingElement> injectedConverters) {
        Map<String, ElasticConvertingElement> convertersMap = Maps.newHashMap();
        for (ElasticConvertingElement converter : injectedConverters) {
            for (String name : converter.names()) {
                convertersMap.put(name, converter);
            }
        }
        this.elasticConverters = ImmutableMap.copyOf(convertersMap);
        this.contentParser = parser;
    }

    public XContentParser getContentParser() {
        return contentParser;
    }

    public ElasticConvertingElement getElasticConverter(String name) throws ClassNotFoundException {
        ElasticConvertingElement result = elasticConverters.get(name);
        if (result == null) {
            throw new ClassNotFoundException(String.format("Can not find elastic parser for: %s", name));
        }
        return result;
    }

}
