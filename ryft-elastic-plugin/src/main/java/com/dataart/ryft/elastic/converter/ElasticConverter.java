package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import static org.elasticsearch.common.xcontent.XContentParser.Token.*;

public class ElasticConverter {

    private final ImmutableMap<String, ElasticConvertingElement> converters;

    @Inject
    public ElasticConverter(Set<ElasticConvertingElement> injectedConverters) {
        Map<String, ElasticConvertingElement> convertersMap = Maps.newHashMap();
        for (ElasticConvertingElement converter : injectedConverters) {
            for (String name : converter.names()) {
                convertersMap.put(name, converter);
            }
        }
        this.converters = ImmutableMap.copyOf(convertersMap);
    }

    public Optional<RyftQuery> parse(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        String currentName = parser.currentName();
        if (START_OBJECT.equals(token) && currentName == null) {
            token = parser.nextToken();
            currentName = parser.currentName();
        }
        if (FIELD_NAME.equals(token)) {
            ElasticConvertingContext convertingContext = new ElasticConvertingContext(parser, converters);
            RyftQuery result = converters.get(currentName).convert(convertingContext);
            return Optional.ofNullable(result);
        }
        return Optional.empty();
    }

    public Optional<RyftQuery> parse(ActionRequest request) throws ElasticConversionException {
        if (request instanceof SearchRequest) {
            BytesReference searchContent = ((SearchRequest) request).source();
            try {
                XContentParser parser = XContentFactory.xContent(searchContent)
                        .createParser(searchContent);
                return parse(parser);
            } catch (Exception ex) {
                throw new ElasticConversionException(ex);
            }
        }
        return Optional.empty();
    }

}
