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
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import static org.elasticsearch.common.xcontent.XContentParser.Token.*;

public class ElasticConverter {
    private final ESLogger logger = Loggers.getLogger(getClass());
    private final ImmutableMap<String, ElasticConvertingElement> converters;
    private final ContextFactory contextFactory;

    @Inject
    public ElasticConverter(ContextFactory contextFactory, Set<ElasticConvertingElement> injectedConverters) {
        Map<String, ElasticConvertingElement> convertersMap = Maps.newHashMap();
        for (ElasticConvertingElement converter : injectedConverters) {
            for (String name : converter.names()) {
                convertersMap.put(name, converter);
            }
        }
        this.converters = ImmutableMap.copyOf(convertersMap);
        this.contextFactory = contextFactory;
    }

    public Optional<RyftQuery> parse(ElasticConvertingContext convertingContext) throws IOException {
        XContentParser parser = convertingContext.getContentParser();
        XContentParser.Token token = parser.nextToken();
        String currentName = parser.currentName();
        if (START_OBJECT.equals(token) && currentName == null) {
            token = parser.nextToken();
            currentName = parser.currentName();
        }
        if (FIELD_NAME.equals(token)) {
            ElasticConvertingElement element = converters.get(currentName);
            if (element == null) {
                //parser.ge
                logger.warn("Failed to find appropriate converter for token: ' {} ' available converters {}. Original query: {}", currentName, converters.keySet(), convertingContext.getOriginalQuery());
                return Optional.empty();
            }
            RyftQuery result = element.convert(convertingContext);
            return Optional.ofNullable(result);
        }
        return Optional.empty();
    }

    public Optional<RyftQuery> parse(ActionRequest request) throws ElasticConversionException {
        if (request instanceof SearchRequest) {
            BytesReference searchContent = ((SearchRequest) request).source();
            String query = new String(searchContent.copyBytesArray().array());
            try {
                XContentParser parser = XContentFactory.xContent(searchContent).createParser(searchContent);
                
                //TODO: [imasternoy] Use AssistedInject
                ElasticConvertingContext convertingContext = contextFactory.create(parser);
                convertingContext.setOriginalQuery(query);
                return parse(convertingContext);
            } catch (Exception ex) {
                throw new ElasticConversionException(ex);
            }
        }
        return Optional.empty();
    }

}
