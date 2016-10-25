package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
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

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverter.class);

    private final ImmutableMap<String, ElasticConvertingElement> converters;

    @Inject
    public ElasticConverter(Map<String, ElasticConvertingElement> injectedConverters) {
        this.converters = ImmutableMap.copyOf(injectedConverters);
    }

    public Optional<RyftQuery> convert(ElasticConvertingContext convertingContext) throws IOException, ClassNotFoundException {
        XContentParser parser = convertingContext.getContentParser();
        XContentParser.Token token = parser.nextToken();
        String currentName = parser.currentName();
        if (START_OBJECT.equals(token) && currentName == null) {
            token = parser.nextToken();
            currentName = parser.currentName();
        }
        if (FIELD_NAME.equals(token)) {
            ElasticConvertingElement converter = convertingContext.getElasticConverter(currentName);
            RyftQuery result = converter.convert(convertingContext);
            return Optional.ofNullable(result);
        }
        return Optional.empty();
    }

    public Optional<RyftQuery> convert(ActionRequest request) {
        if (request instanceof SearchRequest) {
            BytesReference searchContent = ((SearchRequest) request).source();
            LOGGER.info("Request payload: {}", searchContent.toUtf8());
            try {
                XContentParser parser = XContentFactory.xContent(searchContent)
                        .createParser(searchContent);
                ElasticConvertingContext convertingContext = new ElasticConvertingContext(parser, converters);
                return convert(convertingContext);
            } catch (IOException | ClassNotFoundException ex) {
                LOGGER.warn("Can not parse elasticsearch request.", ex);
            }
        }
        return Optional.empty();
    }

}
