package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.utils.Try;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

public class ElasticConverter implements ElasticConvertingElement<RyftRequestEvent> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverter.class);

    private final ContextFactory contextFactory;
    private final ObjectMapper mapper;

    @Inject
    public ElasticConverter(ContextFactory contextFactory) {
        this.contextFactory = contextFactory;
        mapper = new ObjectMapper();
        mapper.configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, false);
    }

    @Override
    public Try<RyftRequestEvent> convert(ElasticConvertingContext convertingContext) {
        LOGGER.info("Request payload: {}", convertingContext.getOriginalQuery());
        return Try.apply(() -> {
            while (!XContentParser.Token.END_OBJECT.equals(convertingContext.getContentParser().currentToken())) {
                String currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
                convertingContext.getElasticConverter(currentName)
                        .flatMap(converter -> (Try<RyftQuery>) converter.convert(convertingContext))
                        .getResultOrException();
            }
            return convertingContext.getRyftRequestEvent();
        });
    }

    public Try<RyftRequestEvent> convert(ActionRequest request) {
        return Try.apply(() -> {
            if (request instanceof SearchRequest) {
                SearchRequest searchRequest = (SearchRequest) request;
                BytesReference searchContent = searchRequest.source();
                String queryString = (searchContent == null) ? "" : searchContent.toUtf8();
                XContentParser parser = XContentFactory.xContent(queryString).createParser(queryString);
                ElasticConvertingContext convertingContext = contextFactory.create(parser, queryString);
                RyftRequestEvent result = convert(convertingContext).getResultOrException();
                result.setIndex(searchRequest.indices());
                adjustRequest(searchRequest);
                return result;
            } else {
                throw new ElasticConversionException("Request is not SearchRequest");
            }
        });
    }

    private void adjustRequest(SearchRequest request) throws IOException {
        BytesReference searchContent = request.source();
        String queryString = (searchContent == null) ? "" : searchContent.toUtf8();
        Map<String, Object> parsedQuery = mapper.readValue(queryString, new TypeReference<Map<String, Object>>() {
        });
        parsedQuery.remove(ElasticConverterRyftEnabled.NAME);
        parsedQuery.remove(ElasticConverterRyftLimit.NAME);
        request.source(parsedQuery);
    }

}
