package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.disruptor.messages.RyftRequestEventFactory;
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
    private final RyftRequestEventFactory ryftRequestEventFactory;

    @Inject
    public ElasticConverter(ContextFactory contextFactory,
            RyftRequestEventFactory ryftRequestEventFactory) {
        this.contextFactory = contextFactory;
        this.ryftRequestEventFactory = ryftRequestEventFactory;
        mapper = new ObjectMapper();
        mapper.configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, false);
    }

    @Override
    public Try<RyftRequestEvent> convert(ElasticConvertingContext convertingContext) {
        LOGGER.trace("Request payload: {}", convertingContext.getOriginalQuery());
        return Try.apply(() -> {
            String currentName;
            convertingContext.getContentParser().nextToken();
            RyftQuery ryftQuery = null;
            do {
                currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
                if ((currentName != null) && (!ElasticConversionUtil.isClosePrimitive(convertingContext))) {
                    Object conversionResult = convertingContext.getElasticConverter(currentName)
                            .map(converter -> converter.convert(convertingContext).getResultOrException())
                            .getResultOrException();
                    if (conversionResult instanceof RyftQuery) {
                        ryftQuery = (RyftQuery) conversionResult;
                    }
                }
            } while (convertingContext.getContentParser().currentToken() != null);
            if (ryftQuery == null) {
                return null;
            }
            return getRyftRequestEvent(convertingContext, ryftQuery);
        });
    }

    public Try<RyftRequestEvent> convert(ActionRequest request) {
        return Try.apply(() -> {
            if (request instanceof SearchRequest) {
                SearchRequest searchRequest = (SearchRequest) request;
                BytesReference searchContent = searchRequest.source();
                String queryString = (searchContent == null) ? "" : searchContent.toUtf8();
                LOGGER.debug("Request: {}", queryString);
                XContentParser parser = XContentFactory.xContent(queryString).createParser(queryString);
                ElasticConvertingContext convertingContext = contextFactory.create(parser, queryString);
                RyftRequestEvent result = convert(convertingContext).getResultOrException();
                if (result != null) {
                    LOGGER.info("Constructed query: {}", result.getQuery().buildRyftString());
                    result.setIndex(searchRequest.indices());
                }
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
        parsedQuery.remove(ElasticConverterRyft.NAME);
        if (parsedQuery.containsKey(ElasticConverterQuery.NAME)) {
            Map<String, Object> innerQuery = ((Map<String, Object>) parsedQuery.get(ElasticConverterQuery.NAME));
            innerQuery.remove(ElasticConverterRyftEnabled.NAME);
            innerQuery.remove(ElasticConverterRyft.NAME);
        }
        request.source(parsedQuery);
    }

    private RyftRequestEvent getRyftRequestEvent(ElasticConvertingContext convertingContext, RyftQuery ryftQuery) {
        RyftRequestEvent result = ryftRequestEventFactory.create(ryftQuery);
        result.getRyftProperties().putAll(convertingContext.getQueryProperties());
        return result;
    }
}
