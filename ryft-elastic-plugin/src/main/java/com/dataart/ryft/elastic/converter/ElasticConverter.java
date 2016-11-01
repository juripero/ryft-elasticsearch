package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.utils.Try;
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

    @Inject
    public ElasticConverter(ContextFactory contextFactory) {
        this.contextFactory = contextFactory;
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
                XContentParser parser = XContentFactory.xContent(searchContent).createParser(searchContent);
                ElasticConvertingContext convertingContext = contextFactory.create(parser, queryString);
                RyftRequestEvent result = convert(convertingContext).getResultOrException();
                result.setIndex(searchRequest.indices());
                return result;
            } else {
                throw new ElasticConversionException("Request is not SearchRequest");
            }
        });
    }

}
