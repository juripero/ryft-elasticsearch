package com.dataart.ryft.elastic.converter;

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

public class ElasticConverter implements ElasticConvertingElement {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverter.class);

    private final ContextFactory contextFactory;

    @Inject
    public ElasticConverter(ContextFactory contextFactory) {
        this.contextFactory = contextFactory;
    }

    @Override
    public Try<RyftQuery> convert(ElasticConvertingContext convertingContext) {
        LOGGER.info("Request payload: {}", convertingContext.getOriginalQuery());
        return Try.apply(() -> {
            String currentName = getNextElasticPrimitive(convertingContext);
            return convertingContext.getElasticConverter(currentName)
                    .flatMap(converter -> converter.convert(convertingContext))
                    .getResultOrException();
        });
    }

    public Try<RyftQuery> convert(ActionRequest request) {
        return Try.apply(() -> {
            if (request instanceof SearchRequest) {
                BytesReference searchContent = ((SearchRequest) request).source();
                String queryString = searchContent.toUtf8();
                XContentParser parser = XContentFactory.xContent(searchContent).createParser(searchContent);
                ElasticConvertingContext convertingContext = contextFactory.create(parser, queryString);
                return convert(convertingContext).getResultOrException();
            } else {
                throw new ElasticConversionException("Request is not SearchRequest");
            }
        });
    }

}
