package com.ryft.elasticsearch.converter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQueryComplex;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import com.ryft.elasticsearch.converter.entities.RyftRequestParametersFactory;
import com.ryft.elasticsearch.converter.ryftdsl.RyftFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentType;

public class ElasticConverter implements ElasticConvertingElement<RyftRequestParameters> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverter.class);

    private final ContextFactory contextFactory;
    private final RyftRequestParametersFactory ryftRequestParametersFactory;

    @Inject
    public ElasticConverter(ContextFactory contextFactory,
            RyftRequestParametersFactory ryftRequestParametersFactory) {
        this.contextFactory = contextFactory;
        this.ryftRequestParametersFactory = ryftRequestParametersFactory;
    }

    @Override
    public RyftRequestParameters convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.info("Request payload: {}", convertingContext.getOriginalQuery());
        String currentName;
        try {
            convertingContext.getContentParser().nextToken();
        } catch (IOException ex) {
            throw new ElasticConversionException("Request parsing error", ex.getCause());
        }
        RyftQuery ryftQuery = null;
        do {
            currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
            if ((currentName != null) && (!ElasticConversionUtil.isClosePrimitive(convertingContext))) {
                Object conversionResult = convertingContext.getElasticConverter(currentName).convert(convertingContext);
                if (conversionResult instanceof RyftQuery) {
                    ryftQuery = (RyftQuery) conversionResult;
                } else if (convertingContext.getFiltered()) {
                    ryftQuery = applyFilters(convertingContext, ryftQuery, conversionResult);
                }
            }
        } while (convertingContext.getContentParser().currentToken() != null);
        if (ryftQuery == null) {
            return null;
        }
        return getRyftRequestParameters(convertingContext, ryftQuery);
    }

    public RyftRequestParameters convert(ActionRequest request) throws ElasticConversionException {
        try {
            if (request instanceof SearchRequest) {
                SearchRequest searchRequest = (SearchRequest) request;
                ElasticConvertingContext convertingContext = contextFactory.create();
                convertingContext.setSearchRequest(searchRequest);
                if (convertingContext.getContentParser().contentType().equals(XContentType.JSON)
                        && (convertingContext.getIndices().length > 0)
                        && !convertingContext.getIndices()[0].equals(".kibana")) {
                    RyftRequestParameters result = convert(convertingContext);
                    adjustRequest(searchRequest, convertingContext);
                    return result;
                } else {
                    return null;
                }
            } else {
                throw new ElasticConversionException("Request is not SearchRequest");
            }
        } catch (IOException ex) {
            throw new ElasticConversionException("Request parsing error", ex);
        } catch (RuntimeException ex) {
            throw new ElasticConversionException(ex.getMessage());
        }
    }

    private void adjustRequest(SearchRequest request, ElasticConvertingContext convertingContext) throws IOException {
        convertingContext.getQueryJsonNode().findParents(QueryConverterHelper.RYFT_PROPERTY)
                .forEach(parentNode -> ((ObjectNode) parentNode).remove(QueryConverterHelper.RYFT_PROPERTY));
        convertingContext.getQueryJsonNode().findParents(QueryConverterHelper.RYFT_ENABLED_PROPERTY)
                .forEach(parentNode -> ((ObjectNode) parentNode).remove(QueryConverterHelper.RYFT_ENABLED_PROPERTY));
        request.source(convertingContext.getQueryMap());
    }

    private RyftQuery applyFilters(ElasticConvertingContext convertingContext, RyftQuery ryftQuery, Object conversionResult) {
        if (conversionResult instanceof ArrayList && ryftQuery != null) {
            ((ArrayList) conversionResult).add(ryftQuery);
            ryftQuery = convertingContext.getQueryFactory().buildComplexQuery(RyftQueryComplex.RyftLogicalOperator.AND, (Collection<RyftQuery>) conversionResult);
        }
        return ryftQuery;
    }

    private RyftRequestParameters getRyftRequestParameters(ElasticConvertingContext convertingContext, RyftQuery ryftQuery) {
        Map<String, Object> queryProperties = QueryConverterHelper.getQueryProperties(convertingContext);
        RyftFormat format = (RyftFormat) queryProperties.get(PropertiesProvider.RYFT_FORMAT);
        if (format != null && (format.equals(RyftFormat.UTF8) || format.equals(RyftFormat.RAW))) {
            ryftQuery = ryftQuery.toRawTextQuery();
        }
        RyftRequestParameters result = ryftRequestParametersFactory.create(ryftQuery, convertingContext.getIndices(), convertingContext.getQueryMap());
        result.getRyftProperties().putAll(queryProperties);
        return result;
    }
}
