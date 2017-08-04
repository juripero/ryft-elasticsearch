package com.ryft.elasticsearch.converter;

import com.ryft.elasticsearch.converter.ElasticConverterRyft.ElasticConverterFormat.RyftFormat;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQueryComplex;
import com.ryft.elasticsearch.plugin.PropertiesProvider;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;
import com.ryft.elasticsearch.converter.entities.RyftRequestParametersFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ElasticConverter implements ElasticConvertingElement<RyftRequestParameters> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverter.class);

    private final ContextFactory contextFactory;
    private final ObjectMapper mapper;
    private final RyftRequestParametersFactory ryftRequestParametersFactory;

    @Inject
    public ElasticConverter(ContextFactory contextFactory,
            RyftRequestParametersFactory ryftRequestParametersFactory) {
        this.contextFactory = contextFactory;
        this.ryftRequestParametersFactory = ryftRequestParametersFactory;
        // TODO API clients can send requests with message represented in yaml, smile or cbor formats. 
        // Use ObjectMapper with appropriate JsonFactory.
        mapper = new ObjectMapper();
        mapper.configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, false);
    }

    @Override
    public RyftRequestParameters convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug("Request payload: {}", convertingContext.getOriginalQuery());
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

        RyftFormat format = (RyftFormat) convertingContext.getQueryProperties().get(PropertiesProvider.RYFT_FORMAT);

        if (format != null && (format.equals(RyftFormat.UTF8) || format.equals(RyftFormat.RAW))) {
            ryftQuery = ryftQuery.toRawTextQuery();
        }

        return getRyftRequestParameters(convertingContext, ryftQuery);
    }

    public RyftRequestParameters convert(ActionRequest request) throws ElasticConversionException {
        try {
            if (request instanceof SearchRequest) {
                SearchRequest searchRequest = (SearchRequest) request;
                ElasticConvertingContext convertingContext = contextFactory.create();
                convertingContext.setSearchRequest(searchRequest);
                RyftRequestParameters result = convert(convertingContext);
                adjustRequest(searchRequest);
                return result;
            } else {
                throw new ElasticConversionException("Request is not SearchRequest");
            }
        } catch (IOException ex) {
            throw new ElasticConversionException("Request parsing error", ex);
        } catch (UnsupportedOperationException ex) {
            throw new ElasticConversionException(ex.getMessage());
        }
    }

    private void adjustRequest(SearchRequest request) throws IOException {
        Map<String, Object> parsedQuery = mapper.readValue(request.source().array(), new TypeReference<Map<String, Object>>() {
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

    private RyftQuery applyFilters(ElasticConvertingContext convertingContext, RyftQuery ryftQuery, Object conversionResult) {
        if (conversionResult instanceof ArrayList && ryftQuery != null) {
            ((ArrayList) conversionResult).add(ryftQuery);
            ryftQuery = convertingContext.getQueryFactory().buildComplexQuery(RyftQueryComplex.RyftLogicalOperator.AND, (Collection<RyftQuery>) conversionResult);
        }
        return ryftQuery;
    }

    private RyftRequestParameters getRyftRequestParameters(ElasticConvertingContext convertingContext, RyftQuery ryftQuery) {
        RyftRequestParameters result = ryftRequestParametersFactory.create(ryftQuery, convertingContext.getIndices(), convertingContext.getAggregationBuilders());
        result.getRyftProperties().putAll(convertingContext.getQueryProperties());
        return result;
    }
}
