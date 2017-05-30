package com.ryft.elasticsearch.converter;

import com.ryft.elasticsearch.converter.ryftdsl.RyftOperator;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQueryComplex.RyftLogicalOperator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;

public class ElasticConverterBool implements ElasticConvertingElement<RyftQuery> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterBool.class);

    public static class ElasticConverterMust implements ElasticConvertingElement<List<RyftQuery>> {

        static final String NAME = "must";

        static RyftQuery buildQuery(ElasticConvertingContext convertingContext,
                List<RyftQuery> queryParts) {
            return convertingContext.getQueryFactory().buildComplexQuery(RyftLogicalOperator.AND, queryParts);
        }

        @Override
        public List<RyftQuery> convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            convertingContext.setRyftOperator(RyftOperator.CONTAINS);
            List<RyftQuery> ryftQueryParts = new ArrayList<>();
            XContentParser.Token token;
            try {
                token = convertingContext.getContentParser().nextToken();
            } catch (IOException ex) {
                throw new ElasticConversionException("Request parsing error");
            }
            if (XContentParser.Token.START_ARRAY.equals(token)) {
                List<RyftQuery> ryftQueryList = ElasticConversionUtil.getArray(convertingContext);
                ryftQueryParts.addAll(ryftQueryList);
            }
            if (XContentParser.Token.START_OBJECT.equals(token)) {
                RyftQuery ryftQuery = ElasticConversionUtil.getObject(convertingContext);
                ryftQueryParts.add(ryftQuery);
            }
            return ryftQueryParts;
        }

    }

    public static class ElasticConverterMustNot implements ElasticConvertingElement<List<RyftQuery>> {

        static final String NAME = "must_not";

        static RyftQuery buildQuery(ElasticConvertingContext convertingContext,
                List<RyftQuery> queryParts) {
            return convertingContext.getQueryFactory().buildComplexQuery(RyftLogicalOperator.OR, queryParts);
        }

        @Override
        public List<RyftQuery> convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            convertingContext.setRyftOperator(RyftOperator.NOT_CONTAINS);
            List<RyftQuery> ryftQueryParts = new ArrayList<>();
            XContentParser.Token token;
            try {
                token = convertingContext.getContentParser().nextToken();
            } catch (IOException ex) {
                throw new ElasticConversionException("Request parsing error");
            }
            if (XContentParser.Token.START_ARRAY.equals(token)) {
                List<RyftQuery> ryftQueryList = ElasticConversionUtil.getArray(convertingContext);
                ryftQueryParts.addAll(ryftQueryList);
            }
            if (XContentParser.Token.START_OBJECT.equals(token)) {
                RyftQuery ryftQuery = ElasticConversionUtil.getObject(convertingContext);
                ryftQueryParts.add(ryftQuery);
            }
            return ryftQueryParts;

        }
    }

    public static class ElasticConverterShould implements ElasticConvertingElement<List<RyftQuery>> {

        static final String NAME = "should";

        @Override
        public List<RyftQuery> convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            convertingContext.setRyftOperator(RyftOperator.CONTAINS);
            List<RyftQuery> ryftQueryParts = new ArrayList<>();
            XContentParser.Token token;
            try {
                token = convertingContext.getContentParser().nextToken();
            } catch (IOException ex) {
                throw new ElasticConversionException("Request parsing error");
            }
            if (XContentParser.Token.START_ARRAY.equals(token)) {
                List<RyftQuery> ryftQueryList = ElasticConversionUtil.getArray(convertingContext);
                ryftQueryParts.addAll(ryftQueryList);
            }
            if (XContentParser.Token.START_OBJECT.equals(token)) {
                RyftQuery ryftQuery = ElasticConversionUtil.getObject(convertingContext);
                ryftQueryParts.add(ryftQuery);
            }
            return ryftQueryParts;
        }

        static RyftQuery buildQuery(ElasticConvertingContext convertingContext,
                List<RyftQuery> queryParts) {
            List<List<RyftQuery>> queryCombinationsList = getSublists(queryParts, convertingContext.getMinimumShouldMatch());
            List<RyftQuery> shouldQueryCombinedList = new ArrayList<>();
            for (List<RyftQuery> shouldQueryCombination : queryCombinationsList) {
                shouldQueryCombinedList.add(convertingContext.getQueryFactory().buildComplexQuery(RyftLogicalOperator.AND, shouldQueryCombination));
            }
            return convertingContext.getQueryFactory().buildComplexQuery(RyftLogicalOperator.OR, shouldQueryCombinedList);
        }

        private static <T> void getSublists(List<T> superSet, Integer size, Integer index, List<T> current, List<List<T>> result) {
            if (current.size() == size) { //successful stop clause
                result.add(new ArrayList<>(current));
                return;
            }
            if (index == superSet.size()) { //unseccessful stop clause
                return;
            }
            T x = superSet.get(index);
            current.add(x);
            getSublists(superSet, size, index + 1, current, result); //"guess" x is in the subset
            current.remove(x);
            getSublists(superSet, size, index + 1, current, result); //"guess" x is not in the subset
        }

        private static <T> List<List<T>> getSublists(List<T> list, Integer size) {
            List<List<T>> result = new ArrayList<>();
            getSublists(list, size, 0, new ArrayList<T>(), result);
            return result;
        }
    }

    public static class ElasticConverterMinimumShouldMatch implements ElasticConvertingElement<Integer> {

        final static String NAME = "minimum_should_match";
        final static String NAME_ALTERNATIVE = "minimum_number_should_match";

        @Override
        public Integer convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            Integer minimumShouldMatch = ElasticConversionUtil.getInteger(convertingContext);
            convertingContext.setMinimumShouldMatch(minimumShouldMatch);
            return minimumShouldMatch;
        }

    }

    static final String NAME = "bool";

    @Override
    public RyftQuery convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        Map<String, List<RyftQuery>> boolQueryMap = new HashMap<>();
        XContentParser parser = convertingContext.getContentParser();
        try {
            parser.nextToken();
        } catch (IOException ex) {
            throw new ElasticConversionException("Request parsing error");
        }
        String currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
        do {
            Object conversionResult = convertingContext.getElasticConverter(currentName).convert(convertingContext);
            if (conversionResult instanceof List) {
                boolQueryMap.put(currentName, (List) conversionResult);
            }
            currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
        } while (!XContentParser.Token.END_OBJECT.equals(parser.currentToken()));
        return getRyftQuery(convertingContext, boolQueryMap);
    }

    private RyftQuery getRyftQuery(ElasticConvertingContext convertingContext, Map<String, List<RyftQuery>> queryPartsMap) {
        Map<String, RyftQuery> resultQueryMap = new HashMap<>();
        for (Map.Entry<String, List<RyftQuery>> entry : queryPartsMap.entrySet()) {
            String key = entry.getKey();
            List<RyftQuery> value = entry.getValue();
            switch (key) {
                case ElasticConverterMust.NAME:
                    resultQueryMap.put(key, ElasticConverterMust.buildQuery(convertingContext, value));
                    break;
                case ElasticConverterMustNot.NAME:
                    resultQueryMap.put(key, ElasticConverterMustNot.buildQuery(convertingContext, value));
                    break;
                case ElasticConverterShould.NAME:
                    resultQueryMap.put(key, ElasticConverterShould.buildQuery(convertingContext, value));
                    break;
                default:
                    return null;
            }
        }
        if (resultQueryMap.containsKey(ElasticConverterShould.NAME)
                && (!convertingContext.isMinimumShouldMatchDefined())
                && (resultQueryMap.containsKey(ElasticConverterMust.NAME)
                || resultQueryMap.containsKey(ElasticConverterMustNot.NAME))) {
            resultQueryMap.remove(ElasticConverterShould.NAME);
        }
        return convertingContext.getQueryFactory().buildComplexQuery(RyftLogicalOperator.AND, resultQueryMap.values());
    }

}
