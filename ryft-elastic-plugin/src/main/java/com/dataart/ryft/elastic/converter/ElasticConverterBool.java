package com.dataart.ryft.elastic.converter;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftOperator;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryComplex.RyftLogicalOperator;
import com.dataart.ryft.utils.Try;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;

public class ElasticConverterBool implements ElasticConvertingElement<RyftQuery> {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterBool.class);

    public static class ElasticConverterMust implements ElasticConvertingElement<List<RyftQuery>> {

        static final String NAME = "must";

        @Override
        public Try<List<RyftQuery>> convert(ElasticConvertingContext convertingContext) {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return Try.apply(() -> {
                convertingContext.setRyftOperator(RyftOperator.CONTAINS);
                List<RyftQuery> ryftQueryParts = ElasticConversionUtil.getArray(convertingContext);
                return ryftQueryParts;
            });
        }

        public static RyftQuery buildQuery(ElasticConvertingContext convertingContext,
                List<RyftQuery> queryParts) {
            return convertingContext.getQueryFactory().buildComplexQuery(RyftLogicalOperator.AND, queryParts);
        }

    }

    public static class ElasticConverterMustNot implements ElasticConvertingElement<List<RyftQuery>> {

        static final String NAME = "must_not";

        @Override
        public Try<List<RyftQuery>> convert(ElasticConvertingContext convertingContext) {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return Try.apply(() -> {
                convertingContext.setRyftOperator(RyftOperator.NOT_CONTAINS);
                List<RyftQuery> ryftQueryParts = ElasticConversionUtil.getArray(convertingContext);
                return ryftQueryParts;
            });
        }

        public static RyftQuery buildQuery(ElasticConvertingContext convertingContext,
                List<RyftQuery> queryParts) {
            return convertingContext.getQueryFactory().buildComplexQuery(RyftLogicalOperator.OR, queryParts);
        }
    }

    public static class ElasticConverterShould implements ElasticConvertingElement<List<RyftQuery>> {

        static final String NAME = "should";

        @Override
        public Try<List<RyftQuery>> convert(ElasticConvertingContext convertingContext) {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return Try.apply(() -> {
                convertingContext.setRyftOperator(RyftOperator.CONTAINS);
                List<RyftQuery> ryftQueryParts = ElasticConversionUtil.getArray(convertingContext);
                return ryftQueryParts;
            });
        }

        public static RyftQuery buildQuery(ElasticConvertingContext convertingContext,
                List<RyftQuery> queryParts) {
            List<List<RyftQuery>> queryCombinationsList = getSublists(queryParts, convertingContext.getMinimumShouldMatch());
            List<RyftQuery> shouldQueryCombinedList = queryCombinationsList.stream()
                    .map(shouldQueryCombination -> convertingContext.getQueryFactory().buildComplexQuery(RyftLogicalOperator.AND, shouldQueryCombination))
                    .collect(Collectors.toList());
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

        @Override
        public Try<Integer> convert(ElasticConvertingContext convertingContext) {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return Try.apply(() -> {
                Integer minimumShouldMatch = ElasticConversionUtil.getInteger(convertingContext);
                convertingContext.setMinimumShouldMatch(minimumShouldMatch);
                return minimumShouldMatch;
            });
        }

    }

    static final String NAME = "bool";

    @Override
    public Try<RyftQuery> convert(ElasticConvertingContext convertingContext) {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
        Map<String, List<RyftQuery>> boolQueryMap = new HashMap<>();
        return Try.apply(() -> {
            XContentParser parser = convertingContext.getContentParser();
            parser.nextToken();
            String currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
            do {
                Object conversionResult = convertingContext.getElasticConverter(currentName)
                        .flatMap(converter -> converter.convert(convertingContext)).getResultOrException();
                if (conversionResult instanceof List) {
                    boolQueryMap.put(currentName, (List) conversionResult);
                }
                currentName = ElasticConversionUtil.getNextElasticPrimitive(convertingContext);
            } while (!XContentParser.Token.END_OBJECT.equals(parser.currentToken()));
            RyftQuery result = getRyftQuery(convertingContext, boolQueryMap);
            return result;
        });
    }

    private RyftQuery getRyftQuery(ElasticConvertingContext convertingContext, Map<String, List<RyftQuery>> boolQueryMap) {
        List<RyftQuery> boolQueryList = new ArrayList<>();
        boolQueryMap.forEach((name, queryParts) -> {
            switch (name) {
                case ElasticConverterMust.NAME:
                    boolQueryList.add(ElasticConverterMust.buildQuery(convertingContext, queryParts));
                    break;
                case ElasticConverterMustNot.NAME:
                    boolQueryList.add(ElasticConverterMustNot.buildQuery(convertingContext, queryParts));
                    break;
                case ElasticConverterShould.NAME:
                    boolQueryList.add(ElasticConverterShould.buildQuery(convertingContext, queryParts));
                    break;
            }
        });
        return convertingContext.getQueryFactory().buildComplexQuery(RyftLogicalOperator.AND, boolQueryList);
    }

}
