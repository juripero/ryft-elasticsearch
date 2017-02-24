package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

import com.googlecode.ipv6.IPv6Network;
import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConversionException;
import com.ryft.elasticsearch.plugin.elastic.converter.entities.FuzzyQueryParameters;
import com.ryft.elasticsearch.plugin.elastic.converter.entities.RangeQueryParameters;
import com.ryft.elasticsearch.plugin.elastic.converter.entities.TermQueryParameters;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQueryComplex.RyftLogicalOperator;

import java.io.IOException;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class RyftQueryFactory {

    private final static ESLogger LOGGER = Loggers.getLogger(RyftQueryFactory.class);

    public final static Integer FUZZYNESS_AUTO_VALUE = -1;
    private final static Integer TEXT_LENGTH_NO_FUZZINESS = 3;
    private final static Integer TEXT_LENGTH_FUZZINESS = 5;

    public RyftQuery buildFuzzyQuery(FuzzyQueryParameters fuzzyQueryParameters) throws ElasticConversionException {
        fuzzyQueryParameters.check();
        switch (fuzzyQueryParameters.getSearchType()) {
            case FUZZY:
                return buildQueryFuzzy(
                        fuzzyQueryParameters.getSearchValue(),
                        fuzzyQueryParameters.getFieldName(),
                        fuzzyQueryParameters.getMetric(),
                        fuzzyQueryParameters.getFuzziness(),
                        fuzzyQueryParameters.getRyftOperator(),
                        fuzzyQueryParameters.getWidth(),
                        fuzzyQueryParameters.getLine());
            case MATCH:
                return buildQueryMatch(
                        fuzzyQueryParameters.getSearchValue(),
                        fuzzyQueryParameters.getFieldName(),
                        fuzzyQueryParameters.getOperator(),
                        fuzzyQueryParameters.getMetric(),
                        fuzzyQueryParameters.getFuzziness(),
                        fuzzyQueryParameters.getRyftOperator(),
                        fuzzyQueryParameters.getWidth(),
                        fuzzyQueryParameters.getLine());
            case MATCH_PHRASE:
                return buildQueryMatchPhrase(
                        fuzzyQueryParameters.getSearchValue(),
                        fuzzyQueryParameters.getFieldName(),
                        fuzzyQueryParameters.getMetric(),
                        fuzzyQueryParameters.getFuzziness(),
                        fuzzyQueryParameters.getRyftOperator(),
                        fuzzyQueryParameters.getWidth(),
                        fuzzyQueryParameters.getLine());
            case WILDCARD:
                return buildQueryWildcard(
                        fuzzyQueryParameters.getSearchValue(),
                        fuzzyQueryParameters.getFieldName(),
                        fuzzyQueryParameters.getRyftOperator(),
                        fuzzyQueryParameters.getWidth(),
                        fuzzyQueryParameters.getLine());
            default:
                throw new ElasticConversionException("Unknown search type");
        }
    }

    public RyftQuery buildTermQuery(TermQueryParameters termQueryParameters) throws ElasticConversionException {
        switch (termQueryParameters.getDataType()) {
            case DATETIME:
                return buildQueryDateTimeTerm(
                        termQueryParameters.getSearchValue(),
                        termQueryParameters.getFormat(),
                        termQueryParameters.getFieldName(),
                        termQueryParameters.getRyftOperator());
            case NUMBER:
                return buildQueryNumericTerm(
                        termQueryParameters.getSearchValue(),
                        termQueryParameters.getFieldName(),
                        termQueryParameters.getSeparator(),
                        termQueryParameters.getDecimal(),
                        termQueryParameters.getRyftOperator());
            case NUMBER_ARRAY:
                return buildQueryNumericArrayTerm(
                        termQueryParameters.getSearchArray(),
                        termQueryParameters.getFieldName(),
                        termQueryParameters.getSeparator(),
                        termQueryParameters.getDecimal(),
                        termQueryParameters.getRyftOperator());
            case CURRENCY:
                return buildQueryCurrencyTerm(
                        termQueryParameters.getSearchValue(),
                        termQueryParameters.getFieldName(),
                        termQueryParameters.getCurrency(),
                        termQueryParameters.getSeparator(),
                        termQueryParameters.getDecimal(),
                        termQueryParameters.getRyftOperator());
            case IPV4:
                return buildQueryIpv4Term(
                        termQueryParameters.getSearchValue(),
                        termQueryParameters.getFieldName(),
                        termQueryParameters.getRyftOperator());
            case IPV6:
                return buildQueryIpv6Term(
                        termQueryParameters.getSearchValue(),
                        termQueryParameters.getFieldName(),
                        termQueryParameters.getRyftOperator());
            default:
                throw new ElasticConversionException("Unknown data type");
        }

    }

    public RyftQuery buildRangeQuery(RangeQueryParameters rangeQueryParameters) throws ElasticConversionException {
        rangeQueryParameters.check();
        switch (rangeQueryParameters.getDataType()) {
            case DATETIME:
                return buildQueryDateTimeRange(
                        rangeQueryParameters.getLowerBound(),
                        rangeQueryParameters.getUpperBound(),
                        rangeQueryParameters.getFormat(),
                        rangeQueryParameters.getFieldName(),
                        rangeQueryParameters.getRyftOperator());
            case NUMBER:
                return buildQueryNumericRange(
                        rangeQueryParameters.getLowerBound(),
                        rangeQueryParameters.getUpperBound(),
                        rangeQueryParameters.getFieldName(),
                        rangeQueryParameters.getSeparator(),
                        rangeQueryParameters.getDecimal(),
                        rangeQueryParameters.getRyftOperator());
            case CURRENCY:
                return buildQueryCurrencyRange(
                        rangeQueryParameters.getLowerBound(),
                        rangeQueryParameters.getUpperBound(),
                        rangeQueryParameters.getFieldName(),
                        rangeQueryParameters.getCurrency(),
                        rangeQueryParameters.getSeparator(),
                        rangeQueryParameters.getDecimal(),
                        rangeQueryParameters.getRyftOperator());
            case IPV4:
                return buildQueryIpv4Range(
                        rangeQueryParameters.getLowerBound(),
                        rangeQueryParameters.getUpperBound(),
                        rangeQueryParameters.getFieldName(),
                        rangeQueryParameters.getRyftOperator());
            case IPV6:
                return buildQueryIpv6Range(
                        rangeQueryParameters.getLowerBound(),
                        rangeQueryParameters.getUpperBound(),
                        rangeQueryParameters.getFieldName(),
                        rangeQueryParameters.getRyftOperator());
            default:
                throw new ElasticConversionException("Unknown data type");
        }

    }

    private RyftQuery buildQueryMatchPhrase(String searchText, String fieldName,
                                            RyftFuzzyMetric metric, Integer fuzziness, RyftOperator ryftOperator,
                                            Integer width, Boolean line) {
        RyftExpression ryftExpression;
        if (FUZZYNESS_AUTO_VALUE.equals(fuzziness)) {
            fuzziness = getFuzzinessAuto(searchText);
        }
        if (fuzziness == 0) {
            if (line != null) {
                ryftExpression = new RyftExpressionExactSearch(searchText, line);
            } else if (width != null) {
                ryftExpression = new RyftExpressionExactSearch(searchText, width);
            } else {
                ryftExpression = new RyftExpressionExactSearch(searchText);
            }
        } else {
            fuzziness = adjustFuzziness(fuzziness, searchText);
            if (line != null) {
                ryftExpression = new RyftExpressionFuzzySearch(searchText, metric, fuzziness, line);
            } else if (width != null) {
                ryftExpression = new RyftExpressionFuzzySearch(searchText, metric, fuzziness, width);
            } else {
                ryftExpression = new RyftExpressionFuzzySearch(searchText, metric, fuzziness);
            }
        }
        return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName),
                ryftOperator, ryftExpression);
    }

    private RyftQuery buildQueryMatch(String searchText, String fieldName,
                                      RyftLogicalOperator operator, RyftFuzzyMetric metric, Integer fuzziness,
                                      RyftOperator ryftOperator,
                                      Integer width, Boolean line) {
        Collection<RyftQuery> operands = tokenize(searchText).stream()
                .map(searchToken -> buildQueryMatchPhrase(searchToken, fieldName, metric, fuzziness, ryftOperator, width, line))
                .collect(Collectors.toList());
        return buildComplexQuery(operator, operands);
    }

    private RyftQuery buildQueryWildcard(String searchText, String fieldName,
                                         RyftOperator ryftOperator,
                                         Integer width, Boolean line) {
        String searchTextFormatted = searchText.replace("?", "\"?\"");
        RyftExpression ryftExpression = new RyftExpressionExactSearch(searchTextFormatted);
        return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName),
                ryftOperator, ryftExpression);
    }

    private RyftQuery buildQueryDateTimeTerm(String searchText, String format, String fieldName,
                                             RyftOperator ryftOperator) throws ElasticConversionException {
        DateFormat dateFormat = RyftExpressionDate.getDateFormat(format);
        DateFormat timeFormat = RyftExpressionTime.getTimeFormat(format);

        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            Date date = sdf.parse(searchText);

            if (dateFormat != null && timeFormat != null) {
                RyftQuery dateQuery = RyftQueryDateTimeUtil.buildSimpleDateQuery(date, format, ryftOperator,
                        fieldName, RyftExpressionRange.RyftOperatorCompare.EQ);

                RyftQuery timeQuery = RyftQueryDateTimeUtil.buildSimpleTimeQuery(date, format, ryftOperator,
                        fieldName, RyftExpressionRange.RyftOperatorCompare.EQ);

                List<RyftQuery> queries = Arrays.asList(dateQuery, timeQuery);

                return buildComplexQuery(RyftLogicalOperator.AND, queries);
            } else if (dateFormat == null && timeFormat != null) {
                return RyftQueryDateTimeUtil.buildSimpleTimeQuery(date, format, ryftOperator,
                        fieldName, RyftExpressionRange.RyftOperatorCompare.EQ);
            } else if (timeFormat == null && dateFormat != null) {
                return RyftQueryDateTimeUtil.buildSimpleDateQuery(date, format, ryftOperator,
                        fieldName, RyftExpressionRange.RyftOperatorCompare.EQ);
            } else {
                throw new ElasticConversionException("Could not parse datetime format: " + format);
            }
        } catch (ParseException e) {
            throw new ElasticConversionException("Could not parse datetime format", e.getCause());
        }
    }

    private RyftQuery buildQueryDateTimeRange(Map<RyftExpressionRange.RyftOperatorCompare, String> lowerBound,
                                              Map<RyftExpressionRange.RyftOperatorCompare, String> upperBound,
                                              String format, String fieldName,
                                              RyftOperator ryftOperator) throws ElasticConversionException {
        DateFormat dateFormat = RyftExpressionDate.getDateFormat(format);
        DateFormat timeFormat = RyftExpressionTime.getTimeFormat(format);

        try {
            if (timeFormat == null && dateFormat != null) {
                return RyftQueryDateTimeUtil.buildSimpleRangeQuery(lowerBound, upperBound, format, ryftOperator, fieldName, false);
            } else if (dateFormat == null && timeFormat != null) {
                return RyftQueryDateTimeUtil.buildSimpleRangeQuery(lowerBound, upperBound, format, ryftOperator, fieldName, true);
            } else if (dateFormat != null && timeFormat != null) {
                return RyftQueryDateTimeUtil.buildFullRangeQuery(lowerBound, upperBound, format, ryftOperator, fieldName);
            }
            throw new ElasticConversionException("Could not parse datetime format: " + format);
        } catch (ParseException e) {
            throw new ElasticConversionException("Could not parse datetime format", e.getCause());
        }
    }

    private RyftQuery buildQueryNumericTerm(String searchText,
                                            String fieldName,
                                            String separator,
                                            String decimal,
                                            RyftOperator ryftOperator) {
        RyftExpression ryftExpression = new RyftExpressionNumeric(searchText,
                RyftExpressionRange.RyftOperatorCompare.EQ,
                separator,
                decimal);
        return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
    }

    private RyftQuery buildQueryNumericArrayTerm(List<String> searchArray,
                                                 String fieldName,
                                                 String separator,
                                                 String decimal,
                                                 RyftOperator ryftOperator) {
        List<RyftQuery> queries = new ArrayList<>();
        searchArray.forEach(searchText -> {
            RyftExpression ryftExpression = new RyftExpressionNumeric(searchText,
                    RyftExpressionRange.RyftOperatorCompare.EQ,
                    separator,
                    decimal);
            queries.add(new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression));
        });

        return buildComplexQuery(RyftLogicalOperator.OR, queries);
    }

    private RyftQuery buildQueryNumericRange(Map<RyftExpressionRange.RyftOperatorCompare, String> lowerBound,
                                             Map<RyftExpressionRange.RyftOperatorCompare, String> upperBound,
                                             String fieldName,
                                             String separator,
                                             String decimal,
                                             RyftOperator ryftOperator) throws ElasticConversionException {
        Optional<RyftExpressionRange.RyftOperatorCompare> operatorCompareLower = Optional.empty();
        if (lowerBound != null) {
            operatorCompareLower = lowerBound.keySet().stream().findFirst();
        }
        Optional<RyftExpressionRange.RyftOperatorCompare> operatorCompareUpper = Optional.empty();
        if (upperBound != null) {
            operatorCompareUpper = upperBound.keySet().stream().findFirst();
        }

        if (operatorCompareLower.isPresent() && operatorCompareUpper.isPresent()) {
            String numberLower = lowerBound.get(operatorCompareLower.get());
            String numberUpper = upperBound.get(operatorCompareUpper.get());

            //Resulting expression is of format "a < x < b". So, we must reverse operatorCompareLower in order for the comparison logic to be preserved
            RyftExpression ryftExpression = new RyftExpressionNumeric(numberLower,
                    RyftExpressionRange.RyftOperatorCompare.getOppositeValue(operatorCompareLower.get()),
                    operatorCompareUpper.get(), numberUpper, separator, decimal);

            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
        } else if (operatorCompareLower.isPresent() && !operatorCompareUpper.isPresent()) {
            String number = lowerBound.get(operatorCompareLower.get());
            RyftExpression ryftExpression = new RyftExpressionNumeric(number, operatorCompareLower.get(), separator, decimal);
            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
        } else if (operatorCompareUpper.isPresent() && !operatorCompareLower.isPresent()) {
            String number = upperBound.get(operatorCompareUpper.get());
            RyftExpression ryftExpression = new RyftExpressionNumeric(number, operatorCompareUpper.get(), separator, decimal);
            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
        } else {
            throw new ElasticConversionException("Range query must have either an upper bound, a lower bound, or both");
        }
    }

    private RyftQuery buildQueryCurrencyTerm(String searchText,
                                             String fieldName,
                                             String currency,
                                             String separator,
                                             String decimal,
                                             RyftOperator ryftOperator) {
        RyftExpression ryftExpression = new RyftExpressionCurrency(searchText,
                RyftExpressionRange.RyftOperatorCompare.EQ,
                currency,
                separator,
                decimal);
        return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
    }

    private RyftQuery buildQueryCurrencyRange(Map<RyftExpressionRange.RyftOperatorCompare, String> lowerBound,
                                              Map<RyftExpressionRange.RyftOperatorCompare, String> upperBound,
                                              String fieldName,
                                              String currency,
                                              String separator,
                                              String decimal,
                                              RyftOperator ryftOperator) throws ElasticConversionException {
        Optional<RyftExpressionRange.RyftOperatorCompare> operatorCompareLower = Optional.empty();
        if (lowerBound != null) {
            operatorCompareLower = lowerBound.keySet().stream().findFirst();
        }
        Optional<RyftExpressionRange.RyftOperatorCompare> operatorCompareUpper = Optional.empty();
        if (upperBound != null) {
            operatorCompareUpper = upperBound.keySet().stream().findFirst();
        }

        if (operatorCompareLower.isPresent() && operatorCompareUpper.isPresent()) {
            String valueLower = lowerBound.get(operatorCompareLower.get());
            String valueUpper = upperBound.get(operatorCompareUpper.get());

            //Resulting expression is of format "a < x < b". So, we must reverse operatorCompareLower in order for the comparison logic to be preserved
            RyftExpression ryftExpression = new RyftExpressionCurrency(valueLower,
                    RyftExpressionRange.RyftOperatorCompare.getOppositeValue(operatorCompareLower.get()),
                    operatorCompareUpper.get(), valueUpper, currency, separator, decimal);

            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
        } else if (operatorCompareLower.isPresent() && !operatorCompareUpper.isPresent()) {
            String value = lowerBound.get(operatorCompareLower.get());
            RyftExpression ryftExpression = new RyftExpressionCurrency(value, operatorCompareLower.get(), currency, separator, decimal);
            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
        } else if (operatorCompareUpper.isPresent() && !operatorCompareLower.isPresent()) {
            String value = upperBound.get(operatorCompareUpper.get());
            RyftExpression ryftExpression = new RyftExpressionCurrency(value, operatorCompareUpper.get(), currency, separator, decimal);
            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
        } else {
            throw new ElasticConversionException("Range query must have either an upper bound, a lower bound, or both");
        }
    }

    private RyftQuery buildQueryIpv4Term(String searchText,
                                         String fieldName,
                                         RyftOperator ryftOperator) throws ElasticConversionException {
        String[] parts = searchText.split("/");
        if (parts.length == 2) {
            SubnetUtils network = new SubnetUtils(searchText);
            network.setInclusiveHostCount(true);
            String lowerIp = network.getInfo().getLowAddress();
            String upperIp = network.getInfo().getHighAddress();
            Map<RyftExpressionRange.RyftOperatorCompare, String> lowerBound =
                    Collections.singletonMap(RyftExpressionRange.RyftOperatorCompare.GTE, lowerIp);
            Map<RyftExpressionRange.RyftOperatorCompare, String> upperBound =
                    Collections.singletonMap(RyftExpressionRange.RyftOperatorCompare.LTE, upperIp);
            return buildQueryIpv4Range(lowerBound, upperBound, fieldName, ryftOperator);
        }

        RyftExpression ryftExpression = new RyftExpressionIPv4(searchText, RyftExpressionRange.RyftOperatorCompare.EQ);
        return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
    }

    private RyftQuery buildQueryIpv4Range(Map<RyftExpressionRange.RyftOperatorCompare, String> lowerBound,
                                          Map<RyftExpressionRange.RyftOperatorCompare, String> upperBound,
                                          String fieldName,
                                          RyftOperator ryftOperator) throws ElasticConversionException {
        Optional<RyftExpressionRange.RyftOperatorCompare> operatorCompareLower = Optional.empty();
        if (lowerBound != null) {
            operatorCompareLower = lowerBound.keySet().stream().findFirst();
        }
        Optional<RyftExpressionRange.RyftOperatorCompare> operatorCompareUpper = Optional.empty();
        if (upperBound != null) {
            operatorCompareUpper = upperBound.keySet().stream().findFirst();
        }

        if (operatorCompareLower.isPresent() && operatorCompareUpper.isPresent()) {
            String ipLower = lowerBound.get(operatorCompareLower.get());
            String ipUpper = upperBound.get(operatorCompareUpper.get());

            //Resulting expression is of format "a < x < b". So, we must reverse operatorCompareLower in order for the comparison logic to be preserved
            RyftExpression ryftExpression = new RyftExpressionIPv4(ipLower,
                    RyftExpressionRange.RyftOperatorCompare.getOppositeValue(operatorCompareLower.get()),
                    operatorCompareUpper.get(), ipUpper);

            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
        } else if (operatorCompareLower.isPresent() && !operatorCompareUpper.isPresent()) {
            String ip = lowerBound.get(operatorCompareLower.get());
            RyftExpression ryftExpression = new RyftExpressionIPv4(ip, operatorCompareLower.get());
            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
        } else if (operatorCompareUpper.isPresent() && !operatorCompareLower.isPresent()) {
            String ip = upperBound.get(operatorCompareUpper.get());
            RyftExpression ryftExpression = new RyftExpressionIPv4(ip, operatorCompareUpper.get());
            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
        } else {
            throw new ElasticConversionException("Range query must have either an upper bound, a lower bound, or both");
        }
    }

    private RyftQuery buildQueryIpv6Term(String searchText,
                                         String fieldName,
                                         RyftOperator ryftOperator) throws ElasticConversionException {
        String[] parts = searchText.split("/");
        if (parts.length == 2) {
            IPv6Network network = IPv6Network.fromString(searchText);

            String lowerIp = network.getFirst().toString();
            String upperIp = network.getLast().toString();
            Map<RyftExpressionRange.RyftOperatorCompare, String> lowerBound =
                    Collections.singletonMap(RyftExpressionRange.RyftOperatorCompare.GTE, lowerIp);
            Map<RyftExpressionRange.RyftOperatorCompare, String> upperBound =
                    Collections.singletonMap(RyftExpressionRange.RyftOperatorCompare.LTE, upperIp);
            return buildQueryIpv6Range(lowerBound, upperBound, fieldName, ryftOperator);
        }

        RyftExpression ryftExpression = new RyftExpressionIPv6(searchText, RyftExpressionRange.RyftOperatorCompare.EQ);
        return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
    }

    private RyftQuery buildQueryIpv6Range(Map<RyftExpressionRange.RyftOperatorCompare, String> lowerBound,
                                          Map<RyftExpressionRange.RyftOperatorCompare, String> upperBound,
                                          String fieldName,
                                          RyftOperator ryftOperator) throws ElasticConversionException {
        Optional<RyftExpressionRange.RyftOperatorCompare> operatorCompareLower = Optional.empty();
        if (lowerBound != null) {
            operatorCompareLower = lowerBound.keySet().stream().findFirst();
        }
        Optional<RyftExpressionRange.RyftOperatorCompare> operatorCompareUpper = Optional.empty();
        if (upperBound != null) {
            operatorCompareUpper = upperBound.keySet().stream().findFirst();
        }

        if (operatorCompareLower.isPresent() && operatorCompareUpper.isPresent()) {
            String ipLower = lowerBound.get(operatorCompareLower.get());
            String ipUpper = upperBound.get(operatorCompareUpper.get());

            //Resulting expression is of format "a < x < b". So, we must reverse operatorCompareLower in order for the comparison logic to be preserved
            RyftExpression ryftExpression = new RyftExpressionIPv6(ipLower,
                    RyftExpressionRange.RyftOperatorCompare.getOppositeValue(operatorCompareLower.get()),
                    operatorCompareUpper.get(), ipUpper);

            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
        } else if (operatorCompareLower.isPresent() && !operatorCompareUpper.isPresent()) {
            String ip = lowerBound.get(operatorCompareLower.get());
            RyftExpression ryftExpression = new RyftExpressionIPv6(ip, operatorCompareLower.get());
            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
        } else if (operatorCompareUpper.isPresent() && !operatorCompareLower.isPresent()) {
            String ip = upperBound.get(operatorCompareUpper.get());
            RyftExpression ryftExpression = new RyftExpressionIPv6(ip, operatorCompareUpper.get());
            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName), ryftOperator, ryftExpression);
        } else {
            throw new ElasticConversionException("Range query must have either an upper bound, a lower bound, or both");
        }
    }

    private RyftQuery buildQueryFuzzy(String searchText, String fieldName,
                                      RyftFuzzyMetric metric, Integer fuzziness, RyftOperator ryftOperator,
                                      Integer width, Boolean line) {
        String splitSearchText = tokenize(searchText).stream()
                .collect(Collectors.joining());
        return buildQueryMatchPhrase(splitSearchText, fieldName, metric, fuzziness, ryftOperator, width, line);
    }

    public RyftQuery buildComplexQuery(RyftLogicalOperator operator, Collection<RyftQuery> operands) {
        return new RyftQueryComplex(operator, operands);
    }

    private Integer getFuzzinessAuto(String searchText) {
        Integer textLength = searchText.length();
        if (textLength < TEXT_LENGTH_NO_FUZZINESS) {
            return 0;
        } else if (textLength <= TEXT_LENGTH_FUZZINESS) {
            return 1;
        } else {
            return 2;
        }
    }

    private Integer adjustFuzziness(Integer fuzziness, String searchText) {
        if (searchText.length() == 1 && fuzziness > 0) {
            return 0;
        }
        Double fuzzy = Math.ceil((double) searchText.length() / 2);
        return fuzzy.intValue() >= fuzziness ? fuzziness : fuzzy.intValue();
    }

    private Collection<String> tokenize(String searchText) {
        Collection<String> result = new ArrayList<>();
        try (Tokenizer tokenizer = new WhitespaceTokenizer()) {
            tokenizer.setReader(new StringReader(searchText));
            CharTermAttribute charTermAttrib = tokenizer.getAttribute(CharTermAttribute.class);
            tokenizer.reset();
            while (tokenizer.incrementToken()) {
                String token = charTermAttrib.toString();
                if (!result.contains(token)) {
                    result.add(token);
                }
            }
            tokenizer.end();
        } catch (IOException ex) {
            LOGGER.error("Tokenization error.", ex);
        }
        return result;
    }
}
