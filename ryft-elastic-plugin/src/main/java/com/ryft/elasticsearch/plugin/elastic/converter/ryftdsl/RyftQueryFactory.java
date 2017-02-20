package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

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
        } else if (textLength < TEXT_LENGTH_FUZZINESS) {
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
