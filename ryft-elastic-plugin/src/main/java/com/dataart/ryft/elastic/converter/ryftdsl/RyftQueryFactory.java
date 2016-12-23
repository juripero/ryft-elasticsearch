package com.dataart.ryft.elastic.converter.ryftdsl;

import com.dataart.ryft.elastic.converter.ElasticConversionException;
import com.dataart.ryft.elastic.converter.entities.FuzzyQueryParameters;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryComplex.RyftLogicalOperator;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
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
                        fuzzyQueryParameters.getRyftOperator());
            case MATCH:
                return buildQueryMatch(
                        fuzzyQueryParameters.getSearchValue(),
                        fuzzyQueryParameters.getFieldName(),
                        fuzzyQueryParameters.getOperator(),
                        fuzzyQueryParameters.getMetric(),
                        fuzzyQueryParameters.getFuzziness(),
                        fuzzyQueryParameters.getRyftOperator());
            case MATCH_PHRASE:
                return buildQueryMatchPhrase(
                        fuzzyQueryParameters.getSearchValue(),
                        fuzzyQueryParameters.getFieldName(),
                        fuzzyQueryParameters.getMetric(),
                        fuzzyQueryParameters.getFuzziness(),
                        fuzzyQueryParameters.getRyftOperator());
            case WILDCARD:
                return buildQueryWildcard(
                        fuzzyQueryParameters.getSearchValue(),
                        fuzzyQueryParameters.getFieldName(),
                        fuzzyQueryParameters.getRyftOperator());
            default:
                throw new ElasticConversionException("Unknown search type");
        }
    }

    private RyftQuery buildQueryMatchPhrase(String searchText, String fieldName,
                                            RyftFuzzyMetric metric, Integer fuzziness, RyftOperator ryftOperator) {
        RyftExpression ryftExpression;
        if (FUZZYNESS_AUTO_VALUE.equals(fuzziness)) {
            fuzziness = getFuzzinessAuto(searchText);
        }
        if (fuzziness == 0) {
            ryftExpression = new RyftExpressionExactSearch(searchText);
        } else {
            fuzziness = adjustFuzziness(fuzziness, searchText);
            ryftExpression = new RyftExpressionFuzzySearch(searchText, metric, fuzziness);
        }
        return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName),
                ryftOperator, ryftExpression);
    }

    private RyftQuery buildQueryMatch(String searchText, String fieldName,
                                      RyftLogicalOperator operator, RyftFuzzyMetric metric, Integer fuzziness,
                                      RyftOperator ryftOperator) {
        Collection<RyftQuery> operands = tokenize(searchText).stream()
                .map(searchToken -> buildQueryMatchPhrase(searchToken, fieldName, metric, fuzziness, ryftOperator))
                .collect(Collectors.toList());
        return buildComplexQuery(operator, operands);
    }

    private RyftQuery buildQueryWildcard(String searchText, String fieldName,
                                         RyftOperator ryftOperator) {
        String searchTextFormatted = searchText.replace("?", "\"?\"");
        RyftExpression ryftExpression = new RyftExpressionExactSearch(searchTextFormatted);
        return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName),
                ryftOperator, ryftExpression);
    }

    private RyftQuery buildQueryFuzzy(String searchText, String fieldName,
                                      RyftFuzzyMetric metric, Integer fuzziness, RyftOperator ryftOperator) {
        String splitSearchText = tokenize(searchText).stream()
                .collect(Collectors.joining());
        return buildQueryMatchPhrase(splitSearchText, fieldName, metric, fuzziness, ryftOperator);
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
