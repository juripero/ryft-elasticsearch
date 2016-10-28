package com.dataart.ryft.elastic.converter.ryftdsl;

import com.dataart.ryft.elastic.converter.ElasticConversionException;
import com.dataart.ryft.elastic.converter.entities.FuzzyQueryParameters;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQueryComplex.RyftLogicalOperator;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Tokenizer;
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
                return null;
            case MATCH:
                return null;
            case MATCH_PHRASE:
                return null;
            default:
                throw new ElasticConversionException("Unknown search type");
        }
    }

    public RyftQuery buildFuzzyQuery(String searchText, String fieldName, RyftFuzzyMetric metric, Integer fuzziness) {
        if ((fieldName != null) && (searchText != null) && (metric != null)) {
            RyftExpression ryftExpression;
            if ((fuzziness == null) || (fuzziness < FUZZYNESS_AUTO_VALUE)) {
                fuzziness = FUZZYNESS_AUTO_VALUE;
            }
            if (fuzziness.equals(FUZZYNESS_AUTO_VALUE)) {
                fuzziness = getFuzzinessAuto(searchText);
            }
            if (fuzziness == 0) {
                ryftExpression = new RyftExpressionExactSearch(searchText);
            } else {
                if (!isCorrectFuzziness(fuzziness, searchText)) {
                    LOGGER.warn("Invalid fyzziness {} for search text \"{}\".", fuzziness, searchText);
                    fuzziness = getFuzzinessAuto(searchText);
                    LOGGER.info("Fuzziness adjusted to {}.", fuzziness);
                }
                ryftExpression = new RyftExpressionFuzzySearch(searchText, metric, fuzziness);
            }
            return new RyftQuerySimple(new RyftInputSpecifierRecord(fieldName),
                    RyftOperator.CONTAINS, ryftExpression);
        } else {
            return null;
        }
    }

    public RyftQuery buildFuzzyQuery(String searchText, String fieldName, RyftFuzzyMetric metric) {
        return buildFuzzyQuery(searchText, fieldName, metric, null);
    }

    public RyftQuery buildFuzzyTokenizedQuery(String searchText, String fieldName,
            RyftLogicalOperator operator, RyftFuzzyMetric metric, Integer fuzziness) {
        Collection<RyftQuery> operands = tokenize(searchText).stream()
                .map(searchToken -> buildFuzzyQuery(searchToken, fieldName, metric, fuzziness))
                .collect(Collectors.toList());
        return buildComplexQuery(operator, operands);
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

    private Boolean isCorrectFuzziness(Integer fuzziness, String searchText) {
        return searchText.length() / 2 >= fuzziness;
    }

    private Collection<String> tokenize(String searchText) {
        Set<String> result = new HashSet<>();
        try (Tokenizer tokenizer = new StandardTokenizer()) {
            tokenizer.setReader(new StringReader(searchText));
            CharTermAttribute charTermAttrib = tokenizer.getAttribute(CharTermAttribute.class);
            tokenizer.reset();
            while (tokenizer.incrementToken()) {
                result.add(charTermAttrib.toString());
            }
            tokenizer.end();
        } catch (IOException ex) {
            LOGGER.error("Tokenization error.", ex);
        }
        return result;
    }
}
