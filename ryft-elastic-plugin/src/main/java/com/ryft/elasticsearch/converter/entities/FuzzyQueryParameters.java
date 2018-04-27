/**
 * ##License
 * Ryft-Customized BSD License
 * Copyright (c) 2018, Ryft Systems, Inc.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
 *   This product includes software developed by Ryft Systems, Inc.
 * 4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
 *   to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.ryft.elasticsearch.converter.entities;

import com.ryft.elasticsearch.converter.ElasticConversionException;
import com.ryft.elasticsearch.converter.ElasticConvertingContext;
import com.ryft.elasticsearch.converter.ryftdsl.RyftExpressionFuzzySearch.RyftFuzzyMetric;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQueryComplex;
import static com.ryft.elasticsearch.converter.ryftdsl.RyftQueryFactory.FUZZYNESS_AUTO_VALUE;

/**
 * Contains parsed query parameters that are required to create a ryft query
 * Used by {@link com.ryft.elasticsearch.converter.ryftdsl.RyftQueryFactory}
 */
public class FuzzyQueryParameters extends QueryParameters<String> {

    public static RyftFuzzyMetric METRIC_DEFAULT = RyftFuzzyMetric.FEDS;
    public static Integer FUZZINESS_DEFAULT = FUZZYNESS_AUTO_VALUE;
    
    protected RyftFuzzyMetric metric = METRIC_DEFAULT;
    protected Integer fuzziness = FUZZINESS_DEFAULT;
    protected RyftQueryComplex.RyftLogicalOperator operator = RyftQueryComplex.RyftLogicalOperator.OR;
    protected ElasticConvertingContext.ElasticSearchType searchType = ElasticConvertingContext.ElasticSearchType.MATCH_PHRASE;
    protected Integer width;
    protected Boolean line;

    public void setMetric(RyftFuzzyMetric metric) {
        this.metric = metric;
    }

    public void setFuzziness(Integer fuzziness) {
        this.fuzziness = fuzziness;
    }

    public void setOperator(RyftQueryComplex.RyftLogicalOperator operator) {
        this.operator = operator;
    }

    public void setSearchType(ElasticConvertingContext.ElasticSearchType searchType) {
        this.searchType = searchType;
    }
    public void setWidth(Integer width) {
        this.width = width;
    }

    public void setLine(Boolean line) {
        this.line = line;
    }

    public RyftFuzzyMetric getMetric() {
        return metric;
    }

    public Integer getFuzziness() {
        return fuzziness;
    }

    public RyftQueryComplex.RyftLogicalOperator getOperator() {
        return operator;
    }

    public ElasticConvertingContext.ElasticSearchType getSearchType() {
        return searchType;
    }

    public Integer getWidth() {
        return width;
    }

    public Boolean getLine() {
        return line;
    }

    @Override
    public void check() throws ElasticConversionException {
        super.check();
        if (searchType == null) {
            throw new ElasticConversionException("Search type should be defined.");
        }
        if ((operator == null) && (ElasticConvertingContext.ElasticSearchType.MATCH.equals(searchType))) {
            throw new ElasticConversionException("Logical operator for match search should be defined.");
        }
        if (fuzziness == null) {
            throw new ElasticConversionException("Fuzziness should be defined.");
        }
        if (metric == null) {
            throw new ElasticConversionException("Metric should be defined.");
        }
    }
}
