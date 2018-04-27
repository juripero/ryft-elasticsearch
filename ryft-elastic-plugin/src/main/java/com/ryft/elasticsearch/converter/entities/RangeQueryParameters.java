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
import com.ryft.elasticsearch.converter.ryftdsl.RyftExpressionRange;

import java.util.List;
import java.util.Map;

/**
 * Contains parsed query parameters that are required to create a ryft query, for cases where the
 * {@link ElasticConvertingContext.ElasticSearchType} is "range"
 * Used by {@link com.ryft.elasticsearch.converter.ryftdsl.RyftQueryFactory}
 */
public class RangeQueryParameters extends QueryParameters<String> {

    private ElasticConvertingContext.ElasticDataType dataType = ElasticConvertingContext.ElasticDataType.NUMBER;
    private String format = "yyyy-MM-dd HH:mm:ss";
    private Map<RyftExpressionRange.RyftOperatorCompare, String> lowerBound;
    private Map<RyftExpressionRange.RyftOperatorCompare, String> upperBound;
    private List<String> searchArray;
    private String separator = ",";
    private String decimal = ".";
    private String currency = "$";
    protected Integer width;
    protected Boolean line;

    public ElasticConvertingContext.ElasticDataType getDataType() {
        return dataType;
    }

    public void setDataType(ElasticConvertingContext.ElasticDataType dataType) {
        this.dataType = dataType;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public Map<RyftExpressionRange.RyftOperatorCompare, String> getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(Map<RyftExpressionRange.RyftOperatorCompare, String> lowerBound) {
        this.lowerBound = lowerBound;
    }

    public Map<RyftExpressionRange.RyftOperatorCompare, String> getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(Map<RyftExpressionRange.RyftOperatorCompare, String> upperBound) {
        this.upperBound = upperBound;
    }

    public List<String> getSearchArray() {
        return searchArray;
    }

    public void setSearchArray(List<String> searchArray) {
        this.searchArray = searchArray;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public String getDecimal() {
        return decimal;
    }

    public void setDecimal(String decimal) {
        this.decimal = decimal;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public Integer getWidth() {
        return width;
    }

    public void setWidth(Integer width) {
        this.width = width;
    }

    public Boolean getLine() {
        return line;
    }

    public void setLine(Boolean line) {
        this.line = line;
    }

    @Override
    public void check() throws ElasticConversionException {
        if (dataType == null) {
            throw new ElasticConversionException("Data type should be defined.");
        }

        if (lowerBound == null && upperBound == null) {
            throw new ElasticConversionException("Range must have either upper bound, lower bound, or both");
        }
    }
}
