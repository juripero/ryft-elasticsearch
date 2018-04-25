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
package com.ryft.elasticsearch.converter;

import com.ryft.elasticsearch.converter.entities.RangeQueryParameters;
import com.ryft.elasticsearch.converter.ryftdsl.RyftExpressionRange;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class ElasticConverterRangeField extends ElasticConverterField {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterRangeField.class);

    public static class ElasticConverterGreaterThanEquals implements ElasticConvertingElement<String> {

        public static final String NAME = "gte";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }

    public static class ElasticConverterGreaterThan implements ElasticConvertingElement<String> {

        public static final String NAME = "gt";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }

    public static class ElasticConverterLessThanEquals implements ElasticConvertingElement<String> {

        public static final String NAME = "lte";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }

    public static class ElasticConverterLessThan implements ElasticConvertingElement<String> {

        public static final String NAME = "lt";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }

    final static String NAME = "range_field";

    @Override
    protected RyftQuery getRyftQuery(ElasticConvertingContext convertingContext, Map<String, Object> fieldQueryMap) throws ElasticConversionException {
        try {
            RangeQueryParameters rangeQueryParameters = new RangeQueryParameters();
            rangeQueryParameters.setDataType(convertingContext.getDataType());

            String fieldName = convertingContext.getContentParser().currentName();
            rangeQueryParameters.setFieldName(fieldName);

            if (convertingContext.getLine() != null) {
                rangeQueryParameters.setLine(convertingContext.getLine());
            } else if (convertingContext.getWidth() != null) {
                rangeQueryParameters.setWidth(convertingContext.getWidth());
            }

            for (Map.Entry<String, Object> entry : fieldQueryMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                switch (key) {
                    case ElasticConverterShared.ElasticConverterValue.NAME:
                        rangeQueryParameters.setSearchValue((String) value);
                        break;
                    case ElasticConverterShared.ElasticConverterDateFormat.NAME:
                        rangeQueryParameters.setFormat((String) value);
                        break;
                    case ElasticConverterGreaterThanEquals.NAME:
                        rangeQueryParameters.setLowerBound(Collections.singletonMap(RyftExpressionRange.RyftOperatorCompare.GTE, (String) value));
                        break;
                    case ElasticConverterGreaterThan.NAME:
                        rangeQueryParameters.setLowerBound(Collections.singletonMap(RyftExpressionRange.RyftOperatorCompare.GT, (String) value));
                        break;
                    case ElasticConverterLessThanEquals.NAME:
                        rangeQueryParameters.setUpperBound(Collections.singletonMap(RyftExpressionRange.RyftOperatorCompare.LTE, (String) value));
                        break;
                    case ElasticConverterLessThan.NAME:
                        rangeQueryParameters.setUpperBound(Collections.singletonMap(RyftExpressionRange.RyftOperatorCompare.LT, (String) value));
                        break;
                    case ElasticConverterShared.ElasticConverterSeparator.NAME:
                        rangeQueryParameters.setSeparator((String) value);
                        break;
                    case ElasticConverterShared.ElasticConverterDecimal.NAME:
                        rangeQueryParameters.setDecimal((String) value);
                        break;
                    case ElasticConverterShared.ElasticConverterCurrency.NAME:
                        rangeQueryParameters.setCurrency((String) value);
                        break;
                }
            }
            //FIXME - workaround for timeseries
            if (convertingContext.getSearchArray() != null) {
                rangeQueryParameters.setSearchArray(convertingContext.getSearchArray());
            }
            return convertingContext.getQueryFactory().buildRangeQuery(rangeQueryParameters);
        } catch (IOException ex) {
            throw new ElasticConversionException("Request parsing error");
        }

    }
}
