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

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

/**
 * Contains converters that are shared between {@link ElasticConverterField} and {@link ElasticConverterRangeField}
 */
public class ElasticConverterShared {

    private final static ESLogger LOGGER = Loggers.getLogger(ElasticConverterField.class);

    public static class ElasticConverterValue implements ElasticConvertingElement<String> {

        public static final String NAME = "value";

        public static final String NAME_ALTERNATIVE = "query";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }

    public static class ElasticConverterDateFormat implements ElasticConvertingElement<String> {

        public static final String NAME = "datetime_format";
        public static final String NAME_ALTERNATIVE = "format";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            String format = ElasticConversionUtil.getString(convertingContext);

            if (format.equals("epoch_millis")) {
                // Epoch millis in requests is sent mainly by Kibana for timeseries datasets
                // We have to explicitly set data type here because we cannot force Kibana to specify it
                convertingContext.setDataType(ElasticConvertingContext.ElasticDataType.DATETIME);
            }

            return format;
        }
    }

    public static class ElasticConverterType implements ElasticConvertingElement<Void> {

        public static final String NAME = "type";

        private final String TYPE_PHRASE = "phrase";
        private final String TYPE_DATETIME = "datetime";
        private final String TYPE_NUMBER = "number";
        private final String TYPE_CURRENCY = "currency";
        private final String TYPE_IPV4 = "ipv4";
        private final String TYPE_IPV6 = "ipv6";

        @Override
        public Void convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
        LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            String type = ElasticConversionUtil.getString(convertingContext);

            if (TYPE_PHRASE.equals(type.toLowerCase())
                    && ElasticConvertingContext.ElasticSearchType.MATCH.equals(convertingContext.getSearchType())) {
                convertingContext.setSearchType(ElasticConvertingContext.ElasticSearchType.MATCH_PHRASE);
            } else {
                switch (type.toLowerCase()) {
                    case TYPE_DATETIME:
                        convertingContext.setDataType(ElasticConvertingContext.ElasticDataType.DATETIME);
                        break;
                    case TYPE_NUMBER:
                        convertingContext.setDataType(ElasticConvertingContext.ElasticDataType.NUMBER);
                        break;
                    case TYPE_CURRENCY:
                        convertingContext.setDataType(ElasticConvertingContext.ElasticDataType.CURRENCY);
                        break;
                    case TYPE_IPV4:
                        convertingContext.setDataType(ElasticConvertingContext.ElasticDataType.IPV4);
                        break;
                    case TYPE_IPV6:
                        convertingContext.setDataType(ElasticConvertingContext.ElasticDataType.IPV6);
                        break;
                }
            }
            return null;

        }
    }

    public static class ElasticConverterSeparator implements ElasticConvertingElement<String> {

        public static final String NAME = "separator";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }

    public static class ElasticConverterDecimal implements ElasticConvertingElement<String> {

        public static final String NAME = "decimal";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }

    public static class ElasticConverterCurrency implements ElasticConvertingElement<String> {

        public static final String NAME = "currency";

        @Override
        public String convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            return ElasticConversionUtil.getString(convertingContext);
        }
    }

    public static class ElasticConverterWidth implements ElasticConvertingElement<Void> {

        public static final String NAME = "width";

        @Override
        public Void convert(ElasticConvertingContext convertingContext) throws ElasticConversionException {
            LOGGER.debug(String.format("Start \"%s\" parsing", NAME));
            Object width = ElasticConversionUtil.getObject(convertingContext);
            if (width instanceof String && width.equals("line")) {
                convertingContext.setLine(true);
            } else if (width instanceof Integer) {
                convertingContext.setWidth((Integer) width);
            }
            return null;
        }
    }
}
