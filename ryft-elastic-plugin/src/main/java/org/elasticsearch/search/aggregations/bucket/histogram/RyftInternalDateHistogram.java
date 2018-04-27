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
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class RyftInternalDateHistogram {

    final static InternalAggregation.Type TYPE = new InternalAggregation.Type("date_histogram", "dhisto");

    public static class Bucket extends RyftInternalHistogram.Bucket {

        Bucket(boolean keyed, ValueFormatter formatter, RyftInternalHistogram.Factory<RyftInternalDateHistogram.Bucket> factory) {
            super(keyed, formatter, factory);
        }

        Bucket(long key, long docCount, InternalAggregations aggregations, boolean keyed, ValueFormatter formatter,
               RyftInternalHistogram.Factory<RyftInternalDateHistogram.Bucket> factory) {
            super(key, docCount, keyed, formatter, factory, aggregations);
        }

        @Override
        public String getKeyAsString() {
            return formatter != null ? formatter.format(key) : ValueFormatter.DateTime.DEFAULT.format(key);
        }

        @Override
        public DateTime getKey() {
            return new DateTime(key, DateTimeZone.UTC);
        }

        @Override
        public String toString() {
            return getKeyAsString();
        }
    }

    public static class Factory extends RyftInternalHistogram.Factory<RyftInternalDateHistogram.Bucket> {

        public Factory() {
        }

        @Override
        public String type() {
            return TYPE.name();
        }

        @Override
        public RyftInternalDateHistogram.Bucket createBucket(InternalAggregations aggregations, RyftInternalDateHistogram.Bucket prototype) {
            return new RyftInternalDateHistogram.Bucket(prototype.key, prototype.docCount, aggregations, prototype.getKeyed(), prototype.formatter, this);
        }

        @Override
        public RyftInternalDateHistogram.Bucket createBucket(Object key, long docCount, InternalAggregations aggregations, boolean keyed,
                                                         ValueFormatter formatter) {
            if (key instanceof Number) {
                return new RyftInternalDateHistogram.Bucket(((Number) key).longValue(), docCount, aggregations, keyed, formatter, this);
            } else if (key instanceof DateTime) {
                return new RyftInternalDateHistogram.Bucket(((DateTime) key).getMillis(), docCount, aggregations, keyed, formatter, this);
            } else {
                throw new AggregationExecutionException("Expected key of type Number or DateTime but got [" + key + "]");
            }
        }

        @Override
        protected RyftInternalDateHistogram.Bucket createEmptyBucket(boolean keyed, ValueFormatter formatter) {
            return new RyftInternalDateHistogram.Bucket(keyed, formatter, this);
        }
    }

    private RyftInternalDateHistogram() {}
}
