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

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.Comparator;
import java.util.List;

public interface RyftHistogram extends MultiBucketsAggregation {

    /**
     * A bucket in the histogram where documents fall in
     */
    static interface Bucket extends MultiBucketsAggregation.Bucket {

    }

    /**
     * @return  The buckets of this histogram (each bucket representing an interval in the histogram)
     */
    @Override
    List<? extends RyftHistogram.Bucket> getBuckets();


    /**
     * A strategy defining the order in which the buckets in this histogram are ordered.
     */
    static abstract class Order implements ToXContent {

        public static final RyftHistogram.Order KEY_ASC = new RyftInternalOrder((byte) 1, "_key", true, new Comparator<RyftInternalHistogram.Bucket>() {
            @Override
            public int compare(RyftInternalHistogram.Bucket b1, RyftInternalHistogram.Bucket b2) {
                return Long.compare(b1.key, b2.key);
            }
        });

        public static final RyftHistogram.Order KEY_DESC = new RyftInternalOrder((byte) 2, "_key", false, new Comparator<RyftInternalHistogram.Bucket>() {
            @Override
            public int compare(RyftInternalHistogram.Bucket b1, RyftInternalHistogram.Bucket b2) {
                return -Long.compare(b1.key, b2.key);
            }
        });

        public static final RyftHistogram.Order COUNT_ASC = new RyftInternalOrder((byte) 3, "_count", true, new Comparator<RyftInternalHistogram.Bucket>() {
            @Override
            public int compare(RyftInternalHistogram.Bucket b1, RyftInternalHistogram.Bucket b2) {
                int cmp = Long.compare(b1.getDocCount(), b2.getDocCount());
                if (cmp == 0) {
                    cmp = Long.compare(b1.key, b2.key);
                }
                return cmp;
            }
        });


        public static final RyftHistogram.Order COUNT_DESC = new RyftInternalOrder((byte) 4, "_count", false, new Comparator<RyftInternalHistogram.Bucket>() {
            @Override
            public int compare(RyftInternalHistogram.Bucket b1, RyftInternalHistogram.Bucket b2) {
                int cmp = -Long.compare(b1.getDocCount(), b2.getDocCount());
                if (cmp == 0) {
                    cmp = Long.compare(b1.key, b2.key);
                }
                return cmp;
            }
        });

        /**
         * Creates a bucket ordering strategy that sorts buckets based on a single-valued calc sug-aggregation
         *
         * @param path the name of the aggregation
         * @param asc             The direction of the order (ascending or descending)
         */
        public static RyftHistogram.Order aggregation(String path, boolean asc) {
            return new RyftInternalOrder.Aggregation(path, asc);
        }

        /**
         * Creates a bucket ordering strategy that sorts buckets based on a multi-valued calc sug-aggregation
         *
         * @param aggregationName the name of the aggregation
         * @param valueName       The name of the value of the multi-value get by which the sorting will be applied
         * @param asc             The direction of the order (ascending or descending)
         */
        public static RyftHistogram.Order aggregation(String aggregationName, String valueName, boolean asc) {
            return new RyftInternalOrder.Aggregation(aggregationName + "." + valueName, asc);
        }

        /**
         * @return The bucket comparator by which the order will be applied.
         */
        abstract Comparator<RyftInternalHistogram.Bucket> comparator();

    }
}

