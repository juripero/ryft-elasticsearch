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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.io.IOException;
import java.util.Comparator;

public class RyftInternalOrder extends RyftHistogram.Order {

    final byte id;
    final String key;
    final boolean asc;
    final Comparator<RyftInternalHistogram.Bucket> comparator;

    RyftInternalOrder(byte id, String key, boolean asc, Comparator<RyftInternalHistogram.Bucket> comparator) {
        this.id = id;
        this.key = key;
        this.asc = asc;
        this.comparator = comparator;
    }

    byte id() {
        return id;
    }

    String key() {
        return key;
    }

    boolean asc() {
        return asc;
    }

    @Override
    Comparator<RyftInternalHistogram.Bucket> comparator() {
        return comparator;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(key, asc ? "asc" : "desc").endObject();
    }

    static class Aggregation extends RyftInternalOrder {

        static final byte ID = 0;

        Aggregation(String key, boolean asc) {
            super(ID, key, asc, new MultiBucketsAggregation.Bucket.SubAggregationComparator<RyftInternalHistogram.Bucket>(key, asc));
        }

        private static String key(String aggName, String valueName) {
            return (valueName == null) ? aggName : aggName + "." + valueName;
        }

    }

    static class Streams {

        /**
         * Writes the given order to the given output (based on the id of the order).
         */
        public static void writeOrder(RyftInternalOrder order, StreamOutput out) throws IOException {
            out.writeByte(order.id());
            if (order instanceof RyftInternalOrder.Aggregation) {
                out.writeBoolean(order.asc());
                out.writeString(order.key());
            }
        }

        public static RyftInternalOrder readOrder(StreamInput in) throws IOException {
            byte id = in.readByte();
            switch (id) {
                case 1: return (RyftInternalOrder) RyftHistogram.Order.KEY_ASC;
                case 2: return (RyftInternalOrder) RyftHistogram.Order.KEY_DESC;
                case 3: return (RyftInternalOrder) RyftHistogram.Order.COUNT_ASC;
                case 4: return (RyftInternalOrder) RyftHistogram.Order.COUNT_DESC;
                case 0:
                    boolean asc = in.readBoolean();
                    String key = in.readString();
                    return new RyftInternalOrder.Aggregation(key, asc);
                default:
                    throw new RuntimeException("unknown histogram order");
            }
        }

    }


}
