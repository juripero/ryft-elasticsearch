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
package org.elasticsearch.search.aggregations.metrics.sum;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.List;
import java.util.Map;

//FIXME - hack due to the fact that InternalSum has package-private constructor, check after switch to ES 6+
public class RyftInternalSum extends InternalNumericMetricsAggregation.SingleValue implements Sum {

    public final static Type TYPE = new Type("sum");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalSum readResult(StreamInput in) throws IOException {
            InternalSum result = new InternalSum();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private double sum;

    RyftInternalSum() {} // for serialization

    public RyftInternalSum(String name, double sum, ValueFormatter formatter, List<PipelineAggregator> pipelineAggregators,
                Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.sum = sum;
        this.valueFormatter = formatter;
    }

    @Override
    public double value() {
        return sum;
    }

    @Override
    public double getValue() {
        return sum;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public RyftInternalSum doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        double sum = 0;
        for (InternalAggregation aggregation : aggregations) {
            sum += ((RyftInternalSum) aggregation).sum;
        }
        return new RyftInternalSum(name, sum, valueFormatter, pipelineAggregators(), getMetaData());
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        valueFormatter = ValueFormatterStreams.readOptional(in);
        sum = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeDouble(sum);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE, sum);
        if (!(valueFormatter instanceof ValueFormatter.Raw)) {
            builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(sum));
        }
        return builder;
    }

}
