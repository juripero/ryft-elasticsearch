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
