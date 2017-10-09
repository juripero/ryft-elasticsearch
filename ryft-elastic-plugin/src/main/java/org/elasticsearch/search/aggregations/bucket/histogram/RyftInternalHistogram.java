package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.*;

public class RyftInternalHistogram<B extends RyftInternalHistogram.Bucket> extends InternalMultiBucketAggregation<RyftInternalHistogram, B> implements
        Histogram {

    final static Type TYPE = new Type("histogram", "histo");

    private final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public RyftInternalHistogram readResult(StreamInput in) throws IOException {
            RyftInternalHistogram histogram = new RyftInternalHistogram();
            histogram.readFrom(in);
            return histogram;
        }
    };

    private final static BucketStreams.Stream<RyftInternalHistogram.Bucket> BUCKET_STREAM = new BucketStreams.Stream<RyftInternalHistogram.Bucket>() {
        @Override
        public RyftInternalHistogram.Bucket readResult(StreamInput in, BucketStreamContext context) throws IOException {
            RyftInternalHistogram.Factory<?> factory = (RyftInternalHistogram.Factory<?>) context.attributes().get("factory");
            if (factory == null) {
                throw new IllegalStateException("No factory found for histogram buckets");
            }
            RyftInternalHistogram.Bucket histogram = new RyftInternalHistogram.Bucket(context.keyed(), context.formatter(), factory);
            histogram.readFrom(in);
            return histogram;
        }

        @Override
        public BucketStreamContext getBucketStreamContext(RyftInternalHistogram.Bucket bucket) {
            BucketStreamContext context = new BucketStreamContext();
            context.formatter(bucket.formatter);
            context.keyed(bucket.keyed);
            return context;
        }
    };

    public static void registerStream() {

        AggregationStreams.registerStream(STREAM, TYPE.stream());
        BucketStreams.registerStream(BUCKET_STREAM, TYPE.stream());
    }

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements Histogram.Bucket {

        long key;
        long docCount;
        InternalAggregations aggregations;
        private transient final boolean keyed;
        protected transient final ValueFormatter formatter;
        private RyftInternalHistogram.Factory<?> factory;

        public Bucket(boolean keyed, ValueFormatter formatter, RyftInternalHistogram.Factory<?> factory) {
            this.formatter = formatter;
            this.keyed = keyed;
            this.factory = factory;
        }

        public Bucket(long key, long docCount, boolean keyed, ValueFormatter formatter, RyftInternalHistogram.Factory factory,
                      InternalAggregations aggregations) {
            this(keyed, formatter, factory);
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        protected RyftInternalHistogram.Factory<?> getFactory() {
            return factory;
        }

        @Override
        public String getKeyAsString() {
            return formatter != null ? formatter.format(key) : ValueFormatter.RAW.format(key);
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        <B extends RyftInternalHistogram.Bucket> B reduce(List<B> buckets, ReduceContext context) {
            List<InternalAggregations> aggregations = new ArrayList<>(buckets.size());
            long docCount = 0;
            for (RyftInternalHistogram.Bucket bucket : buckets) {
                docCount += bucket.docCount;
                aggregations.add((InternalAggregations) bucket.getAggregations());
            }
            InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
            return (B) getFactory().createBucket(key, docCount, aggs, keyed, formatter);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (formatter != ValueFormatter.RAW) {
                Text keyTxt = new Text(formatter.format(key));
                if (keyed) {
                    builder.startObject(keyTxt.string());
                } else {
                    builder.startObject();
                }
                builder.field(CommonFields.KEY_AS_STRING, keyTxt);
            } else {
                if (keyed) {
                    builder.startObject(String.valueOf(getKey()));
                } else {
                    builder.startObject();
                }
            }
            builder.field(CommonFields.KEY, key);
            builder.field(CommonFields.DOC_COUNT, docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            key = in.readLong();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readAggregations(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(key);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        public ValueFormatter getFormatter() {
            return formatter;
        }

        public boolean getKeyed() {
            return keyed;
        }
    }

    public static class EmptyBucketInfo {

        final Rounding rounding;
        final InternalAggregations subAggregations;
        final ExtendedBounds bounds;

        EmptyBucketInfo(Rounding rounding, InternalAggregations subAggregations) {
            this(rounding, subAggregations, null);
        }

        EmptyBucketInfo(Rounding rounding, InternalAggregations subAggregations, ExtendedBounds bounds) {
            this.rounding = rounding;
            this.subAggregations = subAggregations;
            this.bounds = bounds;
        }

        public static RyftInternalHistogram.EmptyBucketInfo readFrom(StreamInput in) throws IOException {
            Rounding rounding = Rounding.Streams.read(in);
            InternalAggregations aggs = InternalAggregations.readAggregations(in);
            if (in.readBoolean()) {
                return new RyftInternalHistogram.EmptyBucketInfo(rounding, aggs, ExtendedBounds.readFrom(in));
            }
            return new RyftInternalHistogram.EmptyBucketInfo(rounding, aggs);
        }

        public static void writeTo(RyftInternalHistogram.EmptyBucketInfo info, StreamOutput out) throws IOException {
            Rounding.Streams.write(info.rounding, out);
            info.subAggregations.writeTo(out);
            out.writeBoolean(info.bounds != null);
            if (info.bounds != null) {
                info.bounds.writeTo(out);
            }
        }

    }

    public static class Factory<B extends RyftInternalHistogram.Bucket> {

        public Factory() {
        }

        public String type() {
            return TYPE.name();
        }

        public RyftInternalHistogram<B> create(String name, List<B> buckets, RyftInternalOrder order, long minDocCount,
                                           RyftInternalHistogram.EmptyBucketInfo emptyBucketInfo, ValueFormatter formatter, boolean keyed,
                                           List<PipelineAggregator> pipelineAggregators,
                                           Map<String, Object> metaData) {
            return new RyftInternalHistogram<>(name, buckets, order, minDocCount, emptyBucketInfo, formatter, keyed, this, pipelineAggregators,
                    metaData);
        }

        public RyftInternalHistogram<B> create(List<B> buckets, RyftInternalHistogram<B> prototype) {
            return new RyftInternalHistogram<>(prototype.name, buckets, prototype.order, prototype.minDocCount, prototype.emptyBucketInfo,
                    prototype.formatter, prototype.keyed, this, prototype.pipelineAggregators(), prototype.metaData);
        }

        public B createBucket(InternalAggregations aggregations, B prototype) {
            return (B) new RyftInternalHistogram.Bucket(prototype.key, prototype.docCount, prototype.getKeyed(), prototype.formatter, this, aggregations);
        }

        public B createBucket(Object key, long docCount, InternalAggregations aggregations, boolean keyed, ValueFormatter formatter) {
            if (key instanceof Number) {
                return (B) new RyftInternalHistogram.Bucket(((Number) key).longValue(), docCount, keyed, formatter, this, aggregations);
            } else {
                throw new AggregationExecutionException("Expected key of type Number but got [" + key + "]");
            }
        }

        protected B createEmptyBucket(boolean keyed, ValueFormatter formatter) {
            return (B) new RyftInternalHistogram.Bucket(keyed, formatter, this);
        }

    }

    protected List<B> buckets;
    private RyftInternalOrder order;
    private ValueFormatter formatter;
    private boolean keyed;
    private long minDocCount;
    private RyftInternalHistogram.EmptyBucketInfo emptyBucketInfo;
    protected RyftInternalHistogram.Factory<B> factory;

    RyftInternalHistogram() {} // for serialization

    public RyftInternalHistogram(String name, List<B> buckets, RyftInternalOrder order, long minDocCount, RyftInternalHistogram.EmptyBucketInfo emptyBucketInfo,
                      ValueFormatter formatter, boolean keyed, RyftInternalHistogram.Factory<B> factory, List<PipelineAggregator> pipelineAggregators,
                      Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.buckets = buckets;
        this.order = order;
        assert (minDocCount == 0) == (emptyBucketInfo != null);
        this.minDocCount = minDocCount;
        this.emptyBucketInfo = emptyBucketInfo;
        this.formatter = formatter;
        this.keyed = keyed;
        this.factory = factory;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public List<B> getBuckets() {
        return buckets;
    }

    public RyftInternalHistogram.Factory<B> getFactory() {
        return factory;
    }

    public Rounding getRounding() {
        return emptyBucketInfo.rounding;
    }

    @Override
    public RyftInternalHistogram<B> create(List<B> buckets) {
        return getFactory().create(buckets, this);
    }

    @Override
    public B createBucket(InternalAggregations aggregations, B prototype) {
        return getFactory().createBucket(aggregations, prototype);
    }

    private static class IteratorAndCurrent<B> {

        private final Iterator<B> iterator;
        private B current;

        IteratorAndCurrent(Iterator<B> iterator) {
            this.iterator = iterator;
            current = iterator.next();
        }

    }

    private List<B> reduceBuckets(List<InternalAggregation> aggregations, ReduceContext reduceContext) {

        final PriorityQueue<RyftInternalHistogram.IteratorAndCurrent<B>> pq = new PriorityQueue<RyftInternalHistogram.IteratorAndCurrent<B>>(aggregations.size()) {
            @Override
            protected boolean lessThan(RyftInternalHistogram.IteratorAndCurrent<B> a, RyftInternalHistogram.IteratorAndCurrent<B> b) {
                return a.current.key < b.current.key;
            }
        };
        for (InternalAggregation aggregation : aggregations) {
            RyftInternalHistogram<B> histogram = (RyftInternalHistogram) aggregation;
            if (histogram.buckets.isEmpty() == false) {
                pq.add(new RyftInternalHistogram.IteratorAndCurrent<>(histogram.buckets.iterator()));
            }
        }

        List<B> reducedBuckets = new ArrayList<>();
        if (pq.size() > 0) {
            // list of buckets coming from different shards that have the same key
            List<B> currentBuckets = new ArrayList<>();
            long key = pq.top().current.key;

            do {
                final RyftInternalHistogram.IteratorAndCurrent<B> top = pq.top();

                if (top.current.key != key) {
                    // the key changes, reduce what we already buffered and reset the buffer for current buckets
                    final B reduced = currentBuckets.get(0).reduce(currentBuckets, reduceContext);
                    if (reduced.getDocCount() >= minDocCount) {
                        reducedBuckets.add(reduced);
                    }
                    currentBuckets.clear();
                    key = top.current.key;
                }

                currentBuckets.add(top.current);

                if (top.iterator.hasNext()) {
                    final B next = top.iterator.next();
                    assert next.key > top.current.key : "shards must return data sorted by key";
                    top.current = next;
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            } while (pq.size() > 0);

            if (currentBuckets.isEmpty() == false) {
                final B reduced = currentBuckets.get(0).reduce(currentBuckets, reduceContext);
                if (reduced.getDocCount() >= minDocCount) {
                    reducedBuckets.add(reduced);
                }
            }
        }

        return reducedBuckets;
    }

    private void addEmptyBuckets(List<B> list, ReduceContext reduceContext) {
        B lastBucket = null;
        ExtendedBounds bounds = emptyBucketInfo.bounds;
        ListIterator<B> iter = list.listIterator();

        // first adding all the empty buckets *before* the actual data (based on th extended_bounds.min the user requested)
        InternalAggregations reducedEmptySubAggs = InternalAggregations.reduce(Collections.singletonList(emptyBucketInfo.subAggregations),
                reduceContext);
        if (bounds != null) {
            B firstBucket = iter.hasNext() ? list.get(iter.nextIndex()) : null;
            if (firstBucket == null) {
                if (bounds.min != null && bounds.max != null) {
                    long key = bounds.min;
                    long max = bounds.max;
                    while (key <= max) {
                        iter.add(getFactory().createBucket(key, 0,
                                reducedEmptySubAggs,
                                keyed, formatter));
                        key = emptyBucketInfo.rounding.nextRoundingValue(key);
                    }
                }
            } else {
                if (bounds.min != null) {
                    long key = bounds.min;
                    if (key < firstBucket.key) {
                        while (key < firstBucket.key) {
                            iter.add(getFactory().createBucket(key, 0,
                                    reducedEmptySubAggs,
                                    keyed, formatter));
                            key = emptyBucketInfo.rounding.nextRoundingValue(key);
                        }
                    }
                }
            }
        }

        // now adding the empty buckets within the actual data,
        // e.g. if the data series is [1,2,3,7] there're 3 empty buckets that will be created for 4,5,6
        while (iter.hasNext()) {
            B nextBucket = list.get(iter.nextIndex());
            if (lastBucket != null) {
                long key = emptyBucketInfo.rounding.nextRoundingValue(lastBucket.key);
                while (key < nextBucket.key) {
                    iter.add(getFactory().createBucket(key, 0,
                            reducedEmptySubAggs, keyed,
                            formatter));
                    key = emptyBucketInfo.rounding.nextRoundingValue(key);
                }
                assert key == nextBucket.key;
            }
            lastBucket = iter.next();
        }

        // finally, adding the empty buckets *after* the actual data (based on the extended_bounds.max requested by the user)
        if (bounds != null && lastBucket != null && bounds.max != null && bounds.max > lastBucket.key) {
            long key = emptyBucketInfo.rounding.nextRoundingValue(lastBucket.key);
            long max = bounds.max;
            while (key <= max) {
                iter.add(getFactory().createBucket(key, 0,
                        reducedEmptySubAggs, keyed,
                        formatter));
                key = emptyBucketInfo.rounding.nextRoundingValue(key);
            }
        }
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        List<B> reducedBuckets = reduceBuckets(aggregations, reduceContext);

        // adding empty buckets if needed
        if (minDocCount == 0) {
            addEmptyBuckets(reducedBuckets, reduceContext);
        }

        if (order == RyftInternalOrder.KEY_ASC) {
            // nothing to do, data are already sorted since shards return
            // sorted buckets and the merge-sort performed by reduceBuckets
            // maintains order
        } else if (order == RyftInternalOrder.KEY_DESC) {
            // we just need to reverse here...
            List<B> reverse = new ArrayList<>(reducedBuckets);
            Collections.reverse(reverse);
            reducedBuckets = reverse;
        } else {
            // sorted by sub-aggregation, need to fall back to a costly n*log(n) sort
            CollectionUtil.introSort(reducedBuckets, order.comparator());
        }

        return getFactory().create(getName(), reducedBuckets, order, minDocCount, emptyBucketInfo, formatter, keyed, pipelineAggregators(),
                getMetaData());
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        this.factory = resolveFactory(in.readString());
        order = RyftInternalOrder.Streams.readOrder(in);
        minDocCount = in.readVLong();
        if (minDocCount == 0) {
            emptyBucketInfo = RyftInternalHistogram.EmptyBucketInfo.readFrom(in);
        }
        formatter = ValueFormatterStreams.readOptional(in);
        keyed = in.readBoolean();
        int size = in.readVInt();
        List<B> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            B bucket = getFactory().createEmptyBucket(keyed, formatter);
            bucket.readFrom(in);
            buckets.add(bucket);
        }
        this.buckets = buckets;
    }

    @SuppressWarnings("unchecked")
    private static <B extends RyftInternalHistogram.Bucket> RyftInternalHistogram.Factory<B> resolveFactory(String factoryType) {
        if (factoryType.equals(InternalDateHistogram.TYPE.name())) {
            return (RyftInternalHistogram.Factory<B>) new RyftInternalDateHistogram.Factory();
        } else if (factoryType.equals(TYPE.name())) {
            return new RyftInternalHistogram.Factory<>();
        } else {
            throw new IllegalStateException("Invalid histogram factory type [" + factoryType + "]");
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(factory.type());
        RyftInternalOrder.Streams.writeOrder(order, out);
        out.writeVLong(minDocCount);
        if (minDocCount == 0) {
            RyftInternalHistogram.EmptyBucketInfo.writeTo(emptyBucketInfo, out);
        }
        ValueFormatterStreams.writeOptional(formatter, out);
        out.writeBoolean(keyed);
        out.writeVInt(buckets.size());
        for (B bucket : buckets) {
            bucket.writeTo(out);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS);
        } else {
            builder.startArray(CommonFields.BUCKETS);
        }
        for (B bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

}
